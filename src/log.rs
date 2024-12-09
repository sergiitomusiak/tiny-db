use crate::{EntryMetadata, Result};
use crate::error::TinyDbError;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use std::io::ErrorKind;

const LEN_FIELD_SIZE: usize = std::mem::size_of::<u64>();
const METADATA_FIELD_SIZE: usize = std::mem::size_of::<u64>();

#[derive(Default, Debug)]
pub struct LogStats {
    pub entries_num: usize,
    pub max_version: u64,
}

impl LogStats {
    pub fn merge(&mut self, other: LogStats) {
        self.entries_num += other.entries_num;
        self.max_version = std::cmp::max(self.max_version, other.max_version);
    }
}

pub(crate) struct Log {
    pub(crate) id: u64,
    pub(crate) file: File,
    pub(crate) len: u64,
}

impl Log {
    pub(crate) async fn append(&mut self, buf: &[u8]) -> Result<u64> {
        self.file.write_all(buf).await?;
        let old_len = self.len;
        self.len += buf.len() as u64;
        Ok(old_len)
    }
}

pub(crate) async fn read(file: &mut File, offset: u64, len: u64, buf: &mut Vec<u8>) -> Result<()> {
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let buf_len = buf.len();
    buf.resize(buf_len + len as usize, 0);
    file.read_exact(&mut buf[buf_len..]).await?;
    Ok(())
}

pub(crate) fn write_key_value<K, V>(buf: &mut Vec<u8>, key: &K, value: &V) -> Result<(usize, usize)>
    where
        K: Serialize,
        V: Serialize,
{
        buf.resize(2*LEN_FIELD_SIZE, 0);
        // write key_len, key
        rmp_serde::encode::write(buf,&key)?;
        let key_len = buf.len() - 2*LEN_FIELD_SIZE;
        write_u64(&mut buf[LEN_FIELD_SIZE..], key_len as u64);
        // write val_len, val
        let val_start = buf.len();
        buf.resize(buf.len() + LEN_FIELD_SIZE, 0);
        rmp_serde::encode::write(buf,&value)?;
        let val_len = buf.len() - val_start - LEN_FIELD_SIZE;
        write_u64(&mut buf[val_start..], val_len as u64);
        Ok((val_start + LEN_FIELD_SIZE, val_len))
}

pub(crate) fn write_key_raw_value<K>(buf: &mut Vec<u8>, key: &K, value: &[u8]) -> Result<(usize, usize)>
    where
        K: Serialize,
{
        buf.resize(2*LEN_FIELD_SIZE, 0);
        // write key_len, key
        rmp_serde::encode::write(buf,&key)?;
        let key_len = buf.len() - 2*LEN_FIELD_SIZE;
        write_u64(&mut buf[LEN_FIELD_SIZE..], key_len as u64);
        // write val_len, val
        let val_start = buf.len();
        buf.resize(buf.len() + LEN_FIELD_SIZE, 0);
        buf.extend_from_slice(value);
        write_u64(&mut buf[val_start..], value.len() as u64);
        Ok((val_start + LEN_FIELD_SIZE, value.len()))
}


pub struct LogReader<'a> {
    log_stats: LogStats,
    buf_reader: BufReader<&'a mut File>,
    val_offset: u64,
    val_len: u64,
    buf: Vec<u8>,
}

pub(crate) struct LogEntry<K> {
    pub(crate) key: K,
    pub(crate) key_len: u64,
    pub(crate) val_offset: u64,
    pub(crate) val_len: u64,
    pub(crate) metadata: EntryMetadata,
}

impl<K> LogEntry<K> {
    pub(crate) fn size(&self) -> u64 {
        // lengths of version key_len and val_len fields
        (METADATA_FIELD_SIZE + 2*LEN_FIELD_SIZE) as u64 +
        // lengths of key and value
        self.key_len + self.val_len
    }
}

impl<'a> LogReader<'a> {
    pub(crate) fn new<'b>(file: &'b mut File) -> LogReader<'b> {
        let buf_reader = BufReader::new(file);
        LogReader {
            log_stats: LogStats::default(),
            buf_reader,
            val_offset: 0,
            val_len: 0,
            buf: Vec::new(),
        }
    }

    pub(crate) async fn next<K>(&mut self) -> Result<Option<LogEntry<K>>>
        where K: DeserializeOwned,
    {
        self.next_with_value_buffer(None).await
    }

    pub(crate) async fn next_with_value_buffer<K>(&mut self, value_buf: Option<&mut Vec<u8>>) -> Result<Option<LogEntry<K>>>
        where K: DeserializeOwned,
    {
        let Some(metadata) = read_metadata(&mut self.buf_reader).await? else {
            return Ok(None);
        };

        self.log_stats.max_version = std::cmp::max(self.log_stats.max_version, metadata.version());
        self.log_stats.entries_num += 1;

        // read key
        let key_len = self.buf_reader.read_u64().await?;
        self.buf.resize(key_len as usize, 0);
        self.buf_reader.read_exact(&mut self.buf).await?;
        let key: K = rmp_serde::from_slice(&self.buf)?;

        // read value
        let val_len = self.buf_reader.read_u64().await?;
        self.val_offset += key_len + 3*LEN_FIELD_SIZE as u64;
        self.val_len = val_len;

        let log_entry = LogEntry {
            key,
            key_len,
            val_offset: self.val_offset,
            val_len: self.val_len,
            metadata,
        };

        let buf = if let Some(value_buf) = value_buf {
            // append to input buffer
            let val_start = value_buf.len();
            value_buf.resize(value_buf.len() + val_len as usize, 0);
            &mut value_buf[val_start..(val_start+(val_len as usize))]
        } else {
            self.buf.resize(val_len as usize, 0);
            &mut self.buf
        };

        self.val_offset += val_len as u64;
        self.buf_reader.read_exact(buf).await?;

        Ok(Some(log_entry))
    }

    pub(crate) fn log_stats(self) -> LogStats {
        self.log_stats
    }
}

async fn read_metadata(buf_reader: &mut (impl tokio::io::AsyncBufRead + Unpin)) -> Result<Option<EntryMetadata>> {
    let mut buf = [0u8; METADATA_FIELD_SIZE];
    let read_result = buf_reader.read_exact(&mut buf).await;
    let raw_metadata = match read_result {
        Ok(read_bytes) => {
            if read_bytes != buf.len() {
                return Err(TinyDbError::from(std::io::Error::new(ErrorKind::UnexpectedEof, "log is corrupted")));
            }
            u64::from_be_bytes(buf)
        },
        Err(e) => {
            if e.kind() == ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(TinyDbError::from(e));
        }
    };
    Ok(Some(EntryMetadata::from_raw(raw_metadata)))
}

fn write_u64(buf: &mut [u8], value: u64) {
    let bytes = u64::to_be_bytes(value);
    for i in 0..bytes.len() {
        buf[i] = bytes[i];
    }
}