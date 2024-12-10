mod error;
mod log;

use error::TinyDbError;
use log::{write_key_value, write_key_raw_value, Log, LogEntry, LogReader, LogStats};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as AsyncMutex;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

pub type Result<T> = std::result::Result<T, crate::error::TinyDbError>;

pub type FileSize = u64;

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub active: u64,
    pub read_only: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub log_file_size: u64,
    pub live_faction_compaction_trigger: f64,
    pub create_if_missing: bool,
    pub compaction_chunk_len: usize,
    pub compaction_chunk_size: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            log_file_size: 10 << 20, // 10MiB
            live_faction_compaction_trigger: 0.7,
            create_if_missing: true,
            compaction_chunk_len: 50,
            compaction_chunk_size: 4 << 10, // 4KiB
        }
    }
}

struct EntryMetadata(u64);

impl EntryMetadata {
    const DEFAULT_FLAGS: u64 = 0;
    const DELETED_FLAG: u64 = 1 << 56;
    const VERSION_MASK: u64 = !(0xff << 56);

    fn new(version: u64, deleted: bool) -> Self {
        let deleted = if deleted { Self::DELETED_FLAG } else { Self::DEFAULT_FLAGS };
        EntryMetadata(version | deleted)
    }

    fn from_raw(metadata: u64) -> Self {
        Self(metadata)
    }

    fn is_deleted(&self) -> bool {
        self.0 & Self::DELETED_FLAG != 0
    }

    fn version(&self) -> u64 {
        self.0 & Self::VERSION_MASK
    }
}

#[derive(Debug, Clone, Copy)]
struct IndexEntry {
    offset: u64,
    len: u64,
    file_id: u64,
    version: u64,
}

pub struct Store<K> {
    state: Arc<StoreState<K>>,
}

struct StoreState<K> {
    index: Arc<RwLock<HashMap<K, IndexEntry>>>,
    log_state: Arc<AsyncMutex<LogState>>,
    root: PathBuf,
    options: Options,
    compaction_in_progress: Arc<AtomicBool>,
}

struct LogState {
    active_log: Log,
    manifest: Manifest,
    version: u64,
    next_file_id: u64,
    entries_num: usize,
    live_entries_num: usize,
}

impl LogState {
    fn next_version(&mut self) -> u64 {
        self.version += 1;
        self.version
    }

    fn next_file_id(&mut self) -> u64 {
        self.next_file_id += 1;
        self.next_file_id
    }
}

impl<K> Store<K>
    where K: Eq + PartialEq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub async fn open(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        // create or read manifest
        let manifest = create_or_read_manifest(&path, &options).await?;
        // read index from files
        let mut log_stats = LogStats::default();
        let mut max_file_id: u64 = 0;
        let mut index = HashMap::new();
        for id in manifest.read_only.iter() {
            let log_file = log_file_name(*id);
            let mut file = tokio::fs::OpenOptions::new()
                .read(true)
                .create(false)
                .open(path.as_ref().join(log_file))
                .await?;

            let file_log_stats = read_log_into_index::<K>(*id, &mut file, &mut index).await?;
            log_stats.merge(file_log_stats);
            max_file_id = std::cmp::max(max_file_id, *id);
        }

        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref().join(log_file_name(manifest.active)))
            .await?;

        let file_log_stats = read_log_into_index::<K>(manifest.active, &mut file, &mut index).await?;
        log_stats.merge(file_log_stats);
        max_file_id = std::cmp::max(max_file_id, manifest.active);
        let live_entries_num = index.len();
        let len = file.metadata().await?.len();
        Ok(Self {
            state: Arc::new(StoreState {
                index: Arc::new(RwLock::new(index)),
                log_state: Arc::new(AsyncMutex::new(LogState {
                    active_log: Log {
                        file,
                        len,
                        id: manifest.active,
                    },
                    manifest,
                    version: log_stats.max_version,
                    next_file_id: max_file_id,
                    entries_num: log_stats.entries_num,
                    live_entries_num,
                })),
                root: path.as_ref().to_path_buf(),
                options,
                compaction_in_progress: Arc::new(AtomicBool::new(false)),
          })
        })
    }

    pub async fn get<V: DeserializeOwned>(&self, key: &K) -> Result<Option<V>> {
        let index_entry = {
            let index = self.state.index.read().expect("index read");
            let Some(index_entry) = index.get(key).cloned() else {
                return Ok(None);
            };
            index_entry
        };
        let mut buf = Vec::new();
        let log_file_name = self.state.root.join(log_file_name(index_entry.file_id));
        Self::read_from_log(log_file_name,&index_entry, &mut buf).await?;
        let value = rmp_serde::from_slice(&buf)?;
        Ok(value)
    }

    pub async fn insert<V: Serialize>(&self, key: K, value: V) -> Result<()> {
        let mut buf = Vec::new();
        let (val_offset, val_len) = write_key_value(&mut buf, &key, &value)?;
        self.insert_buf(key, &mut buf, val_len, val_offset, false).await
    }

    pub async fn remove(&self, key: K) -> Result<()> {
        let mut buf = Vec::new();
        let (val_offset, val_len) = write_key_raw_value(&mut buf, &key, &[])?;
        self.insert_buf(key, &mut buf, val_len, val_offset, true).await
    }

    async fn insert_buf(&self, key:  K, buf: &mut [u8], val_len: usize, val_offset: usize, deleting: bool) -> Result<()> {
        if deleting {
            {
                let index = self.state.index.read().expect("index read");
                if !index.contains_key(&key) {
                    return Ok(());
                }
            }
        }

        // write to log with latest version
        let compaction_required = {
            let mut log_state = self.state.log_state.lock().await;
            let version = log_state.next_version();
            write_u64(buf, EntryMetadata::new(version, deleting).0);
            let old_offset = log_state.active_log.append(&buf).await?;

            // update index
            let index_entry = IndexEntry {
                offset: old_offset + val_offset as u64,
                len: val_len as u64,
                file_id: log_state.active_log.id,
                version,
            };
            {
                let mut index = self.state.index.write().expect("index write");
                if deleting {
                    let deleted = index.remove(&key).is_some();
                    if deleted {
                        log_state.live_entries_num -= 1;
                    }
                } else {
                    let added = index.insert(key, index_entry).is_none();
                    if added {
                        log_state.live_entries_num += 1;
                    }
                }
            }

            log_state.entries_num += 1;

            // check if new log must be created
            if log_state.active_log.len >= self.state.options.log_file_size {
                // TODO: Slow function. Need to fix it
                Self::create_new_active_log_file(self.state.root.clone(), &mut log_state).await?;
            }

            // maybe start background compaction
            self.compaction_required(&log_state)
        };

        if compaction_required {
            self.start_background_compaction();
        }

        Ok(())
    }

    pub async fn force_compaction(&self) -> Result<()> {
        Self::compact(self.state.clone()).await?;
        {
            let log_state = self.state.log_state.lock().await;
            let live_fraction = log_state.live_entries_num as f64 / log_state.entries_num as f64;
            println!("live fraction = {live_fraction}, live = {}, log_state.total = {}", log_state.live_entries_num, log_state.entries_num);
        }
        Ok(())
    }

    async fn read_from_log(log_file_name: impl AsRef<Path>, index_entry: &IndexEntry, buf: &mut Vec<u8>) -> Result<()> {
        // TODO: Add read cache
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(log_file_name)
            .await?;

        log::read(&mut file, index_entry.offset, index_entry.len, buf).await
    }

    async fn create_new_active_log_file(root: PathBuf, log_state: &mut LogState) -> Result<()> {
        let file_id = log_state.next_file_id();
        let log_file_name = root.join(log_file_name(file_id));

        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(log_file_name)
            .await?;

        // Update manifest
        log_state.manifest.read_only.push(log_state.manifest.active);
        log_state.manifest.active = file_id;
        let buf = rmp_serde::to_vec(&log_state.manifest)?;
        let manifest_path = root.join(manifest_file());
        let mut manifest_file = tokio::fs::OpenOptions::new()
            .truncate(true)
            .write(true)
            .open(manifest_path)
            .await?;
        manifest_file.write_all(&buf).await?;
        manifest_file.flush().await?;

        log_state.active_log = Log {
            id: file_id,
            file,
            len: 0,
        };

        Ok(())
    }

    fn compaction_required(&self, log_state: &LogState) -> bool {
        let live_fraction = log_state.live_entries_num as f64 / log_state.entries_num as f64;
        if live_fraction <= self.state.options.live_faction_compaction_trigger {
            return !self.state.compaction_in_progress.load(Ordering::Acquire);
        }
        false
    }

    fn start_background_compaction(&self) {
        let update_result = self
            .state
            .compaction_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);

        if let Err(_) = update_result {
            // compaction is already in progress
            return;
        }

        let state = self.state.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::compact(state).await {
                eprint!("compaction failed: {e:?}");
            }
        });
    }

    async fn compact(state: Arc<StoreState<K>>) -> Result<()> {
        let old_manifest = {
            let mut log_state = state.log_state.lock().await;
            Self::create_new_active_log_file(state.root.clone(), &mut log_state).await?;
            log_state.manifest.clone()
        };

        if old_manifest.read_only.is_empty() {
            // No files to compact
            return Ok(())
        }

        let mut new_log = Self::new_log(&state).await?;
        let mut new_logs = HashSet::new();
        let mut buf = Vec::new();
        let mut log_entries_batch_size = 0;
        let mut log_entries_batch = Vec::new();
        let mut values_buf = Vec::new();
        let mut index_updates = HashMap::new();
        let mut entries_discarded = 0;

        // read each read-only file and copy live entries into new read-only file
        for old_log_file_id in old_manifest.read_only.iter() {
            // Read item, check if it is live, and write live item to new read-only file
            let old_log_file_name = state.root.join(log_file_name(*old_log_file_id));
            let mut old_log_file = tokio::fs::OpenOptions::new()
                .read(true)
                .open(old_log_file_name)
                .await?;

            let mut log_reader = LogReader::new(&mut old_log_file);
            while let Some(log_entry) = log_reader.next_with_value_buffer::<K>(Some(&mut values_buf)).await? {
                entries_discarded += 1;
                log_entries_batch_size += log_entry.size();
                log_entries_batch.push(log_entry);
                if log_entries_batch.len() >= state.options.compaction_chunk_len
                    || (new_log.len + log_entries_batch_size) >= state.options.log_file_size
                {
                    Self::write_live_log_entries(
                        &state,
                        &mut new_log,
                        log_entries_batch,
                        &mut index_updates,
                        &values_buf,
                        &mut buf,
                    ).await?;

                    log_entries_batch = Vec::new();
                    values_buf.resize(0, 0);
                }

                // check if new log file has to be created
                if new_log.len >= state.options.log_file_size {
                    new_log.file.flush().await?;
                    new_logs.insert(new_log.id);
                    new_log = Self::new_log(&state).await?;
                }
            }
        }

        // write remaining log entries
        if !log_entries_batch.is_empty() {
            Self::write_live_log_entries(
                &state,
                &mut new_log,
                log_entries_batch,
                &mut index_updates,
                &values_buf,
                &mut buf,
            ).await?;
        }

        // flush new log file
        if new_log.len > 0 {
            new_log.file.flush().await?;
            new_logs.insert(new_log.id);
        } else {
            // No entries were written to the last log file
            let empty_log_file_path = state.root.join(log_file_name(new_log.id));
            tokio::fs::remove_file(empty_log_file_path).await?;
        }

        // add new compacted read-only logs
        {
            let mut log_state = state.log_state.lock().await;
            log_state.manifest.read_only.extend(new_logs.iter());
        }

        entries_discarded -= index_updates.len();
        // update index in chunks
        let chunks = map_into_chunks(index_updates, state.options.compaction_chunk_len);
        for chunk in chunks {
            {
                let mut index = state.index.write().expect("index write lock");
                for (key, new_index_entry) in chunk {
                    index.entry(key).and_modify(|stored_index_entry| {
                        if stored_index_entry.version == new_index_entry.version {
                            *stored_index_entry = new_index_entry;
                        }
                    });
                }
            }
        }

        // remove old compacted read-only logs from manifest
        let old_read_only = old_manifest.read_only.iter().collect::<HashSet<_>>();
        let new_manifest = {
            let mut log_state = state.log_state.lock().await;
            let updated_read_only = log_state.manifest.read_only
                .iter()
                .filter(|read_only| !old_read_only.contains(read_only))
                .map(|read_only| *read_only)
                .collect::<Vec<_>>();
            log_state.manifest.read_only = updated_read_only;
            log_state.entries_num -= entries_discarded;
            log_state.manifest.clone()
        };

        // write manifest
        write_manifest(state.root.clone(), new_manifest).await?;

        // remove old compacted read-only files
        for file_id in old_manifest.read_only {
            let log_file_name = state.root.join(log_file_name(file_id));
            tokio::fs::remove_file(log_file_name).await?;
        }

        state.compaction_in_progress.store(false, Ordering::Release);
        Ok(())
    }

    async fn write_live_log_entries(
        state: &Arc<StoreState<K>>,
        new_log: &mut Log,
        log_entries_batch: Vec<LogEntry<K>>,
        index_updates: &mut HashMap<K, IndexEntry>,
        values_buf: &[u8],
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        // remove non-live entries

        let log_entries_live_status = {
            let mut log_entries_live_status = vec![false; log_entries_batch.len()];
            let index = state.index.read().expect("index read lock");
            for i in 0..log_entries_batch.len() {
                log_entries_live_status[i] = {
                    let log_entry = &log_entries_batch[i];

                    (!log_entry.metadata.is_deleted()) &&
                    index
                        .get(&log_entry.key)
                        .map(|index_entry| index_entry.version == log_entry.metadata.version())
                        .unwrap_or(false)
                };
            }
            log_entries_live_status
        };
        // write batch to log file
        let mut buf_val_start = 0;
        for (i, log_entry) in log_entries_batch.into_iter().enumerate() {
            let val_len = log_entry.val_len as usize;
            if log_entries_live_status[i] {
                // write key value into buffer
                let (val_offset, _) = write_key_raw_value(buf, &log_entry.key, &values_buf[buf_val_start..(buf_val_start+val_len)])?;
                write_u64(buf, log_entry.metadata.0);
                let old_offset = new_log.append(&buf).await?;
                let index_entry = IndexEntry {
                    offset: old_offset + val_offset as u64,
                    len: val_len as u64,
                    file_id: new_log.id,
                    version: log_entry.metadata.version(),
                };
                index_updates.insert(log_entry.key, index_entry);
            }
            buf_val_start += val_len;
        }

        Ok(())
    }

    async fn new_log(state: &Arc<StoreState<K>>) -> Result<Log> {
        let new_log_file_id = {
            state.log_state.lock().await.next_file_id()
        };

        let new_log_file_name = state.root.join(log_file_name(new_log_file_id));
        let new_log_file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(new_log_file_name)
            .await?;

        Ok(Log {
            id: new_log_file_id,
            file: new_log_file,
            len: 0,
        })
    }
}

async fn write_manifest(root: PathBuf, manifest: Manifest) -> Result<()> {
    let manifest_path = root.join(manifest_file());
    let buf = rmp_serde::to_vec(&manifest)?;
    tokio::fs::write(manifest_path, buf).await?;
    Ok(())
}

async fn create_or_read_manifest(path: impl AsRef<Path>, options: &Options) -> Result<Manifest> {
    let manifest_path = path.as_ref().join(manifest_file());
    let manifest = if !tokio::fs::try_exists(&manifest_path).await? {
        if options.create_if_missing {
            if !tokio::fs::try_exists(&path).await? {
                tokio::fs::create_dir(&path).await?;
            }
            let mut manifest_file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(manifest_path)
                .await?;

            let manifest = Manifest::default();
            let buf = rmp_serde::to_vec(&manifest)?;
            manifest_file.write_all(&buf).await?;
            manifest_file.flush().await?;
            manifest
        } else {
            return Err(TinyDbError::ManifestNotFound);
        }
    } else {
        let manifest = tokio::fs::read(manifest_path).await?;
        rmp_serde::from_slice(&manifest)?
    };

    Ok(manifest)
}

// Index entry format
// metadata (u64), key_len, key, value_len, value
async fn read_log_into_index<K>(file_id: u64, file: &mut tokio::fs::File, index: &mut HashMap<K, IndexEntry>) -> Result<LogStats>
    where K: DeserializeOwned + Eq + PartialEq + Hash,
{
    let mut log_reader = LogReader::new(file);
    while let Some(log_entry) = log_reader.next().await? {
        // remove or insert entry into index, if it is missing or has newer version
        let is_newer_version = index.get(&log_entry.key)
            .map(|stored_index_entry| log_entry.metadata.version() > stored_index_entry.version)
            .unwrap_or(true);
        if is_newer_version {
            let LogEntry { key, val_offset, val_len, metadata, ..} = log_entry;
            if metadata.is_deleted() {
                index.remove(&key);
            } else {
                index.insert(key, IndexEntry {
                    offset: val_offset,
                    len: val_len,
                    version: metadata.version(),
                    file_id,
                });
            }
        }
    }
    Ok(log_reader.log_stats())
}

fn map_into_chunks<K, V>(map: HashMap<K, V>, chunk_size: usize) -> Vec<Vec<(K, V)>> {
    assert!(chunk_size > 0);
    let mut res = Vec::new();
    let mut chunk = Vec::new();
    let mut i = 1;
    for (k, v) in map {
        chunk.push((k, v));
        if i % chunk_size == 0 {
            res.push(chunk);
            chunk = Vec::new();
        }
        i += 1;
    }
    if !chunk.is_empty() {
        res.push(chunk);
    }
    res
}

fn write_u64(buf: &mut [u8], value: u64) {
    let bytes = u64::to_be_bytes(value);
    for i in 0..bytes.len() {
        buf[i] = bytes[i];
    }
}

fn log_file_name(id: u64) -> String {
    format!("{id}.log")
}

fn manifest_file() -> &'static str {
    "MANIFEST"
}
