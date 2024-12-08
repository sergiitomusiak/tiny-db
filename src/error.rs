
#[derive(Debug)]
pub enum TinyDbError {
    Io(std::io::Error),
    Encode(rmp_serde::encode::Error),
    Decode(rmp_serde::decode::Error),
    ManifestNotFound,
}

impl From<std::io::Error> for TinyDbError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<rmp_serde::encode::Error> for TinyDbError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Self::Encode(e)
    }
}

impl From<rmp_serde::decode::Error> for TinyDbError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Self::Decode(e)
    }
}