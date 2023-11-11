use super::Map;
use kgdata::error::KGDataError;

pub mod ipcserde;
pub mod nngserver;
pub mod remote_rocksdb_dict;
pub mod request;
pub mod response;
pub mod shmemhelper;

pub use self::nngserver::{serve_db, NNGClient, NNGLocalClient};
pub use self::remote_rocksdb_dict::BaseRemoteRocksDBDict;
pub use self::request::Request;
pub use self::response::Response;
use self::shmemhelper::ReadonlySharedMemBuffer;

pub trait Client: Send + Sync {
    type Message: std::ops::Deref<Target = [u8]>;

    fn open(url: &str) -> Result<Self, KGDataError>
    where
        Self: Sized;

    // send a request to the server
    fn request(&self, req: &[u8]) -> Result<Self::Message, KGDataError>;

    fn get_shm(&self) -> Option<&ReadonlySharedMemBuffer> {
        None
    }
}

pub type RemoteRocksDBDict<K, V> = BaseRemoteRocksDBDict<K, V, NNGLocalClient>;
// pub type RemoteRocksDBDict<K, V> = BaseRemoteRocksDBDict<K, V, NNGClient>;
