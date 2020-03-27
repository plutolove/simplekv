use super::Result;

pub trait KvEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

pub use self::kv::KvStore;

mod kv;
