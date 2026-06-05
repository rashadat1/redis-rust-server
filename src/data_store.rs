use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

type ConcurrentHashMap = Arc<Mutex<HashMap<String, String>>>;
#[derive(Clone)]
pub struct KvStore {
    pub db: ConcurrentHashMap,
}
pub enum SetOptions {
    EX(String),
    PX(String),
}
pub type SetOptionList = Vec<SetOptions>;
impl KvStore {
    pub fn new() -> Self {
        let data: ConcurrentHashMap = Arc::new(Mutex::new(HashMap::new()));
        return KvStore { db: data };
    }
    pub fn get(&self, key: String) -> Option<String> {
        let locked_ref = self.db.lock().unwrap();
        if !locked_ref.contains_key(&key) {
            None
        } else {
            Some(locked_ref.get(&key).unwrap().to_string())
        }
    }
    pub fn set(&self, key: String, value: String, options: Option<SetOptionList>) {
        let mut locked_ref = self.db.lock().unwrap();
        if let Some(val) = locked_ref.get_mut(&key) {
            *val = value;
            return;
        }
        locked_ref.insert(key, value);
        return;
    }
}
