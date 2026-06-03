use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

type ConcurrentHashMap = Arc<Mutex<HashMap<String, String>>>;
pub struct KvStore {
    pub db: ConcurrentHashMap,
}
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
}
