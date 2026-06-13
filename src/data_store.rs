use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::redis_error::RedisError;
#[derive(Clone)]
pub struct StoredVal {
    pub val: String,
    pub expiry: Option<Instant>,
}
#[derive(Clone)]
pub struct Store {
    pub kv: HashMap<String, StoredVal>,
    pub keys_with_expiry: Vec<(String, Instant)>,
    pub index_map: HashMap<String, usize>,
}
type ConcurrentHashMap = Arc<Mutex<Store>>;

#[derive(Clone)]
pub struct KvStore {
    pub db: ConcurrentHashMap,
}
pub enum SetOptions {
    EX(String),
    PX(String),
}
pub type SetOptionList = Vec<SetOptions>;
impl Store {
    pub fn new() -> Self {
        let kv: HashMap<String, StoredVal> = HashMap::new();
        let keys_with_expiry: Vec<(String, Instant)> = Vec::new();
        let index_map: HashMap<String, usize> = HashMap::new();
        return Store {
            kv,
            keys_with_expiry,
            index_map,
        };
    }
    pub fn remove(&mut self, key: &String) {
        // &mut self so that the caller still owns the Store
        self.kv.remove(key);
        let idx = self.index_map.get(key).unwrap().clone();
        let last_element = self.keys_with_expiry[self.keys_with_expiry.len() - 1].clone();
        self.keys_with_expiry.swap_remove(idx);

        self.index_map.remove(key);
        self.index_map.insert(last_element.0, idx);
    }
    pub fn insert(&mut self, key: String, val: StoredVal, expiry: Option<Instant>) {
        self.kv.insert(key.clone(), val);
        if expiry.is_some() {
            self.keys_with_expiry.push((key.clone(), expiry.unwrap()));
            self.index_map
                .insert(key.clone(), self.keys_with_expiry.len() - 1);
        }
    }
}
impl KvStore {
    pub fn new() -> Self {
        let data: ConcurrentHashMap = Arc::new(Mutex::new(Store::new()));
        return KvStore { db: data };
    }
    pub fn get(&self, key: String) -> Option<String> {
        let mut store = self.db.lock().unwrap();
        let locked_ref = &store.kv;

        let expired = match locked_ref.get(&key) {
            None => return None,
            Some(val) => val
                .expiry
                .is_some_and(|time: Instant| time.lt(&Instant::now())),
        };
        if expired {
            store.remove(&key);
            None
        } else {
            Some(locked_ref.get(&key).unwrap().val.clone())
        }
    }
    pub fn set(
        &self,
        key: String,
        new_value: String,
        options: Option<SetOptionList>,
    ) -> Result<(), RedisError> {
        let mut expiry: Option<Instant> = None;
        if let Some(opts) = options {
            for opt in opts {
                match opt {
                    SetOptions::EX(arg) => {
                        let timeout = arg.parse::<u64>().map_err(|_| {
                            RedisError::InvalidArgumentForCommandOption(
                                "SET".to_string(),
                                "EX".to_string(),
                                "Integer".to_string(),
                                arg,
                            )
                        })?;
                        let curr_time = Instant::now();
                        expiry = Some(curr_time + Duration::new(timeout, 0));
                    }
                    SetOptions::PX(arg) => {
                        let timeout = arg.parse::<u32>().map_err(|_| {
                            RedisError::InvalidArgumentForCommandOption(
                                "SET".to_string(),
                                "PX".to_string(),
                                "Integer".to_string(),
                                arg,
                            )
                        })?;
                        let curr_time = Instant::now();
                        expiry = Some(curr_time + Duration::new(0, timeout * 1_000_000))
                    }
                }
            }
        }
        let new_stored_val = StoredVal {
            val: new_value,
            expiry: expiry,
        };
        let mut locked_ref = self.db.lock().unwrap();
        locked_ref.insert(key, new_stored_val, expiry);
        Ok(())
    }
}
