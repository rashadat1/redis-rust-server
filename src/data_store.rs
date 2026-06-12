use chrono::{self, DateTime, TimeDelta, Utc};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::redis_error::RedisError;
pub struct StoredVal {
    pub val: String,
    pub expiry: Option<DateTime<Utc>>,
}
type ConcurrentHashMap = Arc<Mutex<HashMap<String, StoredVal>>>;
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
        let mut locked_ref = self.db.lock().unwrap();
        if !locked_ref.contains_key(&key) {
            // key does not exist
            None
        } else {
            // if key exists check if it has an expiry and if that expiry has passed
            let is_expired = locked_ref
                .get(&key)
                .unwrap()
                .expiry
                .is_some_and(|time: DateTime<Utc>| time.lt(&Utc::now()));
            if is_expired {
                locked_ref.remove(&key);
                None
            } else {
                Some(locked_ref.get(&key).unwrap().val.to_string())
            }
        }
    }
    pub fn set(
        &self,
        key: String,
        new_value: String,
        options: Option<SetOptionList>,
    ) -> Result<(), RedisError> {
        let mut expiry: Option<DateTime<Utc>> = None;
        if let Some(opts) = options {
            for opt in opts {
                match opt {
                    SetOptions::EX(arg) => {
                        let timeout = arg.parse::<i64>().map_err(|_| {
                            RedisError::InvalidArgumentForCommandOption(
                                "SET".to_string(),
                                "EX".to_string(),
                                "Integer".to_string(),
                                arg,
                            )
                        })?;
                        let curr_time = Utc::now();
                        expiry = Some(curr_time + TimeDelta::seconds(timeout));
                    }
                    SetOptions::PX(arg) => {
                        let timeout = arg.parse::<i64>().map_err(|_| {
                            RedisError::InvalidArgumentForCommandOption(
                                "SET".to_string(),
                                "PX".to_string(),
                                "Integer".to_string(),
                                arg,
                            )
                        })?;
                        let curr_time = Utc::now();
                        expiry = Some(curr_time + TimeDelta::milliseconds(timeout));
                    }
                }
            }
        }
        let new_stored_val = StoredVal {
            val: new_value,
            expiry: expiry,
        };
        let mut locked_ref = self.db.lock().unwrap();
        if let Some(old_val) = locked_ref.get_mut(&key) {
            *old_val = new_stored_val;
            return Ok(());
        }
        locked_ref.insert(key, new_stored_val);
        return Ok(());
    }
}
