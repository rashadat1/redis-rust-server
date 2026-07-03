use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::redis_error::RedisError;
#[derive(Clone)]
pub struct StoredVal {
    pub val: RedisValue,
    pub expiry: Option<Instant>,
}
#[derive(Clone)]
pub enum RedisValue {
    StringVal(String),
    ListVal(VecDeque<String>),
}
pub enum PushType {
    Left,
    Right,
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
        let val_removed = self.kv.remove(key);
        if val_removed.is_none() {
            return;
        }
        if val_removed.unwrap().expiry.is_none() {
            return;
        }
        let idx = self.index_map.get(key).unwrap().clone();
        let last_element = self.keys_with_expiry[self.keys_with_expiry.len() - 1].clone();
        self.keys_with_expiry.swap_remove(idx);

        self.index_map.remove(key);
        if idx < self.keys_with_expiry.len() - 1 {
            self.index_map.insert(last_element.0, idx);
        }
    }
    pub fn insert(&mut self, key: String, val: StoredVal, expiry: Option<Instant>) {
        self.kv.insert(key.clone(), val);
        if expiry.is_some() {
            self.keys_with_expiry.push((key.clone(), expiry.unwrap()));
            self.index_map
                .insert(key.clone(), self.keys_with_expiry.len() - 1);
        }
    }
    pub fn push(
        &mut self,
        list_key: String,
        to_append: Vec<String>,
        push_type: PushType,
    ) -> Result<usize, RedisError> {
        let list_val = self.kv.entry(list_key.clone()).or_insert(StoredVal {
            val: RedisValue::ListVal(VecDeque::new()),
            expiry: None,
        });
        match &mut list_val.val {
            RedisValue::StringVal(_) => Err(RedisError::WrongType(format!(
                "Key: {} already exists in kv store and the value for the key is a String. For existing keys, RPUSH requires the value be a list",
                list_key
            ))),
            RedisValue::ListVal(list) => {
                match push_type {
                    PushType::Left => {
                        for el in to_append {
                            list.push_front(el);
                        }
                    }
                    PushType::Right => {
                        for el in to_append {
                            list.push_back(el);
                        }
                    }
                }
                Ok(list.len())
            }
        }
    }
    pub fn lrange(
        &mut self,
        list_key: String,
        start_: i32,
        stop_: i32,
    ) -> Result<Vec<String>, RedisError> {
        let stored = match self.kv.get(&list_key) {
            None => return Ok(Vec::new()),
            Some(whole_list) => whole_list,
        };
        let RedisValue::ListVal(list) = &stored.val else {
            return Err(RedisError::WrongType(format!(
                "Key: {} exists in kv store but the value for the key is a String. LRANGE requires the value be a list",
                list_key
            )));
        };
        let start = normalize_lrange_indices(start_, list.len() as i32);
        let stop = normalize_lrange_indices(stop_, list.len() as i32);
        if start > stop {
            return Ok(Vec::new());
        }
        if start >= list.len() {
            return Ok(Vec::new());
        }
        let stop_ = if stop >= list.len() {
            list.len() - 1
        } else {
            stop
        };
        Ok(list.range(start..=stop_).cloned().collect())
    }
}
impl KvStore {
    pub fn new() -> Self {
        let data: ConcurrentHashMap = Arc::new(Mutex::new(Store::new()));
        return KvStore { db: data };
    }
    pub fn get(&self, key: String) -> Option<RedisValue> {
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
                        expiry = Some(curr_time + Duration::from_secs(timeout));
                    }
                    SetOptions::PX(arg) => {
                        let timeout = arg.parse::<u64>().map_err(|_| {
                            RedisError::InvalidArgumentForCommandOption(
                                "SET".to_string(),
                                "PX".to_string(),
                                "Integer".to_string(),
                                arg,
                            )
                        })?;
                        let curr_time = Instant::now();
                        expiry = Some(curr_time + Duration::from_millis(timeout));
                    }
                }
            }
        }
        let new_stored_val = StoredVal {
            val: RedisValue::StringVal(new_value),
            expiry: expiry,
        };
        let mut locked_ref = self.db.lock().unwrap();
        locked_ref.insert(key, new_stored_val, expiry);
        Ok(())
    }
    pub fn push(
        &self,
        list_key: String,
        to_append: Vec<String>,
        push_type: PushType,
    ) -> Result<usize, RedisError> {
        let mut locked_ref = self.db.lock().unwrap();
        locked_ref.push(list_key, to_append, push_type)
    }
    pub fn lrange(
        &self,
        list_key: String,
        start: i32,
        stop: i32,
    ) -> Result<Vec<String>, RedisError> {
        let mut locked_ref = self.db.lock().unwrap();
        locked_ref.lrange(list_key, start, stop)
    }
    pub fn llen(
        &self,
        list_key: String,
    ) -> Result<usize, RedisError> {
        let mut locked_ref = 
    }
}
fn normalize_lrange_indices(index: i32, cap: i32) -> usize {
    return if index >= 0 {
        index as usize
    } else if index < -1 * cap {
        0
    } else {
        (index + cap) as usize
    };
}
