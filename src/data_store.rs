use crate::{redis_error::{RedisError::{self, Parse, StreamInsert}, RedisParseError, RedisStreamInsertError}, streams::Stream};
use std::{
    collections::{HashMap, VecDeque, hash_map::Entry}, sync::{Arc, Mutex}, time::{Duration, Instant},
};
use chrono::Utc;
use tokio::{sync::oneshot, time::timeout};
#[derive(Clone)]
pub struct StoredVal {
    pub val: RedisValue,
    pub expiry: Option<Instant>,
}
#[derive(Clone)]
pub enum RedisValue {
    StringVal(String),
    ListVal(VecDeque<String>),
    StreamVal(Stream),
}
pub enum PushType {
    Left,
    Right,
}
pub struct BlpopData {
    pub channel: oneshot::Sender<Vec<String>>,
    pub deadline: Instant,
}
pub struct Store {
    pub kv: HashMap<String, StoredVal>,
    pub keys_with_expiry: Vec<(String, Instant)>,
    pub index_map: HashMap<String, usize>,
    pub list_waiters: HashMap<String, VecDeque<BlpopData>>,
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
        let list_waiters: HashMap<String, VecDeque<BlpopData>> = HashMap::new();
        return Store {
            kv,
            keys_with_expiry,
            index_map,
            list_waiters,
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
    ) -> Result<usize, RedisParseError> {
        let list_val = self.kv.entry(list_key.clone()).or_insert(StoredVal {
            val: RedisValue::ListVal(VecDeque::new()),
            expiry: None,
        });
        let list = match &mut list_val.val {
            RedisValue::ListVal(lst) => lst,
            _ => Err(RedisParseError::WrongType(format!(
                "Key: {} already exists in kv store and the value for the key is not compatible with this operation `RPUSH` / `LPUSH` which requires the value to be a list",
                list_key
            )))?
        };
        let mut to_append_queue = VecDeque::from(to_append);
        let len_to_append = to_append_queue.len().clone();
        println!("Push command received");
        println!("Append queue:");
        for val in to_append_queue.clone() {
            println!("{}", val);
        }

        loop {
            if let Some(queued_waiters) = self.list_waiters.get_mut(list_key.as_str()) {
                if to_append_queue.len() > 0 {
                    let next_el = to_append_queue.pop_front().unwrap();

                    let oldest_waiter = queued_waiters.pop_front();
                    if oldest_waiter.is_some() {
                        println!("Oldest waiter exists");
                        let waiter_data = oldest_waiter.unwrap();
                        let tx = waiter_data.channel;
                        let mut res: Vec<String> = Vec::new();
                        println!("Sending {} through channel to list waiter", next_el);
                        res.push(list_key.clone());
                        res.push(next_el);
                        match tx.send(res) {
                            Ok(()) => println!("send OK to waiter for {}", list_key),
                            Err(returned) => println!(
                                "send FAILED (receiver gone) for {}: {:?}",
                                list_key, returned
                            ),
                        }
                    } else {
                        // if there are no waiters for the push continue with the push from
                        // to_append queue
                        to_append_queue.push_front(next_el);
                        break;
                    }
                } else {
                    // if to_append_queue is empty from the pushes then we can break out
                    return Ok(len_to_append);
                }
            } else {
                // if there are no list waiters for the list key
                break;
            }
        }
        match push_type {
            PushType::Left => {
                for el in to_append_queue {
                    list.push_front(el);
                }
            }
            PushType::Right => {
                for el in to_append_queue {
                    list.push_back(el);
                }
            }
        }
        Ok(list.len())
    }
    pub fn lrange(
        &mut self,
        list_key: String,
        start_: i32,
        stop_: i32,
    ) -> Result<Vec<String>, RedisParseError> {
        let stored = match self.kv.get(&list_key) {
            None => return Ok(Vec::new()),
            Some(whole_list) => whole_list,
        };
        let RedisValue::ListVal(list) = &stored.val else {
            return Err(RedisParseError::WrongType(format!(
                "Key: {} already exists in kv store and the value for the key is not compatible with this operation `LRANGE` which requires the value to be a list",
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
    pub fn lpop(&mut self, list_key: String, num_to_pop: usize) -> Result<Vec<String>, RedisParseError> {
        let mut entry = match self.kv.entry(list_key.clone()) {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(_) => return Ok(Vec::new()),
        };
        let stored_val = &mut entry.get_mut().val;
        match stored_val {
            RedisValue::ListVal(deque) => {
                let mut res: Vec<String> = Vec::new();
                let num_take = if deque.len() < num_to_pop {
                    deque.len()
                } else {
                    num_to_pop
                };
                for _ in 0..num_take {
                    res.push(deque.pop_front().unwrap());
                }
                Ok(res)
            }
            _ => return Err(RedisParseError::WrongType(format!(
                "Key: {} already exists in kv store and the value for the key is not compatible with this operation `LPOP` which requires the value to be a list",
                list_key,
            )))
        }
    }
    pub fn register_waiter(
        &mut self,
        list_key: String,
        timeout: f64,
    ) -> oneshot::Receiver<Vec<String>> {
        let deadline = Instant::now() + Duration::from_secs_f64(timeout);
        let (tx, rx): (oneshot::Sender<Vec<String>>, oneshot::Receiver<Vec<String>>) =
            oneshot::channel();
        let data = BlpopData {
            channel: tx,
            deadline,
        };
        let list_waiter_entry = self
            .list_waiters
            .entry(list_key.clone())
            .or_insert(VecDeque::new());
        list_waiter_entry.push_back(data);
        rx
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
    ) -> Result<(), RedisParseError> {
        let mut expiry: Option<Instant> = None;
        if let Some(opts) = options {
            for opt in opts {
                match opt {
                    SetOptions::EX(arg) => {
                        let timeout = arg.parse::<u64>().map_err(|_| {
                            RedisParseError::InvalidArgumentForCommandOption(
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
                            RedisParseError::InvalidArgumentForCommandOption(
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
    ) -> Result<usize, RedisParseError> {
        let mut locked_ref = self.db.lock().unwrap();
        locked_ref.push(list_key, to_append, push_type)
    }
    pub fn lrange(
        &self,
        list_key: String,
        start: i32,
        stop: i32,
    ) -> Result<Vec<String>, RedisParseError> {
        let mut locked_ref = self.db.lock().unwrap();
        locked_ref.lrange(list_key, start, stop)
    }
    pub fn llen(&self, list_key: String) -> Result<Option<usize>, RedisParseError> {
        let stored_val = match self.get(list_key.clone()) {
            None => return Ok(None),
            Some(val) => val,
        };
        let result = match stored_val {
            RedisValue::ListVal(lst) => lst.len(),
            _ => return Err(RedisParseError::WrongType(format!(
                "Key: {} already exists in kv store and the value for the key is not compatible with this operation `LLEN` which requires the value to be a list",
                list_key,
            )))
        };
        Ok(Some(result))
    }
    pub fn lpop(&self, list_key: String, num_to_pop: usize) -> Result<Vec<String>, RedisParseError> {
        let mut locked_ref = self.db.lock().unwrap();
        locked_ref.lpop(list_key, num_to_pop)
    }
    pub async fn blpop(&self, list_key: String, time_stop: f64) -> Result<Vec<String>, RedisParseError> {
        let mut rx = {
            let mut locked_ref = self.db.lock().unwrap();
            let option_el = match locked_ref.lpop(list_key.clone(), 1) {
                Ok(v) => v.into_iter().next(),
                Err(e) => Err(e)?,
            };
            match option_el {
                Some(x) => return Ok(vec![list_key.to_string(), x]),
                None => locked_ref.register_waiter(list_key.clone(), time_stop),
            }
        };
        println!("Got rx now waiting for pushes to list key: {}", list_key);
        if time_stop != 0.0 {
            match timeout(Duration::from_secs_f64(time_stop), &mut rx).await {
                Ok(v) => match v {
                    Ok(lpop_res) => return Ok(lpop_res),
                    Err(_) => Err(RedisParseError::IoError(std::io::Error::other(
                        "Receiver dropped for BLPOP operation",
                    ))),
                },
                Err(_e) => {
                    println!("Nothing received by receiver in one-shot channel");
                    return Ok(Vec::new());
                }
            }
        } else {
            match rx.await {
                Ok(v) => return Ok(v),
                Err(_) => Err(RedisParseError::IoError(std::io::Error::other(
                    "Receiver dropped for BLPOP operation",
                ))),
            }
        }
    }
    pub fn xadd(self, stream_name: String, node_id: String, data: HashMap<String, String>) -> Result<String, RedisError> {
        let mut db = self.db.lock().unwrap();
        let store = &mut db.kv;

        let default = StoredVal{
            val: RedisValue::StreamVal(Stream::new()),
            expiry: None,
        };
        let stored_val = store.entry(stream_name.clone()).or_insert(default);
        let stored_stream = match &mut stored_val.val {
            RedisValue::StreamVal(stream_val) => stream_val,
            _ => return Err(Parse(RedisParseError::WrongType(format!(
                "Key: {} already exists in kv store and the value for the key is not compatible with this operation `XADD` which requires the value to be a stream",
                stream_name,
            ))))
        };
        if !is_valid_id(&node_id) {
            return Err(StreamInsert(RedisStreamInsertError::InvalidStreamId(node_id)));
        }
        // insert id is now of the correct form * or [/d]+-* or [/d]+-[/d]

        // insert id after we have replaced the * in the whole id or * in the sequence number
        let to_insert_id = autogenerate_id(&node_id, &stored_stream);
        if after_last_id(&stored_stream, &to_insert_id) {
            stored_stream.insert(to_insert_id, Some(data));
            return Ok(stored_stream.last_id.clone());
        } else {
            return Err(StreamInsert(RedisStreamInsertError::StreamIdTooOld(to_insert_id, stored_stream.last_id.clone(), stream_name)));
        }
    }

    
    pub fn get_type(self, key_name: String) -> String {
        let val = self.get(key_name);
        if val.is_none() {
            return "none".to_string();
        }
        match val.unwrap() {
            RedisValue::StringVal(_) => return "string".to_string(),
            RedisValue::ListVal(_) => return "list".to_string(),
            RedisValue::StreamVal(_) => return "stream".to_string(),
        }
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
fn autogenerate_id(insert_id: &String, stream: &Stream) -> String {
    let last_id = &stream.last_id;
    if !insert_id.contains("*") {
        return insert_id.clone();
    }
    let last_id_parts: Vec<&str> = last_id.split('-').collect();
    let last_milli_time = last_id_parts.get(0).unwrap();
    let seq_number = last_id_parts.get(1).unwrap().parse::<u32>().unwrap();
    let last_milli_time_parsed = last_milli_time.parse::<u64>().unwrap();

    if insert_id.chars().nth(0).unwrap() == '*' {
        // entire id needs to be autogenerated
        let timestamp = Utc::now().timestamp_millis() as u64;
        let gen_seq_number = if timestamp == last_milli_time_parsed {
            seq_number + 1
        } else {
            0
        };
        return format!("{}-{}", timestamp, gen_seq_number);
    } else {
        // it ends with the * so only the sequence number is autogenerated
        
        let insert_id_parts: Vec<&str> = insert_id.split('-').collect();
        if last_milli_time == insert_id_parts.get(0).unwrap() {
            return format!("{}-{}", last_milli_time, seq_number+1);
        } else {
            return format!("{}-{}", insert_id_parts.get(0).unwrap(), 0);
        }
    }
}
fn after_last_id(stream: &Stream, insert_id: &String) -> bool {
    let last_id = &stream.last_id;
    let last_id_parts: Vec<&str> = last_id.split('-').collect();
    let last_milli_time = last_id_parts.get(0).unwrap().parse::<u64>().unwrap();
    let seq_number: u32 = last_id_parts.get(1).unwrap().parse::<u32>().unwrap();

    let insert_id_parts: Vec<&str> = insert_id.split('-').collect();
    let insert_id_milli_time: u64 = insert_id_parts.get(0).unwrap().parse::<u64>().unwrap();
    let insert_id_seq_number: u32 = insert_id_parts.get(1).unwrap().parse::<u32>().unwrap();

    if insert_id_milli_time > last_milli_time {
        return true;
    } else if insert_id_milli_time == last_milli_time && insert_id_seq_number > seq_number {
        return true;
    } else {
        return false;
    }
}
fn is_valid_id(insert_id: &String) -> bool {
    if insert_id.starts_with("*") {
        if insert_id.len() != 1 {
            return false;
        }
        return true;
    }
    let id_parts: Vec<&str> = insert_id.split("-").collect();
    if id_parts.len() != 2 {
        return false;
    }
    for ch in id_parts[0].chars() {
        if !ch.is_ascii_digit() {
            return false;
        }
    }
    if id_parts[1] == "*" {
        return true;
    }
    for ch in id_parts[1].chars() {
        if !ch.is_ascii_digit() {
            return false;
        }
    }
    return true;


}