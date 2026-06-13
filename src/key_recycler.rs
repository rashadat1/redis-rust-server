use std::{cmp::min, time::Instant};

use crate::data_store::KvStore;
use rand::seq::IndexedRandom;
use tokio::{
    sync::mpsc,
    time::{Duration, interval},
};

pub fn key_recycler(kv_store: KvStore, tx: mpsc::Sender<String>) {
    tokio::spawn(async move {
        let mut timer = interval(Duration::from_secs(10));
        loop {
            timer.tick().await;
            let to_log = active_cleanup_pass(&kv_store);
            for line in to_log {
                tx.send(line).await.unwrap_or_default();
            }
        }
    });
}
fn active_cleanup_pass(kv_store: &KvStore) -> Vec<String> {
    let mut store = kv_store.db.lock().unwrap();
    let mut log_lines: Vec<String> = Vec::new();
    if store.keys_with_expiry.len() == 0 {
        log_lines.push("[Active Cleanup Pass] No keys have an expiration: returning".to_string());
        return log_lines;
    }
    let mut k = 1;
    loop {
        let (expired_keys_sample, share_expired) =
            sample_keys_loop(&store.keys_with_expiry, &mut log_lines);
        log_lines.push(format!(
            "[Active Cleanup Pass] Removing the following expired keys: {}",
            list_keys(&expired_keys_sample)
        ));
        for exp_key in expired_keys_sample {
            store.remove(&exp_key);
        }
        if share_expired < 0.25 {
            log_lines.push(format!("[Active Cleanup Pass] Number of expired keys less than 25% of sample, ending loop after {} pass(es)", k));
            break;
        }
        k += 1
    }
    log_lines
}
fn sample_keys_loop(
    keys_expiry: &Vec<(String, Instant)>,
    log_lines: &mut Vec<String>,
) -> (Vec<String>, f32) {
    let mut rng = rand::rng();
    let now = Instant::now();
    let sample_size = min(20, keys_expiry.len());
    let sample: Vec<&(String, Instant)> = keys_expiry.sample(&mut rng, sample_size).collect();
    log_lines.push(format!(
        "[Active Cleanup Pass] Obtaining random sample of {} keys",
        sample_size
    ));
    let expired_keys_sampled: Vec<String> = sample
        .into_iter()
        .filter(|(_, instant)| *instant < now)
        .map(|(string_key, _)| string_key.clone())
        .collect();
    let num_keys_expired_from_sample = expired_keys_sampled.len();
    log_lines.push(format!(
        "[Active Cleanup Pass] Found {} expired keys in the random sample",
        num_keys_expired_from_sample,
    ));
    (
        expired_keys_sampled,
        num_keys_expired_from_sample as f32 / sample_size as f32,
    )
}
fn list_keys(keys: &Vec<String>) -> String {
    let mut res = "".to_string();
    for key in keys {
        res = format!("{}, {} ", res, key);
    }
    res
}
