use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

pub fn init_logger() -> mpsc::Sender<String> {
    let (tx, mut rx) = mpsc::channel::<String>(1024);
    tokio::spawn(async move {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("server.log")
            .await
            .expect("failed to open log file");

        while let Some(line) = rx.recv().await {
            let _ = file.write_all(line.as_bytes()).await;
        }
        let _ = file.flush().await;
    });
    tx
}
