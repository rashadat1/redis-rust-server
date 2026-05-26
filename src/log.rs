use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    sync::mpsc,
    thread,
};

pub fn init_logger() -> mpsc::Sender<String> {
    let (tx, rx) = mpsc::channel::<String>();
    thread::spawn(move || {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("server.log")
            .expect("failed to open log file");
        let mut writer = BufWriter::new(file);

        while let Ok(line) = rx.recv() {
            if let Err(e) = writer.write_all(format!("{}\n", line).as_bytes()) {
                eprintln!("Error writing log: {}", e);
                continue;
            }
            writer.flush().unwrap();
        }
    });

    
    tx
}
