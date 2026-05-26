mod log;
use std::{
    fmt,
    io::Read,
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

use crate::log::init_logger;

enum RedisError {
    IoError(std::io::Error),
    UnknownRESPDataType(i32, String),
}
enum CommandType {
    PING,
}
struct ParsedCommand {
    command_name: String,
    command_args: Vec<String>,
}
impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RedisError::IoError(e) => {
                write!(f, "IoError: {}", e)
            }
            RedisError::UnknownRESPDataType(pos, str) => {
                write!(f, "Unknown RESP data type at position {} in: {}", pos, str)
            }
        }
    }
}
fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let tx = init_logger();
    println!("[Redis Server] Server listening on port 6379");
    tx.send("Server started".to_string()).unwrap();
    loop {
        match listener.accept() {
            Ok((socket, addr)) => {
                println!(
                    "[Redis Server] Accepted new connection from {}",
                    addr.to_string()
                );
                tx.send(
                    format!("Server accepted new connection from: {}", addr.to_string())
                        .to_string(),
                )
                .unwrap();
                match handle_connection(socket, tx.clone()) {
                    Ok(()) => {
                        println!(
                            "[Redis Server] Client request handled, closing connection with: {}",
                            addr.to_string()
                        );
                        tx.send(
                            format!("Closed connection with client: {}", addr.to_string())
                                .to_string(),
                        )
                        .unwrap();
                    }
                    Err(redis) => {
                        println!("{}", redis);
                        tx.send(format!("Error handling client request: {}", redis).to_string())
                            .unwrap();
                    }
                };
            }
            Err(e) => {
                println!("[Redis Server] Error accepting connection exiting: {}", e)
            }
        }
    }
}
fn parse_resp_array(
    accumulator: &Vec<u8>,
    mut curr_position: usize,
) -> Result<ParsedCommand, RedisError> {
    Ok(ParsedCommand {
        command_name: "foo".to_string(),
        command_args: Vec::new(),
    })
}
fn handle_connection(mut socket: TcpStream, tx: mpsc::Sender<String>) -> Result<(), RedisError> {
    let mut read_buf = vec![0u8; 1024];
    let mut accumulator: Vec<u8> = Vec::new();
    loop {
        let n = socket
            .read(&mut read_buf)
            .map_err(|e| RedisError::IoError(e))?;
        println!("Read {} bytes from the stream", n);
        if n == 0 {
            break;
        }
        accumulator.extend_from_slice(&read_buf[..n]);
        println!(
            "Stored {} bytes into the accumulator -> now holds {} bytes",
            n,
            accumulator.len()
        );
        let mut curr: usize = 0;
        match accumulator[curr] {
            // match the data type byte
            b'*' => {
                // Redis commands are serialized as arrays of bulk strings
                // the data type byte for arrays is '*'
                curr += 1;
                Ok(())
            }
            _ => {
                let e = RedisError::UnknownRESPDataType(
                    0,
                    String::from_utf8_lossy(&accumulator[..n]).to_string(),
                );
                tx.send(format!("{}", e)).unwrap();
                Err(e)
            }
        };
    }
    Ok(())
}
