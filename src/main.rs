mod log;
mod parser;
mod redis_error;
use std::{
    io::Read,
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

use crate::log::init_logger;
use crate::parser::parse_resp_array;
use crate::redis_error::RedisError;

enum CommandType {
    PING,
}
struct ParsedCommand {
    command_name: CommandType,
    command_args: Vec<String>,
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
                    format!(
                        "\nServer accepted new connection from: {}",
                        addr.to_string()
                    )
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
                            format!("Closed connection with client: {}\n", addr.to_string())
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
                let result = parse_resp_array(&mut accumulator, &mut curr);
                match result {
                    Err(RedisError::OutOfBytes(str, msg)) => {
                        if let Some(msg_) = msg {
                            tx.send(format!(
                                "{}: Read the following bytes as a string: {}",
                                msg_, str
                            ))
                            .unwrap_or_default();
                        }
                    }
                    Err(e) => {
                        tx.send(format!("{}", e)).unwrap_or_default();
                        tx.send("Clearing accumulator".to_string())
                            .unwrap_or_default();
                        accumulator.clear();
                        return Err(e.into());
                    }
                    Ok(parsed_command) => {
                        accumulator.clear();
                        if let None = parsed_command.get(0) {
                            Err(RedisError::MalformedRequest(
                                0,
                                String::from_utf8_lossy(&accumulator[..accumulator.len()])
                                    .to_string(),
                                Some(
                                    "Malformed Request Error: RESP Command lacks command name"
                                        .to_string(),
                                ),
                            ))?;
                        }
                        println!("Received {} command", parsed_command[0]);
                        tx.send("Registered PING command".to_string())
                            .unwrap_or_default();
                        let command_type = match parsed_command[0].as_str() {
                            "PING" => CommandType::PING,
                            _ => {
                                let e =
                                    RedisError::UnimplementedCommandType(parsed_command[0].clone());
                                tx.send(format!("{}", e)).unwrap_or_default();
                                Err(e.into())?
                            }
                        };
                        let command = ParsedCommand {
                            command_name: command_type,
                            command_args: parsed_command,
                        };
                        // command executor
                        // command responder
                    }
                }
            }
            _ => {
                let e = RedisError::UnknownRESPDataType(
                    0,
                    String::from_utf8_lossy(&accumulator[..n]).to_string(),
                    Some(
                        "RESP Commands are expected to begin with '*' and hence be RESP arrays"
                            .to_string(),
                    ),
                );
                tx.send(format!(
                    "{}, RESP Commands are expected to begin with '*'",
                    e
                ))
                .unwrap_or_default();
            }
        };
    }
    Ok(())
}
