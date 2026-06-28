mod data_store;
mod executor;
mod key_recycler;
mod log;
mod parser;
mod redis_error;
mod resp_serializer;

use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::key_recycler::key_recycler;
use crate::log::init_logger;
use crate::parser::parse_resp_array;
use crate::redis_error::RedisError;
use crate::{
    data_store::KvStore,
    executor::{Command, CommandType, command_executor},
    resp_serializer::command_responder,
};
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let tx = init_logger();
    let kv_store = KvStore::new();
    key_recycler(kv_store.clone(), tx.clone());
    println!("[Redis Server] Server listening on port 6379");
    tx.send("Server started".to_string())
        .await
        .unwrap_or_default();
    loop {
        match listener.accept().await {
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
                .await
                .unwrap_or_default();
                let mut tx2 = tx.clone();
                let kv_store2 = kv_store.clone();
                tokio::spawn(async move {
                    match handle_connection(socket, &mut tx2, kv_store2).await {
                        Ok(()) => {
                            println!(
                                "[Redis Server] Client request handled, closing connection with: {}",
                                addr.to_string()
                            );
                            tx2.send(
                                format!("Closed connection with client: {}\n", addr.to_string())
                                    .to_string(),
                            )
                            .await
                            .unwrap_or_default();
                        }
                        Err(redis) => {
                            eprintln!("{}", redis);
                            tx2.send(
                                format!("Error handling client request: {}", redis).to_string(),
                            )
                            .await
                            .unwrap_or_default();
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("[Redis Server] Error accepting connection exiting: {}", e)
            }
        }
    }
}
async fn handle_connection(
    mut socket: TcpStream,
    tx: &mut mpsc::Sender<String>,
    db: KvStore,
) -> Result<(), RedisError> {
    let mut read_buf = vec![0u8; 1024];
    let mut accumulator: Vec<u8> = Vec::new();
    loop {
        let n = socket
            .read(&mut read_buf)
            .await
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
                            .await
                            .unwrap_or_default();
                        }
                    }
                    Err(e) => {
                        tx.send(format!("{}", e)).await.unwrap_or_default();
                        tx.send("Clearing accumulator".to_string())
                            .await
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
                        tx.send(format!("Registered {} command", parsed_command[0]))
                            .await
                            .unwrap_or_default();
                        let command_type = match parsed_command[0].as_str() {
                            "PING" => CommandType::PING,
                            "ECHO" => CommandType::ECHO,
                            "SET" => CommandType::SET,
                            "GET" => CommandType::GET,
                            "RPUSH" => CommandType::RPUSH,
                            _ => {
                                let e =
                                    RedisError::UnimplementedCommandType(parsed_command[0].clone());
                                tx.send(format!("{}", e)).await.unwrap_or_default();
                                Err(e.into())?
                            }
                        };
                        let command = Command {
                            command_name: command_type,
                            command_args: parsed_command,
                        };
                        match command_executor(command.clone(), db.clone()) {
                            Ok(response) => {
                                let _ = command_responder(response, &mut socket).await;
                            }
                            Err(e) => {
                                tx.send(format!("{}", e)).await.unwrap_or_default();
                                Err(e.into())?
                            }
                        }
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
                .await
                .unwrap_or_default();
            }
        };
    }
    Ok(())
}
