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
    MalformedRequest(i32, String),
    InvalidInteger(i32, String),
    OutOfBytes,
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
            RedisError::MalformedRequest(pos, str) => {
                write!(f, "Malformed RESP request at position {} in: {}", pos, str)
            }
            RedisError::InvalidInteger(pos, str) => {
                write!(
                    f,
                    "Invalid Integer in RESP request at position {} in: {}",
                    pos, str
                )
            }
            RedisError::OutOfBytes => {
                write!(
                    f,
                    "Reached end of bytes in accumulator without hitting the end of the command"
                )
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
    accumulator: &mut Vec<u8>,
    pos: &mut usize,
) -> Result<ParsedCommand, RedisError> {
    let mut command_args: Vec<String> = Vec::new();
    let initial_position = *pos; // copy the position into another variable
    loop {
        // loop to deterine number of elements in the array
        if let (Some(next), Some(next_next)) =
            (accumulator.get(*pos + 1), accumulator.get(*pos + 2))
        {
            if *next == 0x0D && *next_next == 0x0A {
                break;
            }
            *pos += 1;
        } else {
            Err(RedisError::OutOfBytes)?;
        }
    }
    let num_element_bytes = &accumulator[initial_position..=*pos];
    let num_elements_string = str::from_utf8(&num_element_bytes).map_err(|_| {
        RedisError::MalformedRequest(
            *pos as i32,
            String::from_utf8_lossy(&accumulator).to_string(),
        )
    })?;
    let num_elements = num_elements_string
        .parse::<usize>()
        .map_err(|_| RedisError::InvalidInteger(*pos as i32, num_elements_string.to_string()))?;

    *pos += 3;
    for _ in 0..num_elements {
        if let Some(data_type_byte) = accumulator.get(*pos) {
            match data_type_byte {
                b'$' => {
                    *pos += 1;
                    let bulk_string = parse_bulk_string(accumulator, pos)?;
                    command_args.push(bulk_string);
                }
                _ => Err(RedisError::UnknownRESPDataType(
                    *pos as i32,
                    String::from_utf8_lossy(&accumulator[initial_position..=*pos]).to_string(),
                ))?,
            };
        };
    }
    Ok(ParsedCommand {
        command_name: command_args.get(0).unwrap().to_string(),
        command_args: command_args,
    })
}
fn parse_bulk_string(accumulator: &mut Vec<u8>, pos: &mut usize) -> Result<String, RedisError> {
    let current_position = *pos;
    loop {
        if let (Some(next), Some(next_next)) =
            (accumulator.get(*pos + 1), accumulator.get(*pos + 2))
        {
            if *next == 0x0D && *next_next == 0x0A {
                break;
            }
            *pos += 1;
        }
    }
    let string_length_bytes = &accumulator[current_position..=*pos];
    let string_length_string = str::from_utf8(string_length_bytes).map_err(|_| {
        RedisError::MalformedRequest(
            *pos as i32,
            String::from_utf8_lossy(string_length_bytes).to_string(),
        )
    })?;
    let string_length = string_length_string
        .parse::<usize>()
        .map_err(|_| RedisError::InvalidInteger(*pos as i32, string_length_string.to_string()))?;

    *pos += 3; // $4\r\nbark\r\n
    let bulk_string = str::from_utf8(&accumulator[*pos..(*pos + string_length)]).map_err(|_| {
        RedisError::MalformedRequest(
            *pos as i32,
            String::from_utf8_lossy(&accumulator[*pos..(*pos + string_length)]).to_string(),
        )
    })?;
    Ok(bulk_string.to_string())
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
                parse_resp_array(&mut accumulator, &mut curr);
            }
            _ => {
                let e = RedisError::UnknownRESPDataType(
                    0,
                    String::from_utf8_lossy(&accumulator[..n]).to_string(),
                );
                tx.send(format!("{}", e)).unwrap();
            }
        };
    }
    Ok(())
}
