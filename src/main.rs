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
    UnknownRESPDataType(i32, String, Option<String>),
    MalformedRequest(i32, String, Option<String>),
    InvalidInteger(String, String, Option<String>),
    OutOfBytes(String, Option<String>),
    UnimplementedCommandType(String),
}
enum CommandType {
    PING,
}
struct ParsedCommand {
    command_name: CommandType,
    command_args: Vec<String>,
}
impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RedisError::IoError(e) => {
                write!(f, "IoError: {}", e)
            }
            RedisError::UnknownRESPDataType(pos, str, msg) => {
                if let Some(msg_) = msg {
                    write!(f, "{} in: {} at {}", msg_, str, pos)
                } else {
                    write!(f, "Unknown RESP data type at position {} in: {}", pos, str)
                }
            }
            RedisError::MalformedRequest(pos, str, msg) => {
                if let Some(msg_) = msg {
                    write!(f, "{} in: {} at {}", msg_, str, pos)
                } else {
                    write!(f, "Malformed RESP request at position {} in: {}", pos, str)
                }
            }
            RedisError::InvalidInteger(str, error_loc, msg) => {
                if let Some(msg_) = msg {
                    write!(f, "{} at: {} in {}", msg_, error_loc, str)
                } else {
                    write!(
                        f,
                        "Invalid Integer in RESP request at position {} in: {}",
                        error_loc, str
                    )
                }
            }
            RedisError::OutOfBytes(str, msg) => {
                if let Some(msg_) = msg {
                    write!(f, "{}: As a string, the bytes read were: {}", msg_, str)
                } else {
                    write!(
                        f,
                        "Reached end of bytes in accumulator without hitting the end of the command: {}",
                        str
                    )
                }
            }
            RedisError::UnimplementedCommandType(str) => {
                write!(f, "Received Unimplemented RESP command: {}", str)
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
fn parse_resp_array(accumulator: &mut Vec<u8>, pos: &mut usize) -> Result<Vec<String>, RedisError> {
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
            Err(RedisError::OutOfBytes(
                String::from_utf8_lossy(&accumulator[..]).to_string(),
                Some(
                    "Out of Bytes Error: Reached end of command read from buffer before hitting a terminal CRLF, will try to read more bytes".to_string(),
                ),
            ))?;
        }
    }
    let num_element_bytes = &accumulator[initial_position..=*pos];
    let num_elements_string = str::from_utf8(&num_element_bytes).map_err(|_| {
        RedisError::MalformedRequest(
            *pos as i32,
            String::from_utf8_lossy(&accumulator).to_string(),
            Some("Malformed Request Error: Invalid UTF8 string encoded by bytes representing number of elements in RESP array".to_string()),
        )
    })?;
    let num_elements = num_elements_string
        .parse::<usize>()
        .map_err(|_| RedisError::InvalidInteger(String::from_utf8_lossy(&accumulator[..accumulator.len()]).to_string(), num_elements_string.to_string(), Some("Invalid Integer Error: Expected an integer but received non-integer string in place denoting number of elements in RESP array".to_string())))?;

    *pos += 3;
    for i in 0..num_elements {
        if let Some(data_type_byte) = accumulator.get(*pos) {
            match data_type_byte {
                b'$' => {
                    *pos += 1;
                    let bulk_string = parse_bulk_string(accumulator, pos)?;
                    if i == 0 {
                        command_args.push(bulk_string.to_ascii_uppercase()); // uppercase command
                        // the first argument
                    }
                    command_args.push(bulk_string);
                }
                _ => Err(RedisError::UnknownRESPDataType(
                    *pos as i32,
                    String::from_utf8_lossy(&accumulator[initial_position..=*pos]).to_string(),
                    Some("Unknown RESP Data Type Error: RESP array contains an array element of invalid data type".to_string())
                ))?,
            };
        } else {
            Err(RedisError::OutOfBytes(
                String::from_utf8_lossy(&accumulator[..accumulator.len()]).to_string(),
                Some(format!(
                    "Out of Bytes Error: Reached end of bytes read from buffer into accumulator before processing expected number of RESP array elements ({})",
                    num_elements
                )),
            ))?;
        }
    }
    Ok(command_args)
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
        } else {
            Err(RedisError::OutOfBytes(
                String::from_utf8_lossy(&accumulator[..]).to_string(),
                Some(
                    "Out of Bytes Error: Reached end of command read from buffer before hitting a terminal CRLF, will try to read more bytes".to_string(),
            )))?;
        }
    }
    let string_length_bytes = &accumulator[current_position..=*pos];
    let string_length_string = str::from_utf8(string_length_bytes).map_err(|_| {
        RedisError::MalformedRequest(
            *pos as i32,
            String::from_utf8_lossy(string_length_bytes).to_string(),
            Some("Malformed Request Error: Invalid UTF8 string encoded by bytes representing number of elements in RESP bulk string".to_string()),
        )
    })?;
    let string_length = string_length_string
        .parse::<usize>()
        .map_err(|_| RedisError::InvalidInteger(String::from_utf8_lossy(&accumulator[..accumulator.len()]).to_string(), string_length_string.to_string(), Some("Invalid Integer Error: Expected an integer but received non-integer string in place denoting number of elements in RESP bulk string".to_string())))?;

    *pos += 3; // $4\r\nbark\r\n
    let bulk_string = str::from_utf8(&accumulator[*pos..(*pos + string_length)]).map_err(|_| {
        RedisError::MalformedRequest(
            *pos as i32,
            String::from_utf8_lossy(&accumulator[*pos..(*pos + string_length)]).to_string(),
            Some("Malformed Request Error: Invalid UTF8 string encoded by bytes representing RESP bulk string".to_string()),
        )
    })?;
    *pos += string_length + 2;
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
