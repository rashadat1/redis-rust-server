use std::fmt;
#[derive(Debug)]
pub enum RedisError {
    IoError(std::io::Error),
    UnknownRESPDataType(i32, String, Option<String>),
    MalformedRequest(i32, String, Option<String>),
    InvalidInteger(String, String, Option<String>),
    OutOfBytes(String, Option<String>),
    UnimplementedCommandType(String),
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
