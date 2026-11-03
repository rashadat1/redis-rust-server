use std::fmt;
#[derive(Debug)]

pub enum RedisError {
    Parse(RedisParseError),
    StreamInsert(RedisStreamInsertError),
}
#[derive(Debug)]
pub enum RedisParseError {
    IoError(std::io::Error),
    UnknownRESPDataType(i32, String, Option<String>),
    MalformedRequest(i32, String, Option<String>),
    InvalidInteger(String, String, Option<String>),
    OutOfBytes(String, Option<String>),
    UnimplementedCommandType(String),
    MissingArumentForOption(String, String),
    CommandMissingRequiredArguments(String, i32, i32),
    UnrecognizedCommandOption(String, String),
    InvalidArgumentForCommandOption(String, String, String, String),
    WrongType(String),
}
impl fmt::Display for RedisParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RedisParseError::IoError(e) => {
                write!(f, "{}", e)
            }
            RedisParseError::UnknownRESPDataType(pos, str, msg) => {
                if let Some(msg_) = msg {
                    write!(f, "{} in: {} at {}", msg_, str, pos)
                } else {
                    write!(f, "Unknown RESP data type at position {} in: {}", pos, str)
                }
            }
            RedisParseError::MalformedRequest(pos, str, msg) => {
                if let Some(msg_) = msg {
                    write!(f, "{} in: {} at {}", msg_, str, pos)
                } else {
                    write!(f, "Malformed RESP request at position {} in: {}", pos, str)
                }
            }
            RedisParseError::InvalidInteger(str, error_loc, msg) => {
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
            RedisParseError::OutOfBytes(str, msg) => {
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
            RedisParseError::UnimplementedCommandType(str) => {
                write!(f, "Received Unimplemented RESP command: {}", str)
            }
            RedisParseError::MissingArumentForOption(cmd, option) => {
                write!(
                    f,
                    "{} command has option {} specified but is missing a value for this option",
                    cmd, option
                )
            }
            RedisParseError::CommandMissingRequiredArguments(cmd, got, expect) => {
                write!(
                    f,
                    "{} command received {} argument(s) but expected {} argument(s)",
                    cmd, got, expect
                )
            }
            RedisParseError::UnrecognizedCommandOption(cmd, option) => {
                write!(f, "{} command received unrecognized option {}", cmd, option)
            }
            RedisParseError::InvalidArgumentForCommandOption(cmd, option, got, expected_type) => {
                write!(
                    f,
                    "{} command option {} expects an {} got value: {} which cannot be parsed as an {}",
                    cmd, option, expected_type, got, expected_type
                )
            }
            RedisParseError::WrongType(msg) => {
                write!(f, "{}", msg)
            }
        }
    }
}

#[derive(Debug)]

pub enum RedisStreamInsertError {
    StreamIdTooOld(String, String, String),
    InvalidStreamId(String),
}
impl fmt::Display for RedisStreamInsertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisStreamInsertError::StreamIdTooOld(to_insert_id, last_id, stream_key) => {
                write!(f, "{} cannot be inserted because it's timestamp or seq number is earlier than {}, the last inserted id of the stream with key {}",
                    to_insert_id, last_id, stream_key
                )
            }
            RedisStreamInsertError::InvalidStreamId(to_insert_id) => {
                write!(f, "Invalid insert id for a stream {}", to_insert_id)
            }
        }
    }
}