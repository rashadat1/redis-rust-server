use core::f64;
use std::usize;

use crate::{
    data_store::{KvStore, PushType, RedisValue, SetOptionList, SetOptions},
    redis_error::RedisError,
};
#[derive(Clone)]
pub enum CommandType {
    PING,
    ECHO,
    SET,
    GET,
    RPUSH,
    LRANGE,
    LPUSH,
    LLEN,
    LPOP,
    BLPOP,
    TYPE,
    XADD,
}
#[derive(Clone)]
pub struct Command {
    pub command_name: CommandType,
    pub command_args: Vec<String>,
}
pub enum RedisResponse {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Integer(i32),
    BulkStringArray(Vec<String>),
    NullBulkArray,
}
pub async fn command_executor(command: Command, db: KvStore) -> Result<RedisResponse, RedisError> {
    let res = match command.command_name {
        CommandType::PING => RedisResponse::SimpleString(String::from("PONG")),
        CommandType::ECHO => {
            RedisResponse::BulkString(String::from(command.command_args[1].clone()))
        }
        CommandType::SET => {
            let mut opt: SetOptionList = Vec::new();
            let cmd_name = &command.command_args[0];
            if let (Some(key), Some(value)) =
                (command.command_args.get(1), command.command_args.get(2))
            {
                // loop through the potential list of SET command options after the k-v pair
                let mut index: usize = 3;
                loop {
                    if let Some(option) = command.command_args.get(index) {
                        index += 1;
                        if let Some(option_val) = command.command_args.get(index) {
                            let option_arg = match option.as_str() {
                                "EX" => SetOptions::EX(option_val.to_string()),
                                "PX" => SetOptions::PX(option_val.to_string()),
                                _ => Err(RedisError::UnrecognizedCommandOption(
                                    cmd_name.to_string(),
                                    option.to_string(),
                                ))?,
                            };
                            opt.push(option_arg);
                            index += 1;
                        } else {
                            Err(RedisError::MissingArumentForOption(
                                cmd_name.to_string(),
                                option.to_string(),
                            ))?
                        }
                    } else {
                        break;
                    }
                }
                let set_opts = match opt.len() {
                    0 => None,
                    _ => Some(opt),
                };
                let _ = db.set(key.to_string(), value.to_string(), set_opts);
                RedisResponse::SimpleString(String::from("OK"))
            } else {
                Err(RedisError::CommandMissingRequiredArguments(
                    cmd_name.to_string(),
                    (command.command_args.len() - 1) as i32,
                    2,
                ))?
            }
        }
        CommandType::GET => {
            let cmd_name = &command.command_args[0];
            if let Some(key) = command.command_args.get(1) {
                match db.get(key.to_string()) {
                    Some(value) => match value {
                        RedisValue::StringVal(str) => RedisResponse::BulkString(str),
                        RedisValue::ListVal(_) => RedisResponse::NullBulkString,
                    },
                    None => RedisResponse::NullBulkString,
                }
            } else {
                Err(RedisError::CommandMissingRequiredArguments(
                    cmd_name.to_string(),
                    0,
                    1,
                ))?
            }
        }
        CommandType::RPUSH => {
            let mut to_append: Vec<String> = Vec::new();
            let cmd_name = &command.command_args[0];
            if let Some(list_key) = command.command_args.get(1) {
                let mut i = 2;
                loop {
                    if let Some(el) = command.command_args.get(i) {
                        to_append.push(el.to_string());
                        i += 1;
                    } else {
                        break;
                    }
                }
                match db.push(list_key.to_string(), to_append, PushType::Right) {
                    Ok(len_list) => RedisResponse::Integer(len_list as i32),
                    Err(e) => Err(e)?,
                }
            } else {
                Err(RedisError::CommandMissingRequiredArguments(
                    cmd_name.to_string(),
                    0,
                    1,
                ))?
            }
        }
        CommandType::LRANGE => {
            let cmd_name = command.command_args[0].clone();
            if let (Some(list_key), Some(start_str), Some(stop_str)) = (
                command.command_args.get(1),
                command.command_args.get(2),
                command.command_args.get(3),
            ) {
                let start = start_str.parse::<i32>().map_err(|_| {
                    RedisError::WrongType(format!(
                        "LRANGE command argument 2 (start): {} cannot be parsed as an integer",
                        start_str
                    ))
                })?;
                let stop = stop_str.parse::<i32>().map_err(|_| {
                    RedisError::WrongType(format!(
                        "LRANGE command arument 3 (stop): {} cannot be parsed as an integer",
                        stop_str
                    ))
                })?;
                let sliced_vec = match db.lrange(list_key.to_string(), start, stop) {
                    Err(e) => return Err(e)?,
                    Ok(response) => response,
                };
                RedisResponse::BulkStringArray(sliced_vec)
            } else {
                Err(RedisError::CommandMissingRequiredArguments(
                    cmd_name,
                    (command.command_args.len() - 1) as i32,
                    3,
                ))?
            }
        }
        CommandType::LPUSH => {
            let mut to_append: Vec<String> = Vec::new();
            let cmd_name = command.command_args[0].clone();
            let list_key = match command.command_args.get(1) {
                None => Err(RedisError::CommandMissingRequiredArguments(
                    cmd_name,
                    (command.command_args.len() - 1) as i32,
                    1,
                ))?,
                Some(x) => x,
            };
            let mut i = 2;
            loop {
                if let Some(el) = command.command_args.get(i) {
                    to_append.push(el.to_string());
                    i += 1;
                } else {
                    break;
                }
            }
            match db.push(list_key.to_string(), to_append, PushType::Left) {
                Ok(len_list) => RedisResponse::Integer(len_list as i32),
                Err(e) => Err(e)?,
            }
        }
        CommandType::LLEN => {
            let command_name = &command.command_args[0];
            let list_key = match command.command_args.get(1) {
                None => Err(RedisError::CommandMissingRequiredArguments(
                    command_name.to_string(),
                    (command.command_args.len() - 1) as i32,
                    1,
                ))?,
                Some(str) => str,
            };
            let list_len = match db.llen(list_key.to_string()) {
                Ok(val) => match val {
                    None => 0,
                    Some(val) => val,
                },
                Err(e) => Err(e)?,
            };
            RedisResponse::Integer(list_len as i32)
        }
        CommandType::LPOP => {
            let command_name = &command.command_args[0];
            if let Some(list_key) = command.command_args.get(1) {
                let num = match command.command_args.get(2) {
                    Some(to_parse) => {
                        let parsed = to_parse.to_string().parse::<usize>().map_err(|_| RedisError::WrongType(format!("LPOP command argument 2 (num to pop): {} cannnot be parsed as an integer", to_parse)))?;
                        parsed
                    }
                    None => 1,
                };
                let res = match db.lpop(list_key.to_string(), num) {
                    Err(e) => Err(e)?,
                    Ok(vec) => vec,
                };
                if res.len() <= 1 {
                    RedisResponse::BulkString(res.get(0).unwrap_or(&String::from("")).to_string())
                } else {
                    RedisResponse::BulkStringArray(res)
                }
            } else {
                Err(RedisError::CommandMissingRequiredArguments(
                    command_name.to_string(),
                    (command.command_args.len() - 1) as i32,
                    1,
                ))?
            }
        }
        CommandType::BLPOP => {
            let command_name = &command.command_args[0];
            if let Some(list_key) = command.command_args.get(1) {
                let time_end = match command.command_args.get(2) {
                    Some(to_parse) => to_parse.parse::<f64>().map_err(|_| {
                        RedisError::WrongType(format!(
                            "BLPOP command argument 2 (timeout): {} cannot be parsed as a float",
                            to_parse
                        ))
                    })?,
                    None => Err(RedisError::CommandMissingRequiredArguments(
                        command_name.to_string(),
                        (command.command_args.len() - 1) as i32,
                        2,
                    ))?,
                };
                let res = match db.blpop(list_key.to_string(), time_end).await {
                    Err(e) => Err(e)?,
                    Ok(vec) => vec,
                };
                if res.len() > 0 {
                    RedisResponse::BulkStringArray(res)
                } else {
                    RedisResponse::NullBulkArray
                }
            } else {
                Err(RedisError::CommandMissingRequiredArguments(
                    command_name.to_string(),
                    (command.command_args.len() - 1) as i32,
                    2,
                ))?
            }
        }
        CommandType::TYPE => {
            let command_name = command.command_args[0].clone();
            if let Some(key_name) = command.command_args.get(1) {
                let type_val = db.get_type(key_name.to_string());
                RedisResponse::SimpleString(type_val)
            } else {
                Err(RedisError::CommandMissingRequiredArguments(
                    command_name.to_string(),
                    (command.command_args.len() - 1) as i32,
                    1,
                ))?
            }
        }
    };
    Ok(res)
}
