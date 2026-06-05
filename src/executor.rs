use crate::{
    data_store::{self, KvStore, SetOptionList, SetOptions},
    redis_error::RedisError,
};
#[derive(Clone)]
pub enum CommandType {
    PING,
    ECHO,
    SET,
    GET,
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
}
pub fn command_executor(command: Command, db: KvStore) -> Result<RedisResponse, RedisError> {
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
                db.set(key.to_string(), value.to_string(), set_opts);
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
                    Some(value) => RedisResponse::BulkString(value),
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
    };
    Ok(res)
}
