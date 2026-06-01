use crate::redis_error::RedisError;
#[derive(Clone)]
pub enum CommandType {
    PING,
    ECHO,
}
#[derive(Clone)]
pub struct Command {
    pub command_name: CommandType,
    pub command_args: Vec<String>,
}
pub enum RedisResponse {
    SimpleString(String),
    BulkString(String),
}
pub fn command_executor(command: Command) -> Result<RedisResponse, RedisError> {
    for i in 0..command.command_args.len() {
        println!("{}", command.command_args[i].to_string());
    }
    let res = match command.command_name {
        CommandType::PING => RedisResponse::SimpleString(String::from("PONG")),
        CommandType::ECHO => {
            RedisResponse::BulkString(String::from(command.command_args[1].clone()))
        }
    };
    Ok(res)
}
