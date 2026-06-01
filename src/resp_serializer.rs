use crate::{executor::RedisResponse, redis_error::RedisError};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub async fn command_responder(
    response: RedisResponse,
    socket: &mut TcpStream,
) -> Result<(), RedisError> {
    let serialized_response = resp_serializer(response);
    let response_bytes = serialized_response.as_bytes();
    socket
        .write(response_bytes)
        .await
        .map_err(|e| RedisError::IoError(e))?;
    println!("Wrote {} bytes to the stream", response_bytes.len());
    Ok(())
}
fn resp_serializer(response: RedisResponse) -> String {
    match response {
        RedisResponse::SimpleString(str) => format!("+{}\r\n", str),
        RedisResponse::BulkString(str) => format!("${}\r\n{}\r\n", str.len(), str),
    }
}
