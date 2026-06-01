use crate::redis_error::RedisError;

pub fn parse_resp_array(
    accumulator: &mut Vec<u8>,
    pos: &mut usize,
) -> Result<Vec<String>, RedisError> {
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
                    } else {
                        command_args.push(bulk_string);
                    }
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
pub fn parse_bulk_string(accumulator: &mut Vec<u8>, pos: &mut usize) -> Result<String, RedisError> {
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
