# Redis Server Test Guide

This guide provides raw RESP (Redis Serialization Protocol) commands to test each supported command against this server implementation using `nc` (netcat).

Ensure your server is running on `127.0.0.1:6379` before executing these commands.

---

### 1. Connection & Utility Commands

#### PING
* **Description:** A simple connection health check command. The server should respond with `+PONG\r\n` if it is healthy and listening.
* **RESP Representation:** `*1\r\n$4\r\nPING\r\n`
* **Test Command:**
```bash
echo -e "*\(1\r\n\$4\r\nPING\r\\)n" | nc 127.0.0.1 6379
```

#### ECHO
* **Description:** Tests the server's ability to parse arguments and return data back to the client. The server should echo back the exact message argument provided as a bulk string.
* **RESP Representation:** `*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n`
* **Test Command:**
```bash
echo -e "*\(2\r\n\$4\r\nECHO\r\n\$5\r\nhello\r\\)n" | nc 127.0.0.1 6379
```

---

### 2. Key-Value (String) Commands

#### SET
* **Description:** Assigns a string value to a specified key. If the key already exists, the server overrides its current value and type, returning `+OK\r\n`.
* **RESP Representation:** `*3\r\n$3\r\nSET\r\n$4\r\nmykey\r\n$5\r\nworld\r\n`
* **Test Command:**
```bash
echo -e "*\(3\r\n\$3\r\nSET\r\n\$4\r\nmykey\r\n\$5\r\nworld\r\\)n" | nc 127.0.0.1 6379
```

#### GET
* **Description:** Retrieves the string value stored at the requested key. If the key does not exist, the server should return a null bulk string `$-1\r\n`.
* **RESP Representation:** `*2\r\n$3\r\nGET\r\n$4\r\nmykey\r\n`
* **Test Command:**
```bash
echo -e "*\(2\r\n\$3\r\nGET\r\n\$4\r\nmykey\r\\)n" | nc 127.0.0.1 6379
```

#### TYPE
* **Description:** Returns a simple string representing the data type of the value stored at the key (e.g., `string`, `list`, `stream`). If the key does not exist, it returns `none`.
* **RESP Representation:** `*2\r\n$4\r\nTYPE\r\n$4\r\nmykey\r\n`
* **Test Command:**
```bash
echo -e "*\(2\r\n\$4\r\nTYPE\r\n\$4\r\nmykey\r\\)n" | nc 127.0.0.1 6379
```

---

### 3. List Commands

#### LPUSH
* **Description:** Inserts an element at the head (left side) of a list stored at the specified key. The server creates the list if it doesn't exist and returns the new length of the list as an integer.
* **RESP Representation:** `*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$4\r\nitemA\r\n`
* **Test Command:**
```bash
echo -e "*\(3\r\n\$5\r\nLPUSH\r\n\$6\r\nmylist\r\n\$4\r\nitemA\r\\)n" | nc 127.0.0.1 6379
```

#### RPUSH
* **Description:** Appends an element to the tail (right side) of a list stored at the specified key. Like `LPUSH`, it initializes the list if missing and returns the updated list length.
* **RESP Representation:** `*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$4\r\nitemB\r\n`
* **Test Command:**
```bash
echo -e "*\(3\r\n\$5\r\nRPUSH\r\n\$6\r\nmylist\r\n\$4\r\nitemB\r\\)n" | nc 127.0.0.1 6379
```

#### LRANGE
* **Description:** Returns a specified range of elements from a list using zero-based offsets. Passing `0` as the start index and `-1` as the end index instructs the server to return all elements inside the list.
* **RESP Representation:** `*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n`
* **Test Command:**
```bash
echo -e "*\(4\r\n\$6\r\nLRANGE\r\n\$6\r\nmylist\r\n\$1\r\n0\r\n\$2\r\n-1\r\\)n" | nc 127.0.0.1 6379
```

#### LLEN
* **Description:** Returns the total number of elements currently contained inside a list. If the specified key does not exist, the server treats it as an empty list and returns an integer value of `0`.
* **RESP Representation:** `*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n`
* **Test Command:**
```bash
echo -e "*\(2\r\n\$4\r\nLLEN\r\n\$6\r\nmylist\r\\)n" | nc 127.0.0.1 6379
```

#### LPOP
* **Description:** Removes and returns the first element (head) of the specified list. If the list is empty or the key is missing entirely, the server responds with a null bulk string `$-1\r\n`.
* **RESP Representation:** `*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n`
* **Test Command:**
```bash
echo -e "*\(2\r\n\$4\r\nLPOP\r\n\$6\r\nmylist\r\\)n" | nc 127.0.0.1 6379
```

#### BLPOP (Blocking List Pop)
* **Description:** A blocking variant of `LPOP` that pauses execution until an element is available or a timeout is reached. Providing a timeout parameter of `0` tells the server to block the connection indefinitely until data arrives.
* **RESP Representation:** `*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n0\r\n`
* **Test Command:**
```bash
echo -e "*\(3\r\n\$5\r\nBLPOP\r\n\$6\r\nmylist\r\n\$1\r\n0\r\\)n" | nc 127.0.0.1 6379
```

---

### 4. Stream Commands

#### XADD
* **Description:** Appends a new entry consisting of field-value pairs to a stream. Passing `*` as the ID parameter signals the server to automatically generate a unique, time-sorted ID based on the current system clock.
* **RESP Representation:** `*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`
* **Test Command:**
```bash
echo -e "*\(5\r\n\$4\r\nXADD\r\n\$8\r\nmystream\r\n\$1\r\n*\r\n\$3\r\nfoo\r\n\$3\r\nbar\r\\)n" | nc 127.0.0.1 6379
```
