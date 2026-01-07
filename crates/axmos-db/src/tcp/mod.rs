//! Protocol for client-server communication.
use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    io::{self, Read, Write},
};

pub mod session;

pub const PROTOCOL_VERSION: u8 = 1;
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    Create(String),
    Open(String),
    /// Execute SQL (stateless, autocommit via Database::execute)
    Sql(String),
    /// Execute SQL in session context (supports BEGIN/COMMIT/ROLLBACK)
    Begin,
    Rollback,
    Commit,
    Explain(String),
    Analyze {
        sample_rate: f64,
        max_sample_rows: usize,
    },
    Vacuum,
    Close,
    Ping,
    Shutdown,
}

impl Request {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(PROTOCOL_VERSION);

        match self {
            Self::Create(path) => {
                buf.push(0x01);
                write_string(&mut buf, path);
            }
            Self::Open(path) => {
                buf.push(0x02);
                write_string(&mut buf, path);
            }
            Self::Sql(sql) => {
                buf.push(0x03);
                write_string(&mut buf, sql);
            }
            Self::Explain(sql) => {
                buf.push(0x04);
                write_string(&mut buf, sql);
            }
            Self::Analyze {
                sample_rate,
                max_sample_rows,
            } => {
                buf.push(0x05);
                buf.extend_from_slice(&sample_rate.to_le_bytes());
                buf.extend_from_slice(&(*max_sample_rows as u64).to_le_bytes());
            }
            Self::Close => {
                buf.push(0x06);
            }
            Self::Ping => {
                buf.push(0x07);
            }
            Self::Vacuum => {
                buf.push(0x09);
            }
            Self::Begin => {
                buf.push(0x0A);
            }
            Self::Rollback => {
                buf.push(0x0C);
            }
            Self::Commit => {
                buf.push(0x0B);
            }
            Self::Shutdown => {
                buf.push(0xFF);
            }
        }

        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, TcpError> {
        if data.is_empty() {
            return Err(TcpError::InvalidMessage("Empty message".into()));
        }

        let version = data[0];
        if version != PROTOCOL_VERSION {
            return Err(TcpError::VersionMismatch {
                expected: PROTOCOL_VERSION,
                got: version,
            });
        }

        if data.len() < 2 {
            return Err(TcpError::InvalidMessage("Message too short".into()));
        }

        let cmd = data[1];
        let payload = &data[2..];

        match cmd {
            0x01 => {
                let path = read_string(payload)?;
                Ok(Self::Create(path))
            }
            0x02 => {
                let path = read_string(payload)?;
                Ok(Self::Open(path))
            }
            0x03 => {
                let sql = read_string(payload)?;
                Ok(Self::Sql(sql))
            }
            0x04 => {
                let sql = read_string(payload)?;
                Ok(Self::Explain(sql))
            }
            0x05 => {
                if payload.len() < 16 {
                    return Err(TcpError::InvalidMessage("Analyze payload too short".into()));
                }
                let sample_rate = f64::from_le_bytes(payload[0..8].try_into().unwrap());
                let max_sample_rows =
                    u64::from_le_bytes(payload[8..16].try_into().unwrap()) as usize;
                Ok(Self::Analyze {
                    sample_rate,
                    max_sample_rows,
                })
            }
            0x06 => Ok(Self::Close),
            0x07 => Ok(Self::Ping),
            0x09 => Ok(Self::Vacuum),
            0x0A => Ok(Self::Begin),
            0x0B => Ok(Self::Commit),
            0x0C => Ok(Self::Rollback),
            0xFF => Ok(Self::Shutdown),
            _ => Err(TcpError::UnknownCommand(cmd)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StatusCode {
    Ok = 0x00,
    Error = 0x01,
    Rows = 0x02,
    RowsAffected = 0x03,
    Ddl = 0x04,
    Explain = 0x05,
    Pong = 0x06,
    Goodbye = 0x07,
    ShuttingDown = 0x08,
    VacuumComplete = 0x09,
    Begin = 0x0A,
    End = 0x0B,
}

impl TryFrom<u8> for StatusCode {
    type Error = TcpError;

    fn try_from(value: u8) -> Result<StatusCode, TcpError> {
        match value {
            0x00 => Ok(Self::Ok),
            0x01 => Ok(Self::Error),
            0x02 => Ok(Self::Rows),
            0x03 => Ok(Self::RowsAffected),
            0x04 => Ok(Self::Ddl),
            0x05 => Ok(Self::Explain),
            0x06 => Ok(Self::Pong),
            0x07 => Ok(Self::Goodbye),
            0x08 => Ok(Self::ShuttingDown),
            0x09 => Ok(Self::VacuumComplete),
            0x0A => Ok(Self::Begin),
            0x0B => Ok(Self::End),
            _ => Err(TcpError::UnknownStatus(value)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Response {
    Ok(String),
    Error(String),
    Rows {
        columns: Vec<String>,
        data: Vec<Vec<String>>,
    },
    SessionStarted,
    SessionEnd,
    RowsAffected(u64),
    Ddl(String),
    Explain(String),
    VacuumComplete {
        tables_vacuumed: usize,
        bytes_freed: usize,
        transactions_cleaned: usize,
    },
    Pong,
    Goodbye,
    ShuttingDown,
}

impl Response {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(PROTOCOL_VERSION);

        match self {
            Self::Ok(msg) => {
                buf.push(StatusCode::Ok as u8);
                write_string(&mut buf, msg);
            }
            Self::Error(msg) => {
                buf.push(StatusCode::Error as u8);
                write_string(&mut buf, msg);
            }
            Self::Rows { columns, data } => {
                buf.push(StatusCode::Rows as u8);
                buf.extend_from_slice(&(columns.len() as u32).to_le_bytes());
                for col in columns {
                    write_string(&mut buf, col);
                }
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                for row in data {
                    for value in row {
                        write_string(&mut buf, value);
                    }
                }
            }
            Self::RowsAffected(count) => {
                buf.push(StatusCode::RowsAffected as u8);
                buf.extend_from_slice(&count.to_le_bytes());
            }
            Self::Ddl(msg) => {
                buf.push(StatusCode::Ddl as u8);
                write_string(&mut buf, msg);
            }
            Self::Explain(plan) => {
                buf.push(StatusCode::Explain as u8);
                write_string(&mut buf, plan);
            }
            Self::VacuumComplete {
                tables_vacuumed,
                bytes_freed,
                transactions_cleaned,
            } => {
                buf.push(StatusCode::VacuumComplete as u8);
                buf.extend_from_slice(&(*tables_vacuumed as u64).to_le_bytes());
                buf.extend_from_slice(&(*bytes_freed as u64).to_le_bytes());
                buf.extend_from_slice(&(*transactions_cleaned as u64).to_le_bytes());
            }
            Self::Pong => {
                buf.push(StatusCode::Pong as u8);
            }
            Self::Goodbye => {
                buf.push(StatusCode::Goodbye as u8);
            }
            Self::ShuttingDown => {
                buf.push(StatusCode::ShuttingDown as u8);
            }
            Self::SessionStarted => {
                buf.push(StatusCode::Begin as u8);
            }
            Self::SessionEnd => {
                buf.push(StatusCode::End as u8);
            }
        }

        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, TcpError> {
        if data.len() < 2 {
            return Err(TcpError::InvalidMessage("Response too short".into()));
        }

        let version = data[0];
        if version != PROTOCOL_VERSION {
            return Err(TcpError::VersionMismatch {
                expected: PROTOCOL_VERSION,
                got: version,
            });
        }

        let status = StatusCode::try_from(data[1])?;
        let payload = &data[2..];

        match status {
            StatusCode::Ok => {
                let msg = read_string(payload)?;
                Ok(Self::Ok(msg))
            }
            StatusCode::Error => {
                let msg = read_string(payload)?;
                Ok(Self::Error(msg))
            }
            StatusCode::Rows => {
                let mut offset = 0;

                if payload.len() < 4 {
                    return Err(TcpError::InvalidMessage("Missing column count".into()));
                }
                let col_count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                offset += 4;

                let mut columns = Vec::with_capacity(col_count);

                for _ in 0..col_count {
                    let (col, len) = read_string_with_len(&payload[offset..])?;
                    columns.push(col);
                    offset += len;
                }

                if payload.len() < offset + 4 {
                    return Err(TcpError::InvalidMessage("Missing row count".into()));
                }
                let row_count =
                    u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                let mut data = Vec::with_capacity(row_count);
                for _ in 0..row_count {
                    let mut row = Vec::with_capacity(col_count);
                    for _ in 0..col_count {
                        let (value, len) = read_string_with_len(&payload[offset..])?;
                        row.push(value);
                        offset += len;
                    }
                    data.push(row);
                }

                Ok(Self::Rows { columns, data })
            }
            StatusCode::RowsAffected => {
                if payload.len() < 8 {
                    return Err(TcpError::InvalidMessage("Missing count".into()));
                }
                let count = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                Ok(Self::RowsAffected(count))
            }
            StatusCode::Ddl => {
                let msg = read_string(payload)?;
                Ok(Self::Ddl(msg))
            }
            StatusCode::Explain => {
                let plan = read_string(payload)?;
                Ok(Self::Explain(plan))
            }
            StatusCode::VacuumComplete => {
                if payload.len() < 24 {
                    return Err(TcpError::InvalidMessage("Vacuum payload too short".into()));
                }
                let tables_vacuumed =
                    u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
                let bytes_freed = u64::from_le_bytes(payload[8..16].try_into().unwrap()) as usize;
                let transactions_cleaned =
                    u64::from_le_bytes(payload[16..24].try_into().unwrap()) as usize;
                Ok(Self::VacuumComplete {
                    tables_vacuumed,
                    bytes_freed,
                    transactions_cleaned,
                })
            }
            StatusCode::Pong => Ok(Self::Pong),
            StatusCode::Goodbye => Ok(Self::Goodbye),
            StatusCode::ShuttingDown => Ok(Self::ShuttingDown),
            StatusCode::Begin => Ok(Self::SessionStarted),
            StatusCode::End => Ok(Self::SessionEnd),
        }
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }
}

#[derive(Debug)]
pub enum TcpError {
    VersionMismatch { expected: u8, got: u8 },
    UnknownCommand(u8),
    UnknownStatus(u8),
    InvalidMessage(String),
    Io(io::Error),
    MessageTooLarge(usize),
}

impl Display for TcpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::VersionMismatch { expected, got } => {
                write!(
                    f,
                    "Protocol version mismatch: expected {}, got {}",
                    expected, got
                )
            }
            Self::UnknownCommand(cmd) => write!(f, "Unknown command: 0x{:02X}", cmd),
            Self::UnknownStatus(status) => write!(f, "Unknown status: 0x{:02X}", status),
            Self::InvalidMessage(msg) => write!(f, "Invalid message: {}", msg),
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::MessageTooLarge(size) => {
                write!(
                    f,
                    "Message too large: {} bytes (max {})",
                    size, MAX_MESSAGE_SIZE
                )
            }
        }
    }
}

impl Error for TcpError {}

impl From<io::Error> for TcpError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

fn write_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn read_string(data: &[u8]) -> Result<String, TcpError> {
    let (s, _) = read_string_with_len(data)?;
    Ok(s)
}

fn read_string_with_len(data: &[u8]) -> Result<(String, usize), TcpError> {
    if data.len() < 4 {
        return Err(TcpError::InvalidMessage("String length missing".into()));
    }

    let len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;

    if data.len() < 4 + len {
        return Err(TcpError::InvalidMessage(format!(
            "String data truncated: expected {} bytes, got {}",
            len,
            data.len() - 4
        )));
    }

    let s = String::from_utf8_lossy(&data[4..4 + len]).into_owned();
    Ok((s, 4 + len))
}

pub fn read_message<R: Read>(reader: &mut R) -> Result<Vec<u8>, TcpError> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_MESSAGE_SIZE {
        return Err(TcpError::MessageTooLarge(len));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn write_message<W: Write>(writer: &mut W, data: &[u8]) -> Result<(), TcpError> {
    if data.len() > MAX_MESSAGE_SIZE {
        return Err(TcpError::MessageTooLarge(data.len()));
    }
    let len_buf = (data.len() as u32).to_le_bytes();
    writer.write_all(&len_buf)?;
    writer.write_all(data)?;
    writer.flush()?;
    Ok(())
}

pub fn send_request<W: Write>(writer: &mut W, request: &Request) -> Result<(), TcpError> {
    let data = request.to_bytes();
    write_message(writer, &data)
}

pub fn recv_request<R: Read>(reader: &mut R) -> Result<Request, TcpError> {
    let data = read_message(reader)?;
    Request::from_bytes(&data)
}

pub fn send_response<W: Write>(writer: &mut W, response: &Response) -> Result<(), TcpError> {
    let data = response.to_bytes();
    write_message(writer, &data)
}

pub fn recv_response<R: Read>(reader: &mut R) -> Result<Response, TcpError> {
    let data = read_message(reader)?;
    Response::from_bytes(&data)
}
