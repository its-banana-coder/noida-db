//! Codec utilities for reading/writing Kafka frame protocol payloads.

use bytes::BytesMut;
use std::io::{self, Read, Write};

pub fn read_frame<R: Read>(reader: &mut R) -> io::Result<Option<BytesMut>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 100 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Kafka frame length too large: {}", len),
        ));
    }

    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload)?;
    Ok(Some(BytesMut::from(&payload[..])))
}

pub fn write_frame<W: Write>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    let len = (data.len() as u32).to_be_bytes();
    writer.write_all(&len)?;
    writer.write_all(data)?;
    writer.flush()
}
