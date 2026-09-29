use std::io::{self, Read, Write};

pub const OP_REPLY: i32 = 1;
pub const OP_QUERY: i32 = 2004;
pub const OP_MSG: i32 = 2013;

#[derive(Debug)]
pub struct MsgHeader {
    pub message_length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub op_code: i32,
}

impl MsgHeader {
    pub fn read_from<R: Read>(mut r: R) -> io::Result<Self> {
        let mut buf = [0u8; 16];
        r.read_exact(&mut buf)?;
        Ok(Self {
            message_length: i32::from_le_bytes(buf[0..4].try_into().unwrap()),
            request_id: i32::from_le_bytes(buf[4..8].try_into().unwrap()),
            response_to: i32::from_le_bytes(buf[8..12].try_into().unwrap()),
            op_code: i32::from_le_bytes(buf[12..16].try_into().unwrap()),
        })
    }

    pub fn write_to<W: Write>(&self, mut w: W) -> io::Result<()> {
        w.write_all(&self.message_length.to_le_bytes())?;
        w.write_all(&self.request_id.to_le_bytes())?;
        w.write_all(&self.response_to.to_le_bytes())?;
        w.write_all(&self.op_code.to_le_bytes())?;
        Ok(())
    }
}
