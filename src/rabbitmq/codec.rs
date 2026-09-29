use amq_protocol::frame::AMQPFrame;
use std::io::{Cursor, Error, Write};

pub fn write_frame<W: Write>(
    writer: &mut W,
    _channel_id: u16,
    frame: &AMQPFrame,
) -> std::io::Result<()> {
    let mut out = [0u8; 8192];
    let cursor = Cursor::new(&mut out[..]);
    let res = cookie_factory::gen_simple(amq_protocol::frame::gen_frame(frame), cursor);
    if let Ok(c) = res {
        let pos = c.position() as usize;
        writer.write_all(&out[..pos])
    } else {
        Err(Error::other("serialize error"))
    }
}
