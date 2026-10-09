//! A frame of Xmip Storage's wire: its length, four bytes big-endian, and
//! that many bytes, the record in its one binary form ([`Form`]).

use std::io::{Read, Write};

use super::super::record::Form;

/// The most one frame holds: the ceiling every connection in the estate is
/// read for (`net::MAX_BODY`).
const MOST: usize = net::MAX_BODY;

/// Write `record` as one frame.
///
/// # Errors
///
/// Where the connection fails, or the record is larger than a frame holds.
pub(crate) fn send(connection: &mut impl Write, record: &impl Form) -> std::io::Result<()> {
    let bytes = record.bytes();
    let length = u32::try_from(bytes.len())
        .ok()
        .filter(|length| *length as usize <= MOST)
        .ok_or_else(|| std::io::Error::other("a record larger than a frame holds"))?;
    let mut frame = Vec::with_capacity(4 + bytes.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&bytes);
    connection.write_all(&frame)?;
    connection.flush()
}

/// Read one frame, and the record in it. `None` where the connection ended
/// cleanly before a frame began.
///
/// # Errors
///
/// Where the connection fails or ends inside a frame, or the frame is not
/// a record.
pub(crate) fn receive<T: Form>(connection: &mut impl Read) -> std::io::Result<Option<T>> {
    let mut length = [0u8; 4];
    match connection.read_exact(&mut length) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let length = u32::from_be_bytes(length) as usize;
    if length > MOST {
        return Err(std::io::Error::other("a frame larger than a frame holds"));
    }
    let mut bytes = vec![0u8; length];
    connection.read_exact(&mut bytes)?;
    T::from_bytes(&bytes)
        .map(Some)
        .map_err(|error| std::io::Error::other(error.to_string()))
}
