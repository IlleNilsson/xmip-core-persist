//! Where a standby keeps, in each database, how far it has copied: its
//! position record, written in the same batch as every write it copies
//! (`super::copies`), and given up once it is promoted.

use crate::{Change, Engine, PersistError};

/// The position record's engine key: one of a store's own, never copied.
pub(crate) const POSITION: &[u8] = b"\0xmip-core-persist/position\0";

/// The position record: `history`, and `next`, the number of the next
/// write a standby takes.
pub(crate) fn position(history: u128, next: u64) -> Change {
    let mut value = history.to_be_bytes().to_vec();
    value.extend_from_slice(&next.to_be_bytes());
    (POSITION.to_vec(), Some(value))
}

/// The history and the next number `engine`'s position record holds, or
/// `None` where it holds none: it never copied, or it was a primary since.
///
/// # Errors
///
/// Where the engine cannot be read.
pub(crate) fn positioned(engine: &dyn Engine) -> Result<Option<(u128, u64)>, PersistError> {
    let Some(value) = engine.read(POSITION)? else {
        return Ok(None);
    };
    let (history, next) = value.split_at_checked(16).unwrap_or((&[], &[]));
    let (Ok(history), Ok(next)) = (<[u8; 16]>::try_from(history), <[u8; 8]>::try_from(next)) else {
        return Ok(None);
    };
    Ok(Some((
        u128::from_be_bytes(history),
        u64::from_be_bytes(next),
    )))
}
