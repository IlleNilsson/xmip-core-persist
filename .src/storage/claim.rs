//! A claim as the embedded Storage node's runtime database keeps it, and
//! the writes that take, renew and end one (`runtime-model.md` section 3,
//! *Work moves by claim*). Its writer decides each condition
//! ([`super::commit`]); what a claim is written as is here.

use super::commit::{Batch, CLAIM, JOURNEY, MESSAGE};
use super::record::{Claim, Form, HandOn, malformed, read_byte};
use crate::{EncryptedStore, Engine, PersistError};

/// Where a claim stands: held, or ended one of two ways, each kept as it
/// ended so a request asked again is answered for what was done.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Standing {
    Held,
    /// Given back, its step not done: nothing of it was written.
    Released,
    /// Its step handed on: the result and what follows are written.
    HandedOn,
}

/// A claim as stored: the last claim on its Journey, and where it stands.
/// An ended claim keeps its token, so a hand-on asked again after a lost
/// answer is known for what it is — and a hand-on after a mere release is
/// not taken for one.
pub(crate) struct Stored {
    pub(crate) claim: Claim,
    pub(crate) standing: Standing,
}

impl Stored {
    pub(crate) fn bytes(&self) -> Vec<u8> {
        let standing = match self.standing {
            Standing::Held => 0,
            Standing::Released => 1,
            Standing::HandedOn => 2,
        };
        let mut out = vec![standing];
        self.claim.write(&mut out);
        out
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, PersistError> {
        let mut cursor = codec::cursor::Cursor::new(bytes);
        let standing = match read_byte(&mut cursor)? {
            0 => Standing::Held,
            1 => Standing::Released,
            2 => Standing::HandedOn,
            other => return Err(malformed(format!("a claim standing {other}"))),
        };
        let claim = Claim::read(&mut cursor)?;
        Ok(Self { claim, standing })
    }

    pub(crate) fn held_by(&self, claim: &Claim) -> bool {
        self.standing == Standing::Held && self.claim.token == claim.token
    }
}

/// `claim` held until `until`, written in `batch`.
pub(crate) fn hold(batch: &mut Batch<'_>, mut claim: Claim, until: i128) -> Claim {
    claim.until_unix_nanos = until;
    let stored = Stored {
        claim: claim.clone(),
        standing: Standing::Held,
    };
    batch.put(CLAIM, key(&claim), Some(stored.bytes()));
    claim
}

/// `claim` ended as `standing` says, its token kept, written in `batch`.
pub(crate) fn end(batch: &mut Batch<'_>, claim: Claim, standing: Standing) {
    let key = key(&claim);
    let stored = Stored { claim, standing };
    batch.put(CLAIM, key, Some(stored.bytes()));
}

/// Where a Journey's claim is kept: under the Journey's identifier.
pub(crate) fn key(claim: &Claim) -> Vec<u8> {
    claim.journey.value().to_be_bytes().to_vec()
}

/// The claim on `claim`'s Journey in `store`, as `batch` has left it.
pub(crate) fn stored<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &Batch<'_>,
    claim: &Claim,
) -> Result<Option<Stored>, PersistError> {
    batch
        .read(store, CLAIM, &key(claim))?
        .map(|bytes| Stored::from_bytes(&bytes))
        .transpose()
}

/// `hand_on` decided in `batch`: its result, what it made and what follows
/// written and its claim ended handed on — `true` — where the claim is its
/// holder's; `true` and nothing written where it was handed on already
/// under its token, an answer that was lost; `false` otherwise.
pub(crate) fn hand_on<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    hand_on: &HandOn,
) -> Result<bool, PersistError> {
    let Some(stored) = stored(store, batch, &hand_on.claim)? else {
        return Ok(false);
    };
    let ours = stored.claim.token == hand_on.claim.token;
    match stored.standing {
        // Handed on already under this token: an answer that was lost.
        Standing::HandedOn => return Ok(ours),
        // Given back, its step not done: nothing to answer for.
        Standing::Released => return Ok(false),
        Standing::Held if !ours => return Ok(false),
        Standing::Held => {}
    }
    let result = &hand_on.result;
    batch.record(JOURNEY, result.journey.value(), result);
    for message in &hand_on.messages {
        batch.record(MESSAGE, message.message.value(), message);
    }
    for next in &hand_on.next {
        batch.record(JOURNEY, next.journey.value(), next);
    }
    end(batch, stored.claim, Standing::HandedOn);
    Ok(true)
}
