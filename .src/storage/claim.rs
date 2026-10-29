//! A claim as the embedded Storage node's runtime database keeps it, and
//! the writes that take, renew and end one (`runtime-model.md` section 3,
//! *Work moves by claim*). Its writer decides each condition
//! ([`super::commit`]); what a claim is written as is here.

use super::commit::{Batch, CLAIM, JOURNEY, MESSAGE};
use super::hand_on::HandOn;
use super::record::{Claim, Form, malformed, read_byte};
use super::{failed, hold};
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
        self.standing == Standing::Held && self.claim.is(claim)
    }

    /// Whether it is held by the request `asked`, asked again: its token,
    /// the fence the request never knew.
    pub(crate) fn asked_by(&self, asked: &Claim) -> bool {
        self.standing == Standing::Held && self.claim.token == asked.token
    }
}

impl Claim {
    /// Whether `other` is this claim: the same holder's token and fence.
    /// A write under a claim the Journey has passed from is refused by
    /// both, the fence being the number that grows (the owner, 2026-10-27).
    pub(crate) fn is(&self, other: &Claim) -> bool {
        self.token == other.token && self.fence == other.fence
    }
}

/// The fence a claim taken in `epoch` after `before` — the claim the
/// Journey had, where it had one — carries: the epoch in the high 32 bits,
/// the Journey's holder count, one more, in the low 32.
#[must_use]
/// A lease as the nanoseconds a claim is held for, at most what an `i128`
/// holds.
pub(crate) fn lease_nanos(lease: std::time::Duration) -> i128 {
    i128::try_from(lease.as_nanos()).unwrap_or(i128::MAX)
}

pub(crate) fn fence(epoch: u64, before: Option<&Claim>) -> u64 {
    let holders = before.map_or(0, |claim| claim.fence & u64::from(u32::MAX));
    (epoch << 32) | (holders + 1).min(u64::from(u32::MAX))
}

/// `claim` taken in `batch` until `until`, where its Journey has no claim
/// held at `now` — none, ended, or lapsed — or one held under its own token,
/// a request asked again: the claim held, its fence the next for `epoch`,
/// or `None` where another holds it (`runtime-model.md` section 3: *set the
/// owner where the owner is empty or lapsed*).
pub(crate) fn take<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    mut claim: Claim,
    (now, until, epoch): (i128, i128, u64),
) -> Result<Option<Claim>, PersistError> {
    let before = stored(store, batch, &claim)?;
    if let Some(stored) = &before
        && stored.standing == Standing::Held
        && stored.claim.until_unix_nanos >= now
    {
        let ours = stored.claim.token == claim.token;
        return Ok(ours.then(|| stored.claim.clone()));
    }
    claim.fence = fence(epoch, before.as_ref().map(|stored| &stored.claim));
    Ok(Some(hold(batch, claim, until)))
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

/// `hand_on` decided in `batch` at `now`: its result, what it made and what
/// follows written, the places it leaves let go of and the ones it takes
/// kept, its Journey entered in or taken out of a queue's index of failed
/// Journeys, and its claim ended handed on — or kept, where the step waits —
/// `true`, where the claim is its holder's; `true` and nothing written
/// where it was handed on already under its token, an answer that was
/// lost; `false` otherwise.
pub(crate) fn hand_on<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    hand_on: &HandOn,
    now: i128,
) -> Result<bool, PersistError> {
    let Some(stored) = stored(store, batch, &hand_on.claim)? else {
        return Ok(false);
    };
    let ours = stored.claim.is(&hand_on.claim);
    match stored.standing {
        // Handed on already under this token: an answer that was lost.
        Standing::HandedOn => return Ok(ours),
        // Given back, its step not done: nothing to answer for.
        Standing::Released => return Ok(false),
        Standing::Held if !ours => return Ok(false),
        Standing::Held => {}
    }
    let result = &hand_on.result;
    super::origin::counted(store, batch, result, &hand_on.queued)?;
    batch.record(JOURNEY, result.journey.value(), result);
    for message in &hand_on.messages {
        batch.record(MESSAGE, message.message.value(), message);
    }
    for next in &hand_on.next {
        super::origin::counted(store, batch, next, &hand_on.queued)?;
        batch.record(JOURNEY, next.journey.value(), next);
    }
    for queue in &hand_on.leaves {
        hold::let_go_of(store, batch, *queue, hand_on.claim.journey)?;
    }
    for kept in &hand_on.queued {
        hold::keep(store, batch, kept)?;
    }
    for queue in &hand_on.requeued {
        hold::to_the_end(store, batch, *queue, hand_on.claim.journey)?;
    }
    if let Some(failure) = &hand_on.failed {
        failed::enter(store, batch, hand_on.claim.journey, failure, now)?;
    }
    for queue in &hand_on.cleared {
        failed::clear(store, batch, *queue, hand_on.claim.journey)?;
    }
    if let Some(wait) = &hand_on.waits {
        let due = now.saturating_add(i128::from(wait.after_nanos));
        hold::wait(store, batch, (wait.queue, hand_on.claim.journey), due)?;
    }
    match hand_on.kept_for_nanos {
        Some(nanos) => {
            hold(batch, stored.claim, now.saturating_add(i128::from(nanos)));
        }
        None => end(batch, stored.claim, Standing::HandedOn),
    }
    Ok(true)
}
