//! What a Storage node answers each request on the wire: the one place an
//! operation on the wire becomes a call of the operations.

use std::time::Duration;

use super::super::XmipStorage;
use super::super::watch::Position;
use super::super::wire::{Answer, Request};
use super::waiters::{Waiters, Waiting};
use crate::PersistError;

/// What `storage` answers `request`: the one place an operation on the
/// wire becomes a call of the operations. A wait on queues is held among
/// `waiters` by its token, which an end asked over another connection ends
/// (`super::waiters`).
pub(crate) fn answer(storage: &dyn XmipStorage, request: Request, waiters: &Waiters) -> Answer {
    let answered = match request {
        Request::Wait(token, holder, seen, bound) => waited(
            storage,
            (&seen, Duration::from_nanos(bound)),
            &waiters.waiting(token, &holder),
        ),
        Request::EndWait(token) => {
            waiters.end(token);
            Ok(Answer::Done)
        }
        Request::WriteChunk(chunk) => storage.write_chunk(&chunk).map(|()| Answer::Done),
        Request::ReadChunk(stream, index) => storage.read_chunk(stream, index).map(Answer::Chunk),
        Request::WriteMessage(message) => storage.write_message(&message).map(|()| Answer::Done),
        Request::ReadMessage(id) => storage
            .read_message(id)
            .map(|record| Answer::Message(record.map(Box::new))),
        Request::WriteJourney(journey) => storage.write_journey(&journey).map(|()| Answer::Done),
        Request::ReadJourney(id) => storage
            .read_journey(id)
            .map(|record| Answer::Journey(record.map(Box::new))),
        Request::Claim(claim, lease) => storage
            .claim(
                claim.journey,
                &claim.holder,
                claim.token,
                Duration::from_nanos(lease),
            )
            .map(Answer::Claim),
        Request::Renew(claims, lease) => storage
            .renew(&claims, Duration::from_nanos(lease))
            .map(Answer::Claims),
        Request::Release(claim) => storage.release(&claim).map(Answer::Yes),
        Request::HandOn(hand_on) => storage.hand_on(&hand_on).map(Answer::Yes),
        Request::Turn(turn) => storage.turn(&turn).map(Answer::Yes),
        Request::ReadInstance(id) => storage
            .read_instance(id)
            .map(|record| Answer::Instance(record.map(Box::new))),
        Request::ReadInstanceKey(id) => storage
            .read_instance_key(id)
            .map(|found| Answer::Records(found.into_iter().collect())),
        Request::WriteAudit(entry) => storage.write_audit(&entry).map(|()| Answer::Done),
        Request::KeepAudit(most, chunk) => storage
            .keep_audit(most, usize::try_from(chunk).unwrap_or(usize::MAX))
            .map(Answer::Count),
        Request::ReadKeptAudit(id) => storage
            .read_kept_audit(id)
            .map(|entry| Answer::Audit(entry.map(Box::new))),
        Request::WriteAdministration(record) => {
            storage.write_administration(&record).map(|()| Answer::Done)
        }
        Request::ReadAdministration(kind, id) => storage
            .read_administration(kind, id)
            .map(Answer::Administration),
        Request::RemoveAdministration(kind, id) => storage
            .remove_administration(kind, id)
            .map(|()| Answer::Done),
        Request::WriteSecret(secret) => storage.write_secret(&secret).map(|()| Answer::Done),
        Request::ReadSecret(name) => storage
            .read_secret(&name)
            .map(|secret| Answer::Secret(secret.map(Box::new))),
        Request::ListSecrets => storage.list_secrets().map(Answer::Secrets),
        // Answered once durable here: the client's caller acknowledges on it.
        Request::Publish(publication) => storage
            .publish(*publication)
            .and_then(|(held, durable)| durable.wait().map(|()| Answer::Claims(held))),
        Request::ReadHeld(queue, from, most) => {
            storage.read_held(queue, from, most).map(Answer::Held)
        }
        Request::ReadDue(queue, part, from, most) => storage
            .read_due(queue, part, from, most)
            .map(Answer::DueQueue),
        Request::ReadFailed(queue, from, most) => storage
            .read_failed(queue, from, most)
            .map(Answer::FailedQueue),
        Request::ReadDead(queue, from, most) => {
            storage.read_dead(queue, from, most).map(Answer::DeadQueue)
        }
        Request::ReadDeadMessage(queue, message) => {
            storage.read_dead_message(queue, message).map(Answer::Dead)
        }
        Request::Replay(replay) => storage.replay(&replay).map(Answer::Replayed),
        Request::Query(query) => storage.query(&query).map(Answer::Records),
        Request::WriteStream(last, stream) => {
            storage.write_stream(&last, &stream).map(|()| Answer::Done)
        }
        Request::ReadStream(id) => storage.read_stream(id).map(Answer::Stream),
        Request::ReadKeptAuditStream(id, stream) => storage
            .read_kept_audit_stream(id, stream)
            .map(Answer::Stream),
        Request::ReadKeptAuditChunk(id, stream, index) => storage
            .read_kept_audit_chunk(id, stream, index)
            .map(Answer::Bytes),
        // The server's own to answer, never an operation's (`serve`).
        Request::Standing | Request::Follow(_) => Err(PersistError::Failed {
            reason: "a Storage node's standing is its server's to answer".to_string(),
        }),
    };
    answered.unwrap_or_else(|error| match error {
        PersistError::Refused { scope, reason } => Answer::Refused(scope, reason),
        PersistError::Standby { reason } => Answer::Standby(reason),
        other => Answer::Failed(other.to_string()),
    })
}

/// `seen` waited on in `storage` for at most `bound`, or the server's most,
/// as `waiting`: what moved, none where nothing did, or a refusal as a
/// standby's — the next Storage node asked at once — where its server's
/// stop ended it with nothing.
fn waited(
    storage: &dyn XmipStorage,
    (seen, bound): (&[Position], Duration),
    waiting: &Waiting<'_>,
) -> Result<Answer, PersistError> {
    let moved = storage.wait_queues(seen, bound.min(waiting.most), &waiting.waiter)?;
    if moved.is_empty() && waiting.stopping() {
        return Err(PersistError::Standby {
            reason: "this Storage node is stopping".to_string(),
        });
    }
    Ok(Answer::Positions(moved))
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::fixture::crowd::{QUEUE, node, published};

    const SEEN: [Position; 1] = [Position {
        queue: QUEUE,
        next: 0,
    }];

    /// A wait on [`QUEUE`], seen empty, asked with `token`, for no holder.
    fn wait(token: u128) -> Request {
        Request::wait((token, None), &SEEN, Duration::from_secs(60))
    }

    #[test]
    fn a_wait_its_stopping_server_ends_is_refused_as_a_standby_and_one_that_moved_answered() {
        let (node, _) = node();
        let waiters = Waiters::default();
        waiters.stop();
        let refused = answer(&node, wait(1), &waiters);
        assert!(
            matches!(&refused, Answer::Standby(said) if said.contains("stopping")),
            "{refused:?}"
        );
        published(&node, 1..2);
        let moved = answer(&node, wait(2), &waiters);
        assert_eq!(
            moved,
            Answer::Positions(vec![Position {
                queue: QUEUE,
                next: 1
            }])
        );
    }

    #[test]
    fn a_wait_ended_by_its_token_is_answered_with_none_and_none_is_held_past_its_most() {
        let (node, _) = node();
        let waiters = Waiters::default();
        assert_eq!(
            answer(&node, Request::EndWait(7), &waiters),
            Answer::Done,
            "an end that came first"
        );
        let ended = answer(&node, wait(7), &waiters);
        assert_eq!(ended, Answer::Positions(Vec::new()));
        waiters.hold(Duration::from_millis(20));
        let held = Instant::now();
        let capped = answer(&node, wait(8), &waiters);
        assert_eq!(capped, Answer::Positions(Vec::new()));
        assert!(held.elapsed() < Duration::from_secs(30), "held its most");
    }
}
