//! What the runtime writes a Journey's durable state through, and that
//! contract kept by the encrypted store over any engine.

use crate::{
    DeduplicationRecord, DurableExecutionCheckpoint, DurableJourneyState, DurableRecordIdentity,
    EncryptedStore, Engine, PersistError, RecoveryLease,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use xcore::{ClusterId, JourneyId};

/// The store a node's runtime state is written to and recovered from.
///
/// Every error is a [`PersistError`], so a record that fails its
/// authentication tag reaches the caller as [`PersistError::Refused`] with
/// its scope and reason, to be audited as a failure (ADR-0062, ADR-0063).
pub trait RuntimeStore {
    /// Write a Journey's state, replacing the last.
    ///
    /// # Errors
    ///
    /// When the record cannot be encoded, sealed or written.
    fn persist_journey_state(&self, state: DurableJourneyState) -> Result<(), PersistError>;

    /// A Journey's state as last written, or `None`.
    ///
    /// # Errors
    ///
    /// [`PersistError::Refused`] for a record that fails its tag; otherwise
    /// when it cannot be read or decoded.
    fn load_journey_state(
        &self,
        cluster_id: ClusterId,
        journey_id: JourneyId,
    ) -> Result<Option<DurableJourneyState>, PersistError>;

    /// Write an execution checkpoint, replacing the last for its identity.
    ///
    /// # Errors
    ///
    /// When the record cannot be encoded, sealed or written.
    fn persist_checkpoint(
        &self,
        checkpoint: DurableExecutionCheckpoint,
    ) -> Result<(), PersistError>;

    /// The checkpoint last written for `identity`, or `None`.
    ///
    /// # Errors
    ///
    /// As [`RuntimeStore::load_journey_state`].
    fn load_checkpoint(
        &self,
        identity: &DurableRecordIdentity,
    ) -> Result<Option<DurableExecutionCheckpoint>, PersistError>;

    /// Take the recovery lease of a Journey: `true` when taken, `false`
    /// when another holds it.
    ///
    /// # Errors
    ///
    /// When the lease cannot be encoded, sealed or written.
    fn acquire_recovery_lease(&self, lease: RecoveryLease) -> Result<bool, PersistError>;

    /// Give a recovery lease back. Only the holder's token releases it.
    ///
    /// # Errors
    ///
    /// As [`RuntimeStore::load_journey_state`].
    fn release_recovery_lease(&self, lease: &RecoveryLease) -> Result<(), PersistError>;

    /// Remember that a source was seen for a Message of a Journey.
    ///
    /// # Errors
    ///
    /// When the record cannot be encoded, sealed or written.
    fn remember_deduplication(&self, record: DeduplicationRecord) -> Result<(), PersistError>;
}

/// The kinds of record a runtime store keeps, each its own place.
const JOURNEY: &str = "journey";
const CHECKPOINT: &str = "checkpoint";
const LEASE: &str = "lease";
const DEDUPLICATION: &str = "deduplication";

/// A record or its key as bytes. JSON, the shape the records were proved in.
fn encode(value: &impl Serialize) -> Result<Vec<u8>, PersistError> {
    serde_json::to_vec(value).map_err(|error| PersistError::Record {
        reason: error.to_string(),
    })
}

fn decode<T: DeserializeOwned>(bytes: Option<Vec<u8>>) -> Result<Option<T>, PersistError> {
    bytes
        .map(|bytes| serde_json::from_slice(&bytes))
        .transpose()
        .map_err(|error| PersistError::Record {
            reason: error.to_string(),
        })
}

impl<E: Engine> RuntimeStore for EncryptedStore<E> {
    fn persist_journey_state(&self, state: DurableJourneyState) -> Result<(), PersistError> {
        let key = encode(&(state.cluster_id, state.journey.journey_id))?;
        self.put(JOURNEY, &key, &encode(&state)?)
    }

    fn load_journey_state(
        &self,
        cluster_id: ClusterId,
        journey_id: JourneyId,
    ) -> Result<Option<DurableJourneyState>, PersistError> {
        decode(self.get(JOURNEY, &encode(&(cluster_id, journey_id))?)?)
    }

    fn persist_checkpoint(
        &self,
        checkpoint: DurableExecutionCheckpoint,
    ) -> Result<(), PersistError> {
        let key = encode(&checkpoint.identity)?;
        self.put(CHECKPOINT, &key, &encode(&checkpoint)?)
    }

    fn load_checkpoint(
        &self,
        identity: &DurableRecordIdentity,
    ) -> Result<Option<DurableExecutionCheckpoint>, PersistError> {
        decode(self.get(CHECKPOINT, &encode(identity)?)?)
    }

    fn acquire_recovery_lease(&self, lease: RecoveryLease) -> Result<bool, PersistError> {
        let key = encode(&(lease.cluster_id, lease.journey_id))?;
        self.put_new(LEASE, &key, &encode(&lease)?)
    }

    fn release_recovery_lease(&self, lease: &RecoveryLease) -> Result<(), PersistError> {
        let key = encode(&(lease.cluster_id, lease.journey_id))?;
        let held: Option<RecoveryLease> = decode(self.get(LEASE, &key)?)?;
        match held {
            Some(held) if held.lease_token == lease.lease_token => self.remove(LEASE, &key),
            _ => Ok(()),
        }
    }

    fn remember_deduplication(&self, record: DeduplicationRecord) -> Result<(), PersistError> {
        let key = encode(&(record.journey_id, record.message_id))?;
        self.put(DEDUPLICATION, &key, &encode(&record)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::Memory;
    use journey::{ChainCause, ChainLimit, Journey, JourneyEntry, JourneyState};
    use secret::{Held, KekName};
    use xcore::{ExecutionId, MessageId, NodeId};

    const CLUSTER: ClusterId = ClusterId::new(1);

    fn store() -> EncryptedStore<Memory> {
        let keys = Held::new(secret::fixture::Memory::default());
        let kek = KekName::new("runtime").expect("name");
        EncryptedStore::open(Memory::default(), &keys, &kek).expect("open")
    }

    fn lease(token: &str) -> RecoveryLease {
        RecoveryLease {
            cluster_id: CLUSTER,
            journey_id: JourneyId::new(7),
            owner_node_id: NodeId::new(2),
            lease_token: token.to_string(),
            expires_utc: "2026-09-10T00:00:00Z".to_string(),
        }
    }

    fn state(journey: Journey) -> DurableJourneyState {
        DurableJourneyState {
            cluster_id: CLUSTER,
            journey,
            last_known_step: None,
            active_message_ids: vec![MessageId::new(8)],
            audit_position: 3,
        }
    }

    #[test]
    fn a_journey_state_comes_back_as_it_was_stored() {
        let store = store();
        let mut journey = Journey::new(JourneyId::new(7));
        journey.state = JourneyState::Waiting;
        journey.current_xmip_process = Some("orders".to_string());
        let state = state(journey);
        store.persist_journey_state(state.clone()).expect("stored");
        let loaded = store.load_journey_state(CLUSTER, JourneyId::new(7));
        assert_eq!(loaded.expect("loaded"), Some(state));
        let absent = store.load_journey_state(CLUSTER, JourneyId::new(9));
        assert_eq!(absent.expect("loaded"), None);
    }

    #[test]
    fn a_recovery_lease_is_held_by_one_owner_until_released() {
        let store = store();
        assert!(store.acquire_recovery_lease(lease("t")).expect("first"));
        assert!(!store.acquire_recovery_lease(lease("u")).expect("second"));
        // Another's token does not release it.
        store.release_recovery_lease(&lease("u")).expect("ignored");
        assert!(
            !store
                .acquire_recovery_lease(lease("u"))
                .expect("still held")
        );
        store.release_recovery_lease(&lease("t")).expect("released");
        assert!(store.acquire_recovery_lease(lease("u")).expect("again"));
    }

    #[test]
    fn a_dismissed_journey_is_a_record_like_any_other() {
        // The store writes every state the Journey defines; the copy this
        // crate carried stopped at Failed and could not record a decision.
        let store = store();
        let mut journey = Journey::new(JourneyId::new(11));
        journey.state = JourneyState::Dismissed;
        let state = state(journey);
        store.persist_journey_state(state.clone()).expect("stored");
        let loaded = store
            .load_journey_state(CLUSTER, JourneyId::new(11))
            .expect("loaded")
            .expect("present");
        assert!(loaded.journey.state.is_terminal());
        assert_eq!(loaded, state);
    }

    #[test]
    fn a_recovered_journey_keeps_its_chain_and_the_limit_still_holds() {
        // Row a of problem 25: the copy this crate kept had no depth, so a
        // Journey two links deep came back at zero and the limit restarted.
        let limit = ChainLimit::new(2);
        let cause = || ChainCause::subscription("orders");
        let first = Journey::new(JourneyId::new(20));
        let second =
            Journey::following(JourneyId::new(21), &first, cause(), limit).expect("one link");
        let third = Journey::following(JourneyId::new(22), &second, cause(), limit)
            .expect("two links")
            .append(
                JourneyEntry {
                    execution_id: ExecutionId::new(1),
                    message_id: MessageId::new(2),
                    action: "deliver".to_string(),
                    outcome: "sent".to_string(),
                    timestamp_unix_nanos: 1_789_000_000_000_000_000,
                },
                JourneyState::Waiting,
            );
        let store = store();
        store.persist_journey_state(state(third)).expect("stored");
        let read = store
            .load_journey_state(CLUSTER, JourneyId::new(22))
            .expect("loaded")
            .expect("present");

        assert_eq!(read.journey.depth, 2);
        assert_eq!(read.journey.previous_journey_id, Some(JourneyId::new(21)));
        let refused = Journey::following(JourneyId::new(23), &read.journey, cause(), limit);
        assert!(
            refused.is_err(),
            "the limit counts the links made before the restart"
        );
    }

    #[test]
    fn a_checkpoint_is_found_by_its_identity() {
        let store = store();
        let identity = DurableRecordIdentity {
            cluster_id: CLUSTER,
            node_id: NodeId::new(2),
            journey_id: JourneyId::new(7),
            message_id: None,
        };
        let checkpoint = DurableExecutionCheckpoint {
            identity: identity.clone(),
            xmip_process_name: Some("orders".to_string()),
            current_step: "deliver".to_string(),
            generation: 1,
            payload_refs: Vec::new(),
            waiting_for: Vec::new(),
        };
        store
            .persist_checkpoint(checkpoint.clone())
            .expect("stored");
        assert_eq!(
            store.load_checkpoint(&identity).expect("loaded"),
            Some(checkpoint)
        );
    }
}
