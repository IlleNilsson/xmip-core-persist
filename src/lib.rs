//! Durable runtime state: what a node writes down so that a Journey survives
//! the node.
//!
//! The shapes here are persistence's own — a checkpoint, a lease, a
//! deduplication fingerprint. What they are *about* is not: a record is keyed
//! by the estate's identifiers and carries the Journey state `xmip-core-journey`
//! defines. This crate held a raw UUID where `JourneyId` and `MessageId`
//! exist, a name where `ClusterId` and `NodeId` exist, and a six-variant copy
//! of `JourneyState`, until 2026-09-14 (ADR-0044: shared code lives where both
//! already depend). The newtype is what stops a Journey identifier being
//! passed where a Message identifier belongs, which a `Uuid` never could.
//!
//! The times are still text, RFC 3339 in UTC, as they were.

use journey::Journey;
use serde::{Deserialize, Serialize};
use xcore::{ClusterId, JourneyId, MessageId, NodeId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableRecordIdentity {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub journey_id: JourneyId,
    pub message_id: Option<MessageId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableExecutionCheckpoint {
    pub identity: DurableRecordIdentity,
    pub xmip_process_name: Option<String>,
    pub current_step: String,
    pub generation: u32,
    pub payload_refs: Vec<String>,
    pub waiting_for: Vec<RecoveryWaitCondition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryWaitCondition {
    pub condition_name: String,
    pub correlation_key: String,
    pub timeout_utc: Option<String>,
}

/// A Journey as last written, and where recovery picks it up.
///
/// The [`Journey`] is stored whole — its state, its chain back to the Journey
/// that caused it, its depth and its entries. Until 2026-09-23 this held a
/// cut-down copy of the Journey's fields, which dropped `previous_journey_id`,
/// `cause`, `depth` and the entries: a Journey recovered from it restarted its
/// depth at zero, and the chain limit (ADR-0026) forgot every link made before
/// the restart (open-problems.md, problem 25, row a). What is here beside it
/// is persistence's own.
///
/// Every state is written, `Dismissed` included: a Journey an operator
/// stopped is a terminal record like any other (ADR-0013).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableJourneyState {
    pub cluster_id: ClusterId,
    pub journey: Journey,
    pub last_known_step: Option<String>,
    /// The generations of [`Journey::messages`] still in flight.
    pub active_message_ids: Vec<MessageId>,
    pub audit_position: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryLease {
    pub cluster_id: ClusterId,
    pub journey_id: JourneyId,
    pub owner_node_id: NodeId,
    pub lease_token: String,
    pub expires_utc: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeduplicationRecord {
    pub journey_id: JourneyId,
    pub message_id: MessageId,
    pub source_fingerprint: String,
}

pub trait RuntimeStore {
    fn persist_journey_state(&self, state: DurableJourneyState) -> Result<(), String>;
    fn load_journey_state(
        &self,
        cluster_id: ClusterId,
        journey_id: JourneyId,
    ) -> Result<Option<DurableJourneyState>, String>;
    fn persist_checkpoint(&self, checkpoint: DurableExecutionCheckpoint) -> Result<(), String>;
    fn load_checkpoint(
        &self,
        identity: &DurableRecordIdentity,
    ) -> Result<Option<DurableExecutionCheckpoint>, String>;
    fn acquire_recovery_lease(&self, lease: RecoveryLease) -> Result<bool, String>;
    fn release_recovery_lease(&self, lease: &RecoveryLease) -> Result<(), String>;
    fn remember_deduplication(&self, record: DeduplicationRecord) -> Result<(), String>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use journey::{ChainCause, ChainLimit, JourneyEntry, JourneyState};
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use xcore::ExecutionId;

    /// A store in memory: enough of [`RuntimeStore`] to prove the shape a
    /// real engine implements, including that a lease is held once.
    #[derive(Default)]
    struct Memory {
        journeys: Mutex<BTreeMap<(ClusterId, JourneyId), DurableJourneyState>>,
        leases: Mutex<BTreeMap<(ClusterId, JourneyId), RecoveryLease>>,
    }

    impl RuntimeStore for Memory {
        fn persist_journey_state(&self, state: DurableJourneyState) -> Result<(), String> {
            let key = (state.cluster_id, state.journey.journey_id);
            self.journeys
                .lock()
                .map_err(|_| "poisoned")?
                .insert(key, state);
            Ok(())
        }

        fn load_journey_state(
            &self,
            cluster_id: ClusterId,
            journey_id: JourneyId,
        ) -> Result<Option<DurableJourneyState>, String> {
            Ok(self
                .journeys
                .lock()
                .map_err(|_| "poisoned")?
                .get(&(cluster_id, journey_id))
                .cloned())
        }

        fn persist_checkpoint(&self, _: DurableExecutionCheckpoint) -> Result<(), String> {
            Ok(())
        }

        fn load_checkpoint(
            &self,
            _: &DurableRecordIdentity,
        ) -> Result<Option<DurableExecutionCheckpoint>, String> {
            Ok(None)
        }

        fn acquire_recovery_lease(&self, lease: RecoveryLease) -> Result<bool, String> {
            let key = (lease.cluster_id, lease.journey_id);
            let mut leases = self.leases.lock().map_err(|_| "poisoned")?;
            if leases.contains_key(&key) {
                return Ok(false);
            }
            leases.insert(key, lease);
            Ok(true)
        }

        fn release_recovery_lease(&self, lease: &RecoveryLease) -> Result<(), String> {
            let key = (lease.cluster_id, lease.journey_id);
            self.leases.lock().map_err(|_| "poisoned")?.remove(&key);
            Ok(())
        }

        fn remember_deduplication(&self, _: DeduplicationRecord) -> Result<(), String> {
            Ok(())
        }
    }

    const CLUSTER: ClusterId = ClusterId::new(1);

    fn lease() -> RecoveryLease {
        RecoveryLease {
            cluster_id: CLUSTER,
            journey_id: JourneyId::new(7),
            owner_node_id: NodeId::new(2),
            lease_token: "t".to_string(),
            expires_utc: "2026-09-10T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn a_journey_state_comes_back_as_it_was_stored() {
        let store = Memory::default();
        let mut journey = Journey::new(JourneyId::new(7));
        journey.state = JourneyState::Waiting;
        journey.current_xmip_process = Some("orders".to_string());
        let state = DurableJourneyState {
            cluster_id: CLUSTER,
            journey,
            last_known_step: None,
            active_message_ids: vec![MessageId::new(8)],
            audit_position: 3,
        };
        store.persist_journey_state(state.clone()).expect("stored");
        assert_eq!(
            store
                .load_journey_state(CLUSTER, JourneyId::new(7))
                .expect("loaded"),
            Some(state)
        );
        assert_eq!(
            store
                .load_journey_state(CLUSTER, JourneyId::new(9))
                .expect("loaded"),
            None
        );
    }

    #[test]
    fn a_recovery_lease_is_held_by_one_owner_until_released() {
        let store = Memory::default();
        assert!(store.acquire_recovery_lease(lease()).expect("first"));
        assert!(!store.acquire_recovery_lease(lease()).expect("second"));
        store.release_recovery_lease(&lease()).expect("released");
        assert!(store.acquire_recovery_lease(lease()).expect("again"));
    }

    #[test]
    fn a_dismissed_journey_is_a_record_like_any_other() {
        // The store writes every state the Journey defines; the copy this
        // crate carried stopped at Failed and could not record a decision.
        let store = Memory::default();
        let mut journey = Journey::new(JourneyId::new(11));
        journey.state = JourneyState::Dismissed;
        let state = DurableJourneyState {
            cluster_id: CLUSTER,
            journey,
            last_known_step: None,
            active_message_ids: Vec::new(),
            audit_position: 0,
        };
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
        // Written and read as JSON, the shape a real store writes, not only
        // cloned through memory.
        let limit = ChainLimit::new(2);
        let first = Journey::new(JourneyId::new(20));
        let second = Journey::following(
            JourneyId::new(21),
            &first,
            ChainCause::subscription("orders"),
            limit,
        )
        .expect("one link");
        let third = Journey::following(
            JourneyId::new(22),
            &second,
            ChainCause::subscription("orders"),
            limit,
        )
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

        let state = DurableJourneyState {
            cluster_id: CLUSTER,
            journey: third,
            last_known_step: Some("deliver".to_string()),
            active_message_ids: vec![MessageId::new(2)],
            audit_position: 1,
        };
        let written = serde_json::to_string(&state).expect("written");
        let read: DurableJourneyState = serde_json::from_str(&written).expect("read");

        assert_eq!(read, state);
        assert_eq!(read.journey.depth, 2);
        assert_eq!(read.journey.previous_journey_id, Some(JourneyId::new(21)));
        let refused = Journey::following(
            JourneyId::new(23),
            &read.journey,
            ChainCause::subscription("orders"),
            limit,
        );
        assert!(
            refused.is_err(),
            "the limit counts the links made before the restart"
        );
    }
}
