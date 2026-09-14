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

use journey::JourneyState;
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

/// A Journey's state as last written, with where it was and which Message
/// generations it held — enough to resume it from its last checkpoint.
///
/// `state` is every [`JourneyState`], `Dismissed` included: a Journey an
/// operator stopped is a terminal record like any other, and the store has
/// no reason to refuse to write it (ADR-0013).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableJourneyState {
    pub cluster_id: ClusterId,
    pub journey_id: JourneyId,
    pub state: JourneyState,
    pub current_xmip_process: Option<String>,
    pub last_known_step: Option<String>,
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
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// A store in memory: enough of [`RuntimeStore`] to prove the shape a
    /// real engine implements, including that a lease is held once.
    #[derive(Default)]
    struct Memory {
        journeys: Mutex<BTreeMap<(ClusterId, JourneyId), DurableJourneyState>>,
        leases: Mutex<BTreeMap<(ClusterId, JourneyId), RecoveryLease>>,
    }

    impl RuntimeStore for Memory {
        fn persist_journey_state(&self, state: DurableJourneyState) -> Result<(), String> {
            let key = (state.cluster_id, state.journey_id);
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
        let state = DurableJourneyState {
            cluster_id: CLUSTER,
            journey_id: JourneyId::new(7),
            state: JourneyState::Waiting,
            current_xmip_process: Some("orders".to_string()),
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
        let state = DurableJourneyState {
            cluster_id: CLUSTER,
            journey_id: JourneyId::new(11),
            state: JourneyState::Dismissed,
            current_xmip_process: None,
            last_known_step: None,
            active_message_ids: Vec::new(),
            audit_position: 0,
        };
        store.persist_journey_state(state.clone()).expect("stored");
        let loaded = store
            .load_journey_state(CLUSTER, JourneyId::new(11))
            .expect("loaded")
            .expect("present");
        assert!(loaded.state.is_terminal());
        assert_eq!(loaded, state);
    }
}
