use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableRecordIdentity {
    pub cluster_name: String,
    pub node_name: String,
    pub journey_id: Uuid,
    pub message_id: Option<Uuid>,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableJourneyState {
    pub cluster_name: String,
    pub journey_id: Uuid,
    pub state: JourneyRecoveryState,
    pub current_xmip_process: Option<String>,
    pub last_known_step: Option<String>,
    pub active_message_ids: Vec<Uuid>,
    pub audit_position: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum JourneyRecoveryState {
    Active,
    Waiting,
    Suspended,
    Recovering,
    Completed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryLease {
    pub cluster_name: String,
    pub journey_id: Uuid,
    pub owner_node_name: String,
    pub lease_token: String,
    pub expires_utc: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeduplicationRecord {
    pub journey_id: Uuid,
    pub message_id: Uuid,
    pub source_fingerprint: String,
}

pub trait RuntimeStore {
    fn persist_journey_state(&self, state: DurableJourneyState) -> Result<(), String>;
    fn load_journey_state(
        &self,
        cluster_name: &str,
        journey_id: Uuid,
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
        journeys: Mutex<BTreeMap<(String, Uuid), DurableJourneyState>>,
        leases: Mutex<BTreeMap<(String, Uuid), RecoveryLease>>,
    }

    impl RuntimeStore for Memory {
        fn persist_journey_state(&self, state: DurableJourneyState) -> Result<(), String> {
            let key = (state.cluster_name.clone(), state.journey_id);
            self.journeys
                .lock()
                .map_err(|_| "poisoned")?
                .insert(key, state);
            Ok(())
        }

        fn load_journey_state(
            &self,
            cluster_name: &str,
            journey_id: Uuid,
        ) -> Result<Option<DurableJourneyState>, String> {
            let key = (cluster_name.to_string(), journey_id);
            Ok(self
                .journeys
                .lock()
                .map_err(|_| "poisoned")?
                .get(&key)
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
            let key = (lease.cluster_name.clone(), lease.journey_id);
            let mut leases = self.leases.lock().map_err(|_| "poisoned")?;
            if leases.contains_key(&key) {
                return Ok(false);
            }
            leases.insert(key, lease);
            Ok(true)
        }

        fn release_recovery_lease(&self, lease: &RecoveryLease) -> Result<(), String> {
            let key = (lease.cluster_name.clone(), lease.journey_id);
            self.leases.lock().map_err(|_| "poisoned")?.remove(&key);
            Ok(())
        }

        fn remember_deduplication(&self, _: DeduplicationRecord) -> Result<(), String> {
            Ok(())
        }
    }

    fn lease() -> RecoveryLease {
        RecoveryLease {
            cluster_name: "c".to_string(),
            journey_id: Uuid::from_u128(7),
            owner_node_name: "n1".to_string(),
            lease_token: "t".to_string(),
            expires_utc: "2026-09-10T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn a_journey_state_comes_back_as_it_was_stored() {
        let store = Memory::default();
        let state = DurableJourneyState {
            cluster_name: "c".to_string(),
            journey_id: Uuid::from_u128(7),
            state: JourneyRecoveryState::Waiting,
            current_xmip_process: Some("orders".to_string()),
            last_known_step: None,
            active_message_ids: vec![Uuid::from_u128(8)],
            audit_position: 3,
        };
        store.persist_journey_state(state.clone()).expect("stored");
        assert_eq!(
            store
                .load_journey_state("c", Uuid::from_u128(7))
                .expect("loaded"),
            Some(state)
        );
        assert_eq!(
            store
                .load_journey_state("c", Uuid::from_u128(9))
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
}
