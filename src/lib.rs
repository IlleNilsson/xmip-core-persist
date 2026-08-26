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
