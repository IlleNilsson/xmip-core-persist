//! What each request on the wire asks, in the words the Storage plane's
//! admission reads (`authorize::plane`): whether it reads or writes, and
//! the operation it is.

use super::Request;

impl Request {
    /// What it does — `read` or `write` — and the operation, as the
    /// Storage plane's rules name it.
    pub(crate) const fn operation(&self) -> (&'static str, &'static str) {
        match self {
            Self::WriteChunk(_) => ("write", "write_chunk"),
            Self::ReadChunk(..) => ("read", "read_chunk"),
            Self::WriteMessage(_) => ("write", "write_message"),
            Self::ReadMessage(_) => ("read", "read_message"),
            Self::WriteJourney(_) => ("write", "write_journey"),
            Self::ReadJourney(_) => ("read", "read_journey"),
            Self::Claim(..) => ("write", "claim"),
            Self::Renew(..) => ("write", "renew"),
            Self::Release(_) => ("write", "release"),
            Self::HandOn(_) => ("write", "hand_on"),
            Self::WriteAudit(_) => ("write", "write_audit"),
            Self::KeepAudit(..) => ("write", "keep_audit"),
            Self::ReadKeptAudit(_) => ("read", "read_kept_audit"),
            Self::WriteAdministration(_) => ("write", "write_administration"),
            Self::ReadAdministration(..) => ("read", "read_administration"),
            Self::RemoveAdministration(..) => ("write", "remove_administration"),
            Self::Publish(_) => ("write", "publish"),
            Self::ReadHeld(..) => ("read", "read_held"),
            Self::ReadDead(..) => ("read", "read_dead"),
            Self::ReadDeadMessage(..) => ("read", "read_dead_message"),
            Self::Replay(_) => ("write", "replay"),
            Self::Query(_) => ("read", "query"),
            Self::WriteStream(..) => ("write", "write_stream"),
            Self::ReadStream(_) => ("read", "read_stream"),
            Self::ReadKeptAuditChunk(..) => ("read", "read_kept_audit_chunk"),
            Self::ReadKeptAuditStream(..) => ("read", "read_kept_audit_stream"),
            Self::ReadFailed(..) => ("read", "read_failed"),
            Self::ReadDue(..) => ("read", "read_due"),
            Self::Standing => ("read", "standing"),
            Self::Follow(_) => ("read", "follow"),
            Self::Turn(_) => ("write", "turn"),
            Self::ReadInstance(_) => ("read", "read_instance"),
            Self::ReadInstanceKey(_) => ("read", "read_instance_key"),
            Self::WriteSecret(_) => ("write", "write_secret"),
            Self::ReadSecret(_) => ("read", "read_secret"),
            Self::ListSecrets => ("read", "list_secrets"),
        }
    }
}
