//! Who the Storage plane admits, and to what (`authorize::plane`): a
//! node's certificate to every operation, a person's to none.

use authorize::plane::Rule;

/// A caller whose certificate authenticates a client alone: a person's.
pub(super) const PERSON: u8 = 0;
/// A caller whose certificate authenticates a server too: a node's.
pub(super) const NODE: u8 = 1;

/// Who may ask the Storage plane what: every read and every write a
/// node's, the secrets' operations named so none of them falls to a
/// broader rule later.
pub(crate) const RULES: &[Rule] = &[
    Rule::new("read", "read_secret", NODE),
    Rule::new("read", "list_secrets", NODE),
    Rule::new("write", "write_secret", NODE),
    Rule::new("read", "*", NODE),
    Rule::new("write", "*", NODE),
];

/// Who the connection proved, and its standing: a node or a person, by
/// what its certificate authenticates.
pub(super) fn caller(guarded: &tls::GuardedServer) -> (Option<String>, u8) {
    let Some(leaf) = guarded.conn.peer_certificates().and_then(<[_]>::first) else {
        return (None, PERSON);
    };
    let who = tls::peer::Peer::of(&guarded.conn)
        .ok()
        .flatten()
        .map(|peer| peer.subject);
    let standing = if tls::peer::authenticates_servers(leaf) {
        NODE
    } else {
        PERSON
    };
    (who, standing)
}
