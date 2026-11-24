//! Who the Storage plane admits, and to what (`authorize::plane`): a
//! node's certificate to every operation but following the primary, which
//! only a Storage node listed asks, a person's to none.

use authorize::plane::Rule;

use super::super::standby::StorageNode;

/// A caller whose certificate authenticates a client alone: a person's.
pub(super) const PERSON: u8 = 0;
/// A caller whose certificate authenticates a server too: a node's.
pub(super) const NODE: u8 = 1;
/// A node whose certificate is one a Storage node listed presents: that
/// Storage node, which alone may follow the primary, be handed its writes,
/// whole copies and data keys, and be named its successor (open problem
/// 135; ADR-0056, amendment 2026-11-24).
pub(super) const STORAGE: u8 = 2;

/// Who may ask the Storage plane what: following the primary a Storage
/// node's, every other read and every write a node's, the secrets'
/// operations named so none of them falls to a broader rule later.
pub(crate) const RULES: &[Rule] = &[
    Rule::new("read", "follow", STORAGE),
    Rule::new("read", "read_secret", NODE),
    Rule::new("read", "list_secrets", NODE),
    Rule::new("write", "write_secret", NODE),
    Rule::new("read", "*", NODE),
    Rule::new("write", "*", NODE),
];

/// The standing of a node of `standing` that proved, on `guarded`, the
/// certificate a Storage node `node` lists presents: a Storage node's, and
/// its place among those listed, worked out by `node` and never said by the
/// caller (`standby::proven`); as it is otherwise.
pub(super) fn storage_node(
    guarded: &tls::GuardedServer,
    node: &StorageNode,
    standing: u8,
) -> (u8, Option<usize>) {
    let leaf = guarded.conn.peer_certificates().and_then(<[_]>::first);
    match leaf.and_then(|leaf| node.listed_as(leaf.as_ref())) {
        Some(place) if standing == NODE => (STORAGE, Some(place)),
        _ => (standing, None),
    }
}

/// The node a caller of `standing` whose certificate's subject is `who`
/// is: its common name (`tls::Name`). None for a person, or a subject
/// naming no node. What it wrote and took is counted against it
/// (`counted`), and a standby it is, following, is known by it
/// (`standby::followers`).
pub(super) fn node(who: Option<&str>, standing: u8) -> Option<String> {
    let who = tls::Name::parse(who.filter(|_| standing == NODE)?);
    who.common_name().map(str::to_string)
}

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
