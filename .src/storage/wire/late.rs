//! The operations numbered from 22 on, written as `super`'s `Form` writes
//! the rest: each its number and its fields.

use codec::field;
use codec::form::Form;
use codec::writer::ByteWriter;
use xcore::StreamId;

use super::Request;

impl Request {
    /// The operations numbered from 22 on, written as `write` writes the rest.
    pub(super) fn write_late(&self, out: &mut Vec<u8>) {
        match self {
            Self::Query(query) => {
                out.byte(22);
                query.write(out);
            }
            Self::WriteStream(last, stream) => {
                out.byte(23);
                last.write(out);
                stream.write(out);
            }
            Self::ReadStream(id) => {
                out.byte(24);
                out.u128_be(id.value());
            }
            Self::ReadKeptAuditChunk(id, stream, index) => {
                out.byte(25);
                out.u128_be(id.value());
                out.byte(u8::from(stream.is_some()));
                out.u128_be(stream.map_or(0, StreamId::value));
                out.u32_be(*index);
            }
            Self::ReadKeptAuditStream(id, stream) => {
                out.byte(26);
                out.u128_be(id.value());
                out.u128_be(stream.value());
            }
            Self::ReadFailed(queue, from, most) => {
                out.byte(27);
                out.u128_be(*queue);
                out.u64_be(*from);
                out.u32_be(*most);
            }
            Self::ReadDue(queue, part, (due, place), most) => {
                out.byte(28);
                out.u128_be(*queue);
                part.write(out);
                out.u64_be(*due);
                out.u64_be(*place);
                out.u32_be(*most);
            }
            Self::Standing => {
                out.byte(29);
            }
            Self::Follow(follow) => {
                out.byte(30);
                follow.write(out);
            }
            Self::Turn(turn) => {
                out.byte(31);
                turn.write(out);
            }
            Self::ReadInstance(id) => {
                out.byte(32);
                out.u128_be(*id);
            }
            Self::ReadInstanceKey(id) => {
                out.byte(33);
                out.u128_be(*id);
            }
            Self::WriteSecret(secret) => {
                out.byte(34);
                secret.write(out);
            }
            Self::ReadSecret(name) => {
                out.byte(35);
                field::text(out, name.as_str());
            }
            Self::ListSecrets => {
                out.byte(36);
            }
            Self::Wait(token, holder, seen, bound, due) => {
                out.byte(37);
                out.u128_be(*token);
                field::text(out, holder);
                seen.write(out);
                out.u64_be(*bound);
                out.u32_be(*due);
            }
            Self::EndWait(token) => {
                out.byte(38);
                out.u128_be(*token);
            }
            Self::ReadClaimed(queue, at, journey) => {
                out.byte(39);
                out.u128_be(*queue);
                out.u64_be(*at);
                out.u128_be(journey.value());
            }
            Self::ClaimAhead(claim, lease) => {
                out.byte(40);
                claim.write(out);
                out.u64_be(*lease);
            }
            // Numbered from 41 on.
            _ => self.write_administered(out),
        }
    }

    /// The operations numbered from 41 on, on the administration
    /// database's operator acts and membership, written as `write_late`
    /// writes the rest.
    fn write_administered(&self, out: &mut Vec<u8>) {
        match self {
            Self::AskAct(act) => {
                out.byte(41);
                act.write(out);
            }
            Self::DecideAct(act, outcome, said) => {
                out.byte(42);
                out.u128_be(*act);
                outcome.write(out);
                field::text(out, said);
            }
            Self::ReadActs(node, after, most, open) => {
                out.byte(43);
                field::text(out, node);
                out.u64_be(*after);
                out.u32_be(*most);
                out.byte(u8::from(*open));
            }
            Self::WriteMember(member) => {
                out.byte(44);
                member.write(out);
            }
            Self::ReadMembers => {
                out.byte(45);
            }
            _ => {}
        }
    }
}
