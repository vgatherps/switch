//! The send records a session commits to the send ring.
//!
//! Each send is one record in the send ring. A send-ring record has a 32-byte header that
//! carries, at offsets 0, 8, 16 and 24, the commit tag, the length of the rest of the record, the session's
//! stream id ([`Welcome::stream`](crate::control::Welcome::stream)) and a sequence number
//! that counts from 0 for each session. The record's payload starts with a send header,
//! followed directly by the data:
//!
//! ```text
//!  TCP send, 16-byte header          UDP send, 24-byte header
//!  offset size field                 offset size field
//!  0      1    kind = 16             0      1    kind = 17
//!  1      3    zero                  1      3    zero
//!  4      4    slot (u32)            4      4    slot (u32)
//!  8      4    generation (u32)      8      4    generation (u32)
//!  12     4    zero                  12     4    zero
//!  16     ...  payload               16     4    destination address
//!                                    20     2    destination port (u16)
//!                                    22     2    zero
//!                                    24     ...  payload
//! ```
//!
//! Integers are little-endian; the address is in network order. Header and data together are
//! at most [`Welcome::max_record`](crate::control::Welcome::max_record) bytes, so a TCP
//! payload is at most `max_record - 16` bytes and a UDP payload `max_record - 24`. The session
//! ends its stream with an empty record whose sequence number has bit 63 set.

use std::net::{Ipv4Addr, SocketAddrV4};

use crate::control::SocketId;

/// Length of the TCP send header.
pub const TCP_SEND_HEADER: usize = 16;
/// Length of the UDP send header.
pub const UDP_SEND_HEADER: usize = 24;

/// A decoded send record, borrowing its payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendRecord<'a> {
    Tcp {
        id: SocketId,
        payload: &'a [u8],
    },
    Udp {
        id: SocketId,
        to: SocketAddrV4,
        payload: &'a [u8],
    },
}

/// Decodes one send record. Returns `None` for an unknown kind, a record shorter than its
/// header, or nonzero reserved bytes.
pub fn decode(record: &[u8]) -> Option<SendRecord<'_>> {
    if record.len() < TCP_SEND_HEADER || record[1..4] != [0; 3] || record[12..16] != [0; 4] {
        return None;
    }
    let id = SocketId {
        slot: u32::from_le_bytes(record[4..8].try_into().unwrap()),
        generation: u32::from_le_bytes(record[8..12].try_into().unwrap()),
    };
    match record[0] {
        16 => Some(SendRecord::Tcp {
            id,
            payload: &record[TCP_SEND_HEADER..],
        }),
        17 if record.len() >= UDP_SEND_HEADER && record[22..24] == [0; 2] => {
            Some(SendRecord::Udp {
                id,
                to: SocketAddrV4::new(
                    Ipv4Addr::new(record[16], record[17], record[18], record[19]),
                    u16::from_le_bytes(record[20..22].try_into().unwrap()),
                ),
                payload: &record[UDP_SEND_HEADER..],
            })
        }
        _ => None,
    }
}

pub(crate) fn write_tcp(record: &mut [u8], id: SocketId) {
    record[..TCP_SEND_HEADER].fill(0);
    record[0] = 16;
    record[4..8].copy_from_slice(&id.slot.to_le_bytes());
    record[8..12].copy_from_slice(&id.generation.to_le_bytes());
}

pub(crate) fn write_udp(record: &mut [u8], id: SocketId, to: SocketAddrV4) {
    write_tcp(record, id);
    record[0] = 17;
    record[16..20].copy_from_slice(&to.ip().octets());
    record[20..22].copy_from_slice(&to.port().to_le_bytes());
    record[22..24].fill(0);
}
