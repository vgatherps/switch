//! Packet-send records carried by a session's cephalopod stream.

use std::net::{Ipv4Addr, SocketAddrV4};

use crate::control::SocketId;

pub const TCP_SEND_HEADER: usize = 16;
pub const UDP_SEND_HEADER: usize = 24;

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

/// Decodes a little-endian send record, rejecting unknown kinds and reserved bits.
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
