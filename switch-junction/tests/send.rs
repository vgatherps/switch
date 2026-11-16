use std::net::{Ipv4Addr, SocketAddrV4};

use switch_junction::control::SocketId;
use switch_junction::send::{SendRecord, TCP_SEND_HEADER, UDP_SEND_HEADER, decode};

#[test]
fn decode_checks_every_reserved_byte_kind_and_header_length() {
    let mut tcp = [0; TCP_SEND_HEADER];
    tcp[0] = 16;
    tcp[4..8].copy_from_slice(&2u32.to_le_bytes());
    tcp[8..12].copy_from_slice(&3u32.to_le_bytes());
    let id = SocketId {
        slot: 2,
        generation: 3,
    };
    assert_eq!(decode(&tcp), Some(SendRecord::Tcp { id, payload: b"" }));
    for len in 0..TCP_SEND_HEADER {
        assert_eq!(decode(&tcp[..len]), None);
    }
    for index in [1, 2, 3, 12, 13, 14, 15] {
        let mut invalid = tcp;
        invalid[index] = 1;
        assert_eq!(decode(&invalid), None, "reserved byte {index}");
    }
    for kind in 0..=u8::MAX {
        if kind != 16 && kind != 17 {
            let mut invalid = tcp;
            invalid[0] = kind;
            assert_eq!(decode(&invalid), None, "unknown kind {kind}");
        }
    }

    let mut udp = [0; UDP_SEND_HEADER];
    udp[..TCP_SEND_HEADER].copy_from_slice(&tcp);
    udp[0] = 17;
    udp[16..20].copy_from_slice(&[192, 0, 2, 9]);
    udp[20..22].copy_from_slice(&0x1234u16.to_le_bytes());
    assert_eq!(
        decode(&udp),
        Some(SendRecord::Udp {
            id,
            to: SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 9), 0x1234),
            payload: b"",
        })
    );
    for len in 0..UDP_SEND_HEADER {
        assert_eq!(decode(&udp[..len]), None);
    }
    for index in [22, 23] {
        let mut invalid = udp;
        invalid[index] = 1;
        assert_eq!(decode(&invalid), None, "reserved byte {index}");
    }
}
