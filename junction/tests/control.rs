use junction::control::*;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::num::NonZeroU16;

const ID: SocketId = SocketId {
    slot: 0x0102_0304,
    generation: 0xa1a2_a3a4,
};
const TCP: TcpConfig = TcpConfig {
    rx_capacity: 0x0010_2030,
    tx_capacity: 0x1122_3344,
    mss_cap: NonZeroU16::new(1460),
};
const LOCAL: ListenEndpoint = ListenEndpoint {
    addr: None,
    port: 0x1234,
};
const UDP: UdpConfig = UdpConfig {
    local: LOCAL,
    rx_queue: 16,
    tx_checksum: true,
    broadcast: false,
};

fn connect() -> ClientMessage {
    ClientMessage::Op(Op::ConnectTcp {
        id: ID,
        config: TCP,
        remote: SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 1), 443),
        local: LOCAL,
    })
}

fn welcome() -> HostMessage {
    HostMessage::Welcome(Welcome {
        version: VERSION,
        status: WelcomeStatus::Ok,
        stream: 0x0102_0304_0506_0708,
        max_record: 0x1122_3344,
        frame_capacity: 232,
        ingress: "/tmp/j".into(),
        rx: "/rx".into(),
    })
}

fn client_bytes(message: ClientMessage) -> Vec<u8> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes);
    bytes
}

fn host_bytes(message: HostMessage) -> Vec<u8> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes);
    bytes
}

fn client_messages() -> Vec<ClientMessage> {
    let mut messages = vec![
        ClientMessage::Hello {
            version: VERSION,
            max_sockets: 32,
        },
        ClientMessage::Hello {
            version: 17,
            max_sockets: 0,
        },
        connect(),
        ClientMessage::Op(Op::ShutdownTcp { id: ID }),
        ClientMessage::Op(Op::Close {
            id: ID,
            unread: false,
        }),
        ClientMessage::Op(Op::Close {
            id: ID,
            unread: true,
        }),
        ClientMessage::Op(Op::AbortTcp { id: ID }),
    ];
    for addr in [None, Some(Ipv4Addr::LOCALHOST), Some(Ipv4Addr::UNSPECIFIED)] {
        for mss_cap in [None, NonZeroU16::new(1460)] {
            let local = ListenEndpoint { addr, port: 0 };
            let config = TcpConfig { mss_cap, ..TCP };
            messages.push(ClientMessage::Op(Op::ConnectTcp {
                id: ID,
                config,
                remote: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0),
                local,
            }));
            messages.push(ClientMessage::Op(Op::ListenTcp {
                id: ID,
                config,
                local,
            }));
        }
        for tx_checksum in [false, true] {
            for broadcast in [false, true] {
                messages.push(ClientMessage::Op(Op::BindUdp {
                    id: ID,
                    config: UdpConfig {
                        local: ListenEndpoint { addr, ..LOCAL },
                        tx_checksum,
                        broadcast,
                        ..UDP
                    },
                }));
            }
        }
    }
    messages
}

fn host_messages() -> Vec<HostMessage> {
    let mut messages = Vec::new();
    for status in [
        WelcomeStatus::Ok,
        WelcomeStatus::BadVersion,
        WelcomeStatus::SessionsFull,
        WelcomeStatus::BadMaxSockets,
        WelcomeStatus::HostResources,
    ] {
        for version in [VERSION, 17] {
            messages.push(HostMessage::Welcome(Welcome {
                version,
                status,
                stream: 7,
                max_record: 4096,
                frame_capacity: 232,
                ingress: "/tmp/入口".into(),
                rx: "/tmp/受信".into(),
            }));
        }
    }
    for event in [
        Event::Connected { mss: 1460 },
        Event::Connected { mss: 0 },
        Event::Connected { mss: u16::MAX },
        Event::Bound,
        Event::PeerFin,
        Event::Delivered,
        Event::Closed {
            reset_by_peer: false,
        },
        Event::Closed {
            reset_by_peer: true,
        },
        Event::Freed,
    ] {
        messages.push(HostMessage::Event { id: ID, event });
    }
    for code in [
        RejectCode::TcpSocketsFull,
        RejectCode::ConnectInvalidState,
        RejectCode::ConnectUnaddressable,
        RejectCode::ConnectAddressInUse,
        RejectCode::ListenInvalidState,
        RejectCode::ListenUnaddressable,
        RejectCode::UdpSocketsFull,
        RejectCode::UdpPortInUse,
        RejectCode::UdpUnaddressable,
    ] {
        messages.push(HostMessage::Event {
            id: ID,
            event: Event::Rejected(code),
        });
    }
    for reason in [
        EndReason::Kicked,
        EndReason::ProtocolError,
        EndReason::HostShutdown,
        EndReason::Finished,
    ] {
        messages.push(HostMessage::End(reason));
    }
    messages
}

#[test]
fn every_client_message_roundtrips_and_preserves_prefixes() {
    for message in client_messages() {
        let bytes = client_bytes(message);
        assert_eq!(
            ClientMessage::decode(&bytes),
            Ok(Some((message, bytes.len())))
        );
        for end in 0..bytes.len() {
            assert_eq!(
                ClientMessage::decode(&bytes[..end]),
                Ok(None),
                "kind {}, prefix {end}",
                bytes[0]
            );
        }
        if let ClientMessage::Op(op) = message {
            assert_eq!(op.id(), ID);
        }
        let mut appended = vec![0xaa, 0xbb];
        message.encode(&mut appended);
        assert_eq!(&appended[..2], &[0xaa, 0xbb]);
        assert_eq!(&appended[2..], bytes);
    }
}

#[test]
fn every_host_message_roundtrips_and_preserves_prefixes() {
    for message in host_messages() {
        let bytes = host_bytes(message.clone());
        assert_eq!(
            HostMessage::decode(&bytes),
            Ok(Some((message.clone(), bytes.len())))
        );
        for end in 0..bytes.len() {
            assert_eq!(
                HostMessage::decode(&bytes[..end]),
                Ok(None),
                "kind {}, prefix {end}",
                bytes[0]
            );
        }
        let mut appended = vec![0xaa, 0xbb];
        message.encode(&mut appended);
        assert_eq!(&appended[..2], &[0xaa, 0xbb]);
        assert_eq!(&appended[2..], bytes);
    }
}

#[test]
fn golden_bytes() {
    enum Message {
        Client(ClientMessage),
        Host(HostMessage),
    }

    let cases: [(Message, &[u8]); 3] = [
        (
            Message::Host(HostMessage::End(EndReason::Finished)),
            &[72, 0, 1, 0, 0, 0, 0, 0, 4],
        ),
        (
            Message::Client(connect()),
            &[
                2, 0, 36, 0, 0, 0, 0, 0, 0x04, 0x03, 0x02, 0x01, 0xa4, 0xa3, 0xa2, 0xa1, 0x30,
                0x20, 0x10, 0x00, 0x44, 0x33, 0x22, 0x11, 0xb4, 0x05, 0, 0, 192, 0, 2, 1, 0xbb,
                0x01, 0, 0, 0, 0, 0, 0, 0x34, 0x12, 0, 0,
            ],
        ),
        (
            Message::Host(welcome()),
            &[
                64, 0, 37, 0, 0, 0, 0, 0, b'J', b'N', b'C', b'T', 3, 0, 0, 0, 8, 7, 6, 5, 4, 3, 2,
                1, 0x44, 0x33, 0x22, 0x11, 232, 0, 0, 0, 6, 0, b'/', b't', b'm', b'p', b'/', b'j', 3, 0, b'/',
                b'r', b'x',
            ],
        ),
    ];
    for (message, expected) in cases {
        match message {
            Message::Client(message) => {
                assert_eq!(client_bytes(message), expected);
                assert_eq!(
                    ClientMessage::decode(expected),
                    Ok(Some((message, expected.len())))
                );
            }
            Message::Host(message) => {
                assert_eq!(host_bytes(message.clone()), expected);
                assert_eq!(
                    HostMessage::decode(expected),
                    Ok(Some((message, expected.len())))
                );
            }
        }
    }
}

#[test]
fn one_message_and_part_of_next_consumes_only_first() {
    let first = connect();
    let mut bytes = client_bytes(first);
    let consumed = bytes.len();
    let second = client_bytes(ClientMessage::Op(Op::Close {
        id: ID,
        unread: true,
    }));
    bytes.extend_from_slice(&second[..second.len() - 1]);
    assert_eq!(ClientMessage::decode(&bytes), Ok(Some((first, consumed))));
    assert_eq!(ClientMessage::decode(&bytes[consumed..]), Ok(None));

    let first = welcome();
    let mut bytes = host_bytes(first.clone());
    let consumed = bytes.len();
    let second = host_bytes(HostMessage::End(EndReason::Kicked));
    bytes.extend_from_slice(&second[..second.len() - 1]);
    assert_eq!(HostMessage::decode(&bytes), Ok(Some((first, consumed))));
    assert_eq!(HostMessage::decode(&bytes[consumed..]), Ok(None));
}

fn raw_header(kind: u8, flags: u8, len: u16) -> [u8; HEADER_LEN] {
    let len = len.to_le_bytes();
    [kind, flags, len[0], len[1], 0, 0, 0, 0]
}

#[test]
fn unknown_kinds_include_wrong_direction() {
    for kind in [0, 8, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 255] {
        assert_eq!(
            ClientMessage::decode(&raw_header(kind, 0, 8)),
            Err(DecodeError::UnknownKind(kind))
        );
    }
    for kind in [0, 1, 2, 3, 4, 5, 6, 7, 63, 73, 255] {
        assert_eq!(
            HostMessage::decode(&raw_header(kind, 0, 8)),
            Err(DecodeError::UnknownKind(kind))
        );
    }
}

#[test]
fn fixed_lengths_reject_from_header_alone() {
    for (kind, len) in [(1, 8), (2, 36), (3, 28), (4, 24), (5, 8), (6, 8), (7, 8)] {
        for bad_len in [len - 1, len + 1] {
            assert_eq!(
                ClientMessage::decode(&raw_header(kind, 0, bad_len)),
                Err(DecodeError::WrongLength { kind, len: bad_len })
            );
        }
    }
    for (kind, len) in [
        (65, 10),
        (66, 8),
        (67, 9),
        (68, 8),
        (69, 8),
        (70, 8),
        (71, 8),
        (72, 1),
    ] {
        for bad_len in [len - 1, len + 1] {
            assert_eq!(
                HostMessage::decode(&raw_header(kind, 0, bad_len)),
                Err(DecodeError::WrongLength { kind, len: bad_len })
            );
        }
    }
    assert_eq!(
        HostMessage::decode(&raw_header(64, 0, 21)),
        Err(DecodeError::WrongLength { kind: 64, len: 21 })
    );
}

#[test]
fn header_padding_and_flags_reject_without_body() {
    for message in client_messages() {
        let bytes = client_bytes(message);
        let mut header = bytes[..HEADER_LEN].to_vec();
        header[4] = 1;
        assert_eq!(ClientMessage::decode(&header), Err(DecodeError::NonzeroPad));
        header[4] = 0;
        header[1] = 2;
        assert_eq!(
            ClientMessage::decode(&header),
            Err(DecodeError::UnknownFlags {
                kind: header[0],
                flags: 2
            })
        );
        if header[0] != 6 {
            header[1] = 1;
            assert_eq!(
                ClientMessage::decode(&header),
                Err(DecodeError::UnknownFlags {
                    kind: header[0],
                    flags: 1
                })
            );
        }
    }
    for message in host_messages() {
        let bytes = host_bytes(message);
        let mut header = bytes[..HEADER_LEN].to_vec();
        header[7] = 1;
        assert_eq!(HostMessage::decode(&header), Err(DecodeError::NonzeroPad));
        header[7] = 0;
        header[1] = 2;
        assert_eq!(
            HostMessage::decode(&header),
            Err(DecodeError::UnknownFlags {
                kind: header[0],
                flags: 2
            })
        );
        if header[0] != 70 {
            header[1] = 1;
            assert_eq!(
                HostMessage::decode(&header),
                Err(DecodeError::UnknownFlags {
                    kind: header[0],
                    flags: 1
                })
            );
        }
    }
}

#[test]
fn body_padding_is_zero() {
    for offset in [26, 27, 34, 35, 43] {
        let mut bytes = client_bytes(connect());
        bytes[offset] = 1;
        assert_eq!(ClientMessage::decode(&bytes), Err(DecodeError::NonzeroPad));
    }
    for offset in [26, 27, 35] {
        let mut bytes = client_bytes(ClientMessage::Op(Op::ListenTcp {
            id: ID,
            config: TCP,
            local: LOCAL,
        }));
        bytes[offset] = 1;
        assert_eq!(ClientMessage::decode(&bytes), Err(DecodeError::NonzeroPad));
    }
    for offset in [23, 30, 31] {
        let mut bytes = client_bytes(ClientMessage::Op(Op::BindUdp {
            id: ID,
            config: UDP,
        }));
        bytes[offset] = 1;
        assert_eq!(ClientMessage::decode(&bytes), Err(DecodeError::NonzeroPad));
    }
    let mut bytes = host_bytes(welcome());
    bytes[15] = 1;
    assert_eq!(HostMessage::decode(&bytes), Err(DecodeError::NonzeroPad));
}

#[test]
fn invalid_boolean_bytes_are_rejected() {
    for offset in [22, 28, 29] {
        let mut bytes = client_bytes(ClientMessage::Op(Op::BindUdp {
            id: ID,
            config: UDP,
        }));
        bytes[offset] = 2;
        assert_eq!(
            ClientMessage::decode(&bytes),
            Err(DecodeError::InvalidBool(2))
        );
    }
    let mut bytes = client_bytes(connect());
    bytes[42] = 2;
    assert_eq!(
        ClientMessage::decode(&bytes),
        Err(DecodeError::InvalidBool(2))
    );
}

#[test]
fn absent_listen_address_requires_zero_bytes() {
    let mut bytes = client_bytes(connect());
    bytes[36] = 127;
    assert_eq!(
        ClientMessage::decode(&bytes),
        Err(DecodeError::NonzeroAbsentAddress)
    );
}

#[test]
fn magic_is_checked_in_both_handshakes() {
    let mut bytes = client_bytes(ClientMessage::Hello {
        version: VERSION,
        max_sockets: 32,
    });
    bytes[8..12].copy_from_slice(&0x0102_0304u32.to_le_bytes());
    assert_eq!(
        ClientMessage::decode(&bytes),
        Err(DecodeError::BadMagic(0x0102_0304))
    );
    let mut bytes = host_bytes(welcome());
    bytes[8..12].copy_from_slice(&0x0102_0304u32.to_le_bytes());
    assert_eq!(
        HostMessage::decode(&bytes),
        Err(DecodeError::BadMagic(0x0102_0304))
    );
}

#[test]
fn invalid_enum_codes_are_rejected() {
    let mut bytes = host_bytes(welcome());
    bytes[14] = 5;
    assert_eq!(
        HostMessage::decode(&bytes),
        Err(DecodeError::InvalidWelcomeStatus(5))
    );
    for code in [0, 10, 255] {
        let mut bytes = host_bytes(HostMessage::Event {
            id: ID,
            event: Event::Rejected(RejectCode::TcpSocketsFull),
        });
        bytes[16] = code;
        assert_eq!(
            HostMessage::decode(&bytes),
            Err(DecodeError::InvalidRejectCode(code))
        );
    }
    for reason in [0, 5, 255] {
        let mut bytes = host_bytes(HostMessage::End(EndReason::Kicked));
        bytes[8] = reason;
        assert_eq!(
            HostMessage::decode(&bytes),
            Err(DecodeError::InvalidEndReason(reason))
        );
    }
}

#[test]
fn welcome_path_must_be_utf8() {
    for offset in [34, 42] {
        let mut bytes = host_bytes(welcome());
        bytes[offset] = 255;
        let error = std::str::from_utf8(&bytes[offset..]).unwrap_err();
        assert_eq!(
            HostMessage::decode(&bytes),
            Err(DecodeError::InvalidUtf8(error))
        );
    }
}

#[test]
fn welcome_length_must_match_path_length() {
    let mut bytes = host_bytes(welcome());
    for ingress_len in [0u16, 5, 7, u16::MAX] {
        bytes[32..34].copy_from_slice(&ingress_len.to_le_bytes());
        assert_eq!(
            HostMessage::decode(&bytes),
            Err(DecodeError::WrongLength { kind: 64, len: 37 })
        );
    }
    for rx_len in [0u16, 2, 4, u16::MAX] {
        let mut bytes = host_bytes(welcome());
        bytes[40..42].copy_from_slice(&rx_len.to_le_bytes());
        assert_eq!(
            HostMessage::decode(&bytes),
            Err(DecodeError::WrongLength { kind: 64, len: 37 })
        );
    }
}

#[test]
fn invalid_configs_are_checked_directly_and_on_decode() {
    let invalid_tcp = TcpConfig {
        rx_capacity: (1 << 30) + 1,
        ..TCP
    };
    assert_eq!(invalid_tcp.check(), Err(ConfigError::TcpRxCapacityTooLarge));
    for op in [
        Op::ConnectTcp {
            id: ID,
            config: invalid_tcp,
            remote: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            local: LOCAL,
        },
        Op::ListenTcp {
            id: ID,
            config: invalid_tcp,
            local: LOCAL,
        },
    ] {
        assert_eq!(
            ClientMessage::decode(&client_bytes(ClientMessage::Op(op))),
            Err(DecodeError::InvalidConfig(
                ConfigError::TcpRxCapacityTooLarge
            ))
        );
    }
    for (config, error) in [
        (
            UdpConfig {
                local: ListenEndpoint { port: 0, ..LOCAL },
                ..UDP
            },
            ConfigError::UdpPortZero,
        ),
        (
            UdpConfig { rx_queue: 0, ..UDP },
            ConfigError::UdpRxQueueZero,
        ),
    ] {
        assert_eq!(config.check(), Err(error));
        let bytes = client_bytes(ClientMessage::Op(Op::BindUdp { id: ID, config }));
        assert_eq!(
            ClientMessage::decode(&bytes),
            Err(DecodeError::InvalidConfig(error))
        );
    }
}

#[test]
fn config_checks_do_not_add_stack_rejections() {
    for rx_capacity in [0, 1 << 30] {
        let config = TcpConfig {
            rx_capacity,
            tx_capacity: u32::MAX,
            mss_cap: None,
        };
        assert_eq!(config.check(), Ok(()));
        let message = ClientMessage::Op(Op::ListenTcp {
            id: ID,
            config,
            local: ListenEndpoint {
                addr: Some(Ipv4Addr::new(203, 0, 113, 1)),
                port: 0,
            },
        });
        let bytes = client_bytes(message);
        assert_eq!(
            ClientMessage::decode(&bytes),
            Ok(Some((message, bytes.len())))
        );
    }
    assert_eq!(
        UdpConfig {
            rx_queue: u32::MAX,
            ..UDP
        }
        .check(),
        Ok(())
    );
}

#[test]
fn welcome_accepts_largest_encodable_path_and_empty_path() {
    for (ingress_len, rx_len) in [
        (0, 0),
        (usize::from(u16::MAX) - 28, 0),
        (0, usize::from(u16::MAX) - 28),
        (3, usize::from(u16::MAX) - 31),
    ] {
        let message = HostMessage::Welcome(Welcome {
            version: VERSION,
            status: WelcomeStatus::Ok,
            stream: 1,
            max_record: 1,
            frame_capacity: 232,
            ingress: "a".repeat(ingress_len),
            rx: "b".repeat(rx_len),
        });
        let bytes = host_bytes(message.clone());
        assert_eq!(
            HostMessage::decode(&bytes),
            Ok(Some((message, bytes.len())))
        );
    }
}

#[test]
#[should_panic(expected = "Welcome paths exceed the control body length")]
fn welcome_panics_when_body_length_would_overflow() {
    host_bytes(HostMessage::Welcome(Welcome {
        version: VERSION,
        status: WelcomeStatus::Ok,
        stream: 1,
        max_record: 1,
        frame_capacity: 232,
        ingress: "a".repeat(usize::from(u16::MAX) - 27),
        rx: String::new(),
    }));
}
