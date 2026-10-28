//! Every stream yields the same messages however it is cut into frames, and
//! each message's direct/scratch placement and stamp follow from the cut.

mod common;

use common::*;
use relay::{Kind, Role};

fn corpus(role: Role) -> Vec<Stream> {
    let mut out = Vec::new();

    let mut s = Stream::new(role);
    s.msg(Kind::Text, br#"{"b":"1.0","a":"1.1"}"#).msg(Kind::Binary, &[0, 1, 2, 3, 4, 5, 6]).msg(Kind::Text, b"");
    out.push(s);

    let mut s = Stream::new(role);
    s.msg(Kind::Text, "€uro 𝄞 ß".as_bytes()).msg(Kind::Binary, &[0xff; 125]).msg(Kind::Binary, &[7; 126]);
    out.push(s);

    // Fragmented, with control frames between fragments and a code point cut by a fragment boundary.
    let mut s = Stream::new(role);
    let t = "a€b".as_bytes();
    s.begin(Kind::Text, &t[..2])
        .ping(b"p1")
        .cont(&t[2..3], false)
        .pong(b"ignored")
        .cont(&t[3..], true)
        .msg(Kind::Binary, b"after")
        .begin(Kind::Binary, b"")
        .cont(b"x", false)
        .ping(b"p2")
        .cont(b"", true);
    out.push(s);

    let mut s = Stream::new(role);
    s.ping(b"").msg(Kind::Binary, b"") .begin(Kind::Binary, &[9; 40]).cont(&[8; 40], true).msg(Kind::Text, b"z");
    out.push(s);
    out
}

/// Chunk index holding stream offset `off`.
fn chunk_of(bounds: &[usize], off: usize) -> usize {
    bounds.iter().filter(|&&b| b <= off).count() - 1
}

fn check(s: &Stream, cuts: &[usize]) {
    let chunks = split(&s.bytes, cuts);
    let mut bounds = vec![0];
    bounds.extend(cuts.iter().copied().filter(|&c| c > 0 && c < s.bytes.len()));
    bounds.dedup();
    let run = drive(&mut rx(s.role), &chunks);
    assert!(run.end.is_none(), "cuts {cuts:?}: {:?}", run.end);
    assert_eq!(run.msgs.len(), s.msgs.len(), "cuts {cuts:?}");
    for (got, want) in run.msgs.iter().zip(&s.msgs) {
        let direct = match &want.single {
            Some(r) if !r.is_empty() => chunk_of(&bounds, r.start) == chunk_of(&bounds, r.end - 1),
            _ => false,
        };
        let expect = Got {
            kind: want.kind,
            payload: want.payload.clone(),
            direct,
            stamp: stamp(chunk_of(&bounds, want.first)),
        };
        assert_eq!(got, &expect, "cuts {cuts:?}");
    }
    assert_eq!(run.pong, s.last_ping, "cuts {cuts:?}");
}

#[test]
#[cfg_attr(miri, ignore)]
fn split_at_every_offset() {
    for role in [Role::Client, Role::Server] {
        for s in corpus(role) {
            for k in 0..=s.bytes.len() {
                check(&s, &[k]);
            }
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn split_at_every_pair_of_offsets() {
    for role in [Role::Client, Role::Server] {
        let s = &corpus(role)[2];
        for i in 0..=s.bytes.len() {
            for j in i..=s.bytes.len() {
                check(s, &[i, j]);
            }
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn random_chunkings() {
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    for role in [Role::Client, Role::Server] {
        for s in corpus(role) {
            for max in [1, 2, 3, 7, 16, 64] {
                for _ in 0..50 {
                    check(&s, &random_cuts(&mut rng, s.bytes.len(), max));
                }
            }
        }
    }
}

/// The length-encoding boundaries, whole and cut into segment-sized frames.
#[test]
#[cfg_attr(miri, ignore)]
fn length_boundaries() {
    let mut rng = Rng(7);
    for role in [Role::Client, Role::Server] {
        for len in [0, 1, 125, 126, 65535, 65536, 70000] {
            let mut s = Stream::new(role);
            s.msg(Kind::Binary, &rng.bytes(len)).msg(Kind::Text, b"next");
            check(&s, &[]);
            for _ in 0..4 {
                check(&s, &random_cuts(&mut rng, s.bytes.len(), 1460));
            }
        }
    }
}
