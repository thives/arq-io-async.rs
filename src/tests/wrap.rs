use std::collections::VecDeque;

use std::boxed::Box;

use super::*;
use crate::MAX_SEQ;
use crate::frame::MAX_PAYLOAD;

type WArq<const N: usize, const R: usize, L> = Arq<N, 16, R, L, Crc16X25, BchAckCodec, ManualTimer>;

fn wrap_arq<const N: usize, const R: usize, L>(link: L, clock: &Clock, sn: u16) -> WArq<N, R, L> {
    let mut arq = ArqLayer::<N, Crc16X25, BchAckCodec>::new().build_with_timer(link, clock.timer());
    arq.set_seq(sn);
    arq
}

fn seq(sn: u16, k: usize) -> u16 {
    ((sn as usize + k) % MAX_SEQ as usize) as u16
}

fn data_frames(tx: &[u8]) -> Vec<(u16, Vec<u8>)> {
    parse_stream(tx)
        .into_iter()
        .filter_map(|f| match f {
            Frame::Dat(d) | Frame::DatAckReq(d) | Frame::Fin(d) => {
                Some((d.sn(), d.payload().to_vec()))
            }
            Frame::Ack(_) => None,
        })
        .collect()
}

fn acks(tx: &[u8]) -> Vec<u16> {
    parse_stream(tx)
        .into_iter()
        .filter_map(|f| match f {
            Frame::Ack(a) => Some(a.an()),
            _ => None,
        })
        .collect()
}

/// Queues `count` full frames, the `k`-th filled with byte `tag + k`, and
/// sends them without waiting for acknowledgements.
fn send_full_frames<const N: usize, const R: usize>(
    arq: &mut WArq<N, R, MockLink>,
    count: usize,
    tag: u8,
) {
    let mut cx = noop_cx();
    for k in 0..count {
        let chunk = [tag + k as u8; MAX_PAYLOAD];
        let mut op = Op::Write { buf: &chunk };
        assert!(matches!(
            arq.poll_op(&mut cx, &mut op),
            Poll::Ready(Ok(OpOut::Write(MAX_PAYLOAD)))
        ));
    }
    poll_read_pending(arq);
}

fn poll_read_pending<const N: usize, const R: usize>(arq: &mut WArq<N, R, MockLink>) {
    let mut cx = noop_cx();
    let mut buf = [0u8; 1];
    for _ in 0..2 * N + 2 {
        let mut op = Op::Read { buf: &mut buf };
        assert!(arq.poll_op(&mut cx, &mut op).is_pending());
    }
}

fn read_all<const N: usize, const R: usize>(arq: &mut WArq<N, R, MockLink>) -> Vec<u8> {
    let mut cx = noop_cx();
    let mut got = Vec::new();
    let mut buf = [0u8; 512];
    for _ in 0..8 {
        let mut op = Op::Read { buf: &mut buf };
        match arq.poll_op(&mut cx, &mut op) {
            Poll::Ready(Ok(OpOut::Read(n))) => got.extend_from_slice(&buf[..n]),
            Poll::Ready(other) => panic!("unexpected read: {other:?}"),
            Poll::Pending => {}
        }
    }
    got
}

#[test]
fn ring_addresses_window_across_wrap() {
    let crc = crc16();
    let mut ring = crate::Ring::<6>::new();
    ring.base = 16380;
    let sns = [16380, 16381, 16382, 16383, 0, 1];
    for sn in sns {
        assert!(ring.insert(DatFrame::new_dat(&crc, sn, &sn.to_le_bytes())));
    }
    assert!(
        !ring.insert(DatFrame::new_dat(&crc, 2, b"x")),
        "outside the window"
    );
    assert!(
        !ring.insert(DatFrame::new_dat(&crc, 16379, b"x")),
        "behind the window"
    );
    for sn in sns {
        assert_eq!(ring.get(sn).unwrap().payload(), sn.to_le_bytes());
    }
    for (k, sn) in sns.iter().enumerate().take(3) {
        assert_eq!(ring.advance().unwrap().sn(), *sn);
        assert_eq!(ring.base, sns[k + 1]);
        assert!(ring.get(*sn).is_none());
    }
    for sn in [2, 3, 4] {
        assert!(ring.insert(DatFrame::new_dat(&crc, sn, &sn.to_le_bytes())));
    }
    for sn in [16383, 0, 1, 2, 3, 4] {
        assert_eq!(ring.get(sn).unwrap().payload(), sn.to_le_bytes());
    }
    // Advancing over an empty slot keeps later frames at their sequence.
    let mut ring = crate::Ring::<6>::new();
    ring.base = 16382;
    assert!(ring.insert(DatFrame::new_dat(&crc, 1, b"b")));
    assert!(ring.advance().is_none());
    assert!(ring.advance().is_none());
    assert!(ring.advance().is_none());
    assert_eq!(ring.get(1).unwrap().payload(), b"b");
    assert_eq!(ring.advance().unwrap().sn(), 1);
}

#[test]
fn full_send_window_across_wrap_retransmits_original_frames() {
    let clock = Clock::default();
    let start = MAX_SEQ - 4;
    let mut arq: WArq<6, { r::<6>() }, _> = wrap_arq(MockLink::new(), &clock, start);
    send_full_frames(&mut arq, 6, 10);
    let first = data_frames(&arq.channel.tx);
    let expect: Vec<u16> = (0..6).map(|k| seq(start, k)).collect();
    assert_eq!(first.iter().map(|f| f.0).collect::<Vec<_>>(), expect);
    // Earliest frames were lost; the timeout must resend the same frames.
    arq.channel.tx.clear();
    clock.advance(arq.rto);
    poll_read_pending(&mut arq);
    let resent = data_frames(&arq.channel.tx);
    assert_eq!(resent.len(), 6);
    for (k, (sn, payload)) in resent.iter().enumerate() {
        assert_eq!(*sn, seq(start, k));
        assert!(
            payload.iter().all(|&b| b == 10 + k as u8),
            "frame {sn} has wrong payload"
        );
    }
}

#[test]
fn cumulative_ack_across_wrap_keeps_remaining_frames() {
    let clock = Clock::default();
    let start = MAX_SEQ - 2;
    let mut arq: WArq<6, { r::<6>() }, _> = wrap_arq(MockLink::new(), &clock, start);
    send_full_frames(&mut arq, 6, 20);
    // Acknowledge MAX_SEQ-2, MAX_SEQ-1, 0 and 1.
    arq.channel.rx.extend(wire_ack(2));
    poll_read_pending(&mut arq);
    assert_eq!(arq.sb, 2);
    assert_eq!(arq.w, 2);
    arq.channel.tx.clear();
    clock.advance(arq.rto);
    poll_read_pending(&mut arq);
    let resent = data_frames(&arq.channel.tx);
    assert_eq!(resent.iter().map(|f| f.0).collect::<Vec<_>>(), [2, 3]);
    assert!(resent[0].1.iter().all(|&b| b == 24));
    assert!(resent[1].1.iter().all(|&b| b == 25));
}

#[test]
fn ack_during_retransmission_across_wrap() {
    let clock = Clock::default();
    let start = MAX_SEQ - 3;
    let mut arq: WArq<6, { r::<6>() }, _> = wrap_arq(MockLink::new(), &clock, start);
    send_full_frames(&mut arq, 6, 30);
    arq.channel.tx.clear();
    clock.advance(arq.rto);
    let mut cx = noop_cx();
    let mut buf = [0u8; 1];
    // Each poll sends one frame; let two retransmissions go out.
    for _ in 0..2 {
        let mut op = Op::Read { buf: &mut buf };
        assert!(arq.poll_op(&mut cx, &mut op).is_pending());
    }
    let resent = data_frames(&arq.channel.tx);
    assert_eq!(
        resent.iter().map(|f| f.0).collect::<Vec<_>>(),
        [start, seq(start, 1)]
    );
    // Acknowledge everything up to and including sequence 0.
    arq.channel.rx.extend(wire_ack(1));
    poll_read_pending(&mut arq);
    let resent = data_frames(&arq.channel.tx);
    let sns: Vec<u16> = resent.iter().map(|f| f.0).collect();
    assert_eq!(sns, [start, seq(start, 1), 1, 2]);
    assert!(resent[2].1.iter().all(|&b| b == 34));
    assert!(resent[3].1.iter().all(|&b| b == 35));
}

#[test]
fn out_of_order_receive_across_wrap() {
    let clock = Clock::default();
    let start = MAX_SEQ - 2;
    let mut arq: WArq<6, { r::<6>() }, _> = wrap_arq(MockLink::new(), &clock, start);
    // Sequences that share `sn % 6` with buffered predecessors.
    for k in [1usize, 4, 5, 2, 3] {
        arq.channel.rx.extend(wire_dat(seq(start, k), &[k as u8]));
    }
    assert!(read_all(&mut arq).is_empty());
    arq.channel.rx.extend(wire_dat(start, &[0]));
    assert_eq!(read_all(&mut arq), [0, 1, 2, 3, 4, 5]);
    assert_eq!(arq.rn, seq(start, 6));
}

#[test]
fn duplicate_and_stale_frames_around_wrap() {
    let clock = Clock::default();
    let start = MAX_SEQ - 1;
    let mut arq: WArq<6, { r::<6>() }, _> = wrap_arq(MockLink::new(), &clock, start);
    arq.channel.rx.extend(wire_dat_ack_req(start, b"a"));
    arq.channel.rx.extend(wire_dat_ack_req(0, b"b"));
    assert_eq!(read_all(&mut arq), b"ab");
    assert_eq!(arq.rn, 1);
    let before = acks(&arq.channel.tx).len();
    // Stale duplicates from before and after the wrap are re-ACKed only.
    arq.channel.rx.extend(wire_dat(start, b"a"));
    arq.channel.rx.extend(wire_dat(0, b"b"));
    assert!(read_all(&mut arq).is_empty());
    let after = acks(&arq.channel.tx);
    assert!(after.len() > before);
    assert!(after[before..].iter().all(|&an| an == 1));
    // A buffered future frame survives a duplicate of itself.
    arq.channel.rx.extend(wire_dat(2, b"d"));
    arq.channel.rx.extend(wire_dat(2, b"d"));
    arq.channel.rx.extend(wire_dat(1, b"c"));
    assert_eq!(read_all(&mut arq), b"cd");
    assert_eq!(arq.rn, 3);
}

/// A byte pipe between two instances that accepts every send whole and can
/// drop selected frames.
struct Pipe {
    rx: Rc<RefCell<VecDeque<u8>>>,
    tx: Rc<RefCell<VecDeque<u8>>>,
    drop: Box<dyn FnMut(&Frame) -> bool>,
}

fn pipes(
    drop_ab: impl FnMut(&Frame) -> bool + 'static,
    drop_ba: impl FnMut(&Frame) -> bool + 'static,
) -> (Pipe, Pipe) {
    let ab = Rc::new(RefCell::new(VecDeque::new()));
    let ba = Rc::new(RefCell::new(VecDeque::new()));
    (
        Pipe {
            rx: ba.clone(),
            tx: ab.clone(),
            drop: Box::new(drop_ab),
        },
        Pipe {
            rx: ab,
            tx: ba,
            drop: Box::new(drop_ba),
        },
    )
}

impl FrameIo for Pipe {
    type Error = Infallible;

    fn poll_send(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        let frames = parse_stream(buf);
        assert_eq!(frames.len(), 1, "a pipe send must carry exactly one frame");
        if !(self.drop)(&frames[0]) {
            self.tx.borrow_mut().extend(buf);
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_recv(
        &mut self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        let mut rx = self.rx.borrow_mut();
        if rx.is_empty() {
            return Poll::Pending;
        }
        let n = rx.len().min(buf.len());
        for (dst, src) in buf.iter_mut().zip(rx.drain(..n)) {
            *dst = src;
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
}

/// Sends `len` bytes from `a` to `b`, starting both at `start`, while the
/// pipe drops the first transmission of the first two data frames and every
/// third ACK.
fn lossy_wrap_transfer<const N: usize, const R: usize>(start: u16, len: usize) {
    let mut first_seen = Vec::new();
    let mut nacks = 0usize;
    let (pa, pb) = pipes(
        move |f| match f {
            Frame::Dat(d) | Frame::DatAckReq(d) | Frame::Fin(d) => {
                let fresh = !first_seen.contains(&d.sn());
                first_seen.push(d.sn());
                fresh && (d.sn() == start || d.sn() == seq(start, 1))
            }
            Frame::Ack(_) => false,
        },
        move |f| {
            nacks += matches!(f, Frame::Ack(_)) as usize;
            matches!(f, Frame::Ack(_)) && nacks.is_multiple_of(3)
        },
    );
    let clock = Clock::default();
    let mut a: WArq<N, R, _> = wrap_arq(pa, &clock, start);
    let mut b: WArq<N, R, _> = wrap_arq(pb, &clock, start);
    let data: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
    let mut cx = noop_cx();
    let mut off = 0usize;
    let mut flushed = false;
    let mut got = Vec::new();
    let mut buf = [0u8; 300];
    let mut idle = 0usize;
    for _ in 0..200_000 {
        let mut progressed = false;
        let mut op = if off < data.len() {
            Op::Write { buf: &data[off..] }
        } else {
            Op::Flush
        };
        match a.poll_op(&mut cx, &mut op) {
            Poll::Ready(Ok(OpOut::Write(n))) => {
                off += n;
                progressed = true;
            }
            Poll::Ready(Ok(OpOut::Done)) => flushed = true,
            Poll::Ready(other) => panic!("a: {other:?}"),
            Poll::Pending => {}
        }
        let mut op = Op::Read { buf: &mut buf };
        match b.poll_op(&mut cx, &mut op) {
            Poll::Ready(Ok(OpOut::Read(n))) => {
                got.extend_from_slice(&buf[..n]);
                progressed = true;
            }
            Poll::Ready(other) => panic!("b: {other:?}"),
            Poll::Pending => {}
        }
        if flushed && got.len() == data.len() {
            break;
        }
        idle = if progressed { 0 } else { idle + 1 };
        if idle > 8 {
            assert!(clock.advance_to_next(), "stalled without a pending timer");
            idle = 0;
        }
    }
    assert!(flushed, "N={N}: flush did not complete");
    assert_eq!(got, data, "N={N}: stream corrupted");
    let frames = len.div_ceil(MAX_PAYLOAD);
    assert!(frames > 2 * N, "N={N}: the transfer must wrap the window");
    assert_eq!(a.sb, b.rn, "N={N}");
    assert_eq!(dist(start, a.sb) as usize, frames, "N={N}");
}

macro_rules! wrap_transfers {
    ($($name:ident: $n:literal),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                for back in [1u16, $n / 2, $n, $n + 1] {
                    lossy_wrap_transfer::<$n, { r::<$n>() }>(MAX_SEQ - back, ($n * 3 + 1) * MAX_PAYLOAD);
                }
            }
        )*
    };
}

wrap_transfers! {
    lossy_wrap_transfer_n2: 2,
    lossy_wrap_transfer_n4: 4,
    lossy_wrap_transfer_n6: 6,
    lossy_wrap_transfer_n8: 8,
    lossy_wrap_transfer_n10: 10,
    lossy_wrap_transfer_n12: 12,
    lossy_wrap_transfer_n14: 14,
    lossy_wrap_transfer_n16: 16,
    lossy_wrap_transfer_n18: 18,
    lossy_wrap_transfer_n20: 20,
    lossy_wrap_transfer_n22: 22,
    lossy_wrap_transfer_n24: 24,
    lossy_wrap_transfer_n26: 26,
    lossy_wrap_transfer_n28: 28,
    lossy_wrap_transfer_n30: 30,
    lossy_wrap_transfer_n32: 32,
}
