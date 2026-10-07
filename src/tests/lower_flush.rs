use std::collections::VecDeque;

use super::*;

type BArq = Arq<4, 16, BufLink, Crc16X25, BchAckCodec, ManualTimer>;

#[derive(Default)]
struct Ctl {
    pending_flushes: u32,
    waker: Option<Waker>,
    fail_flush: bool,
    flushes: usize,
    /// Every byte delivered to the peer, in order.
    log: Vec<u8>,
    /// Number of upcoming ACK frames to lose at flush time.
    drop_acks: usize,
}

/// A link that stages every written frame and delivers nothing to the peer
/// until it is flushed.
pub(super) struct BufLink {
    rx: Rc<RefCell<VecDeque<Vec<u8>>>>,
    out: Rc<RefCell<VecDeque<Vec<u8>>>>,
    staged: Vec<Vec<u8>>,
    ctl: Rc<RefCell<Ctl>>,
    rx_waker: Rc<RefCell<Option<Waker>>>,
    peer_waker: Rc<RefCell<Option<Waker>>>,
    /// Drops frames at flush time with probability `1 / drop_mod` (seeded,
    /// deterministic); 0 drops nothing.
    pub(super) drop_mod: usize,
    sent: usize,
}

pub(super) fn buf_pair() -> (BufLink, BufLink) {
    let ab = Rc::new(RefCell::new(VecDeque::new()));
    let ba = Rc::new(RefCell::new(VecDeque::new()));
    let (wa, wb) = (Rc::new(RefCell::new(None)), Rc::new(RefCell::new(None)));
    let end = |rx: &Rc<RefCell<VecDeque<Vec<u8>>>>,
               out: &Rc<RefCell<VecDeque<Vec<u8>>>>,
               mine: &Rc<RefCell<Option<Waker>>>,
               peer: &Rc<RefCell<Option<Waker>>>| BufLink {
        rx: rx.clone(),
        out: out.clone(),
        staged: Vec::new(),
        ctl: Rc::default(),
        rx_waker: mine.clone(),
        peer_waker: peer.clone(),
        drop_mod: 0,
        sent: 0x2545F491,
    };
    (end(&ba, &ab, &wa, &wb), end(&ab, &ba, &wb, &wa))
}

impl Transport for BufLink {
    type Error = String;

    fn poll_write(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, String>> {
        self.staged.push(buf.to_vec());
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, String>> {
        let Some(frame) = self.rx.borrow_mut().pop_front() else {
            *self.rx_waker.borrow_mut() = Some(cx.waker().clone());
            return Poll::Pending;
        };
        buf[..frame.len()].copy_from_slice(&frame);
        Poll::Ready(Ok(frame.len()))
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), String>> {
        let mut ctl = self.ctl.borrow_mut();
        if ctl.pending_flushes > 0 {
            ctl.pending_flushes -= 1;
            ctl.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        if core::mem::take(&mut ctl.fail_flush) {
            return Poll::Ready(Err("flush failed".into()));
        }
        ctl.flushes += 1;
        for frame in core::mem::take(&mut self.staged) {
            self.sent = self
                .sent
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let lose_ack = ctl.drop_acks > 0 && matches!(decode_frame(&frame), Ok(Frame::Ack(_)));
            if lose_ack {
                ctl.drop_acks -= 1;
            } else if self.drop_mod == 0 || !(self.sent >> 33).is_multiple_of(self.drop_mod) {
                ctl.log.extend_from_slice(&frame);
                self.out.borrow_mut().push_back(frame);
            }
        }
        drop(ctl);
        if let Some(w) = self.peer_waker.borrow_mut().take() {
            w.wake();
        }
        Poll::Ready(Ok(()))
    }
}

fn buf_arq(link: BufLink, clock: &Clock) -> BArq {
    ArqLayer::<4, Crc16X25, BchAckCodec>::new().build(link, clock.timer())
}

fn data_count(ctl: &Rc<RefCell<Ctl>>) -> usize {
    parse_stream(&ctl.borrow().log)
        .iter()
        .filter(|f| !matches!(f, Frame::Ack(_)))
        .count()
}

fn poll(arq: &mut BArq, op: &mut Op) -> Poll<Result<OpOut, ArqError<String>>> {
    arq.poll_op(&mut noop_cx(), op)
}

/// Writes `data` and flushes on `a` while `b` only reads. Never advances the
/// clock, so the transfer must not depend on retransmission.
fn transfer(a: &mut BArq, b: &mut BArq, data: &[u8]) -> Vec<u8> {
    let mut off = 0;
    let mut flushed = false;
    let mut got = Vec::new();
    for _ in 0..2000 {
        let mut op = if off < data.len() {
            Op::Write { buf: &data[off..] }
        } else {
            Op::Flush
        };
        match poll(a, &mut op) {
            Poll::Ready(Ok(OpOut::Write(n))) => off += n,
            Poll::Ready(Ok(OpOut::Done)) => flushed = true,
            Poll::Ready(other) => panic!("a: {other:?}"),
            Poll::Pending => {}
        }
        let mut buf = [0u8; 64];
        let mut op = Op::Read { buf: &mut buf };
        match poll(b, &mut op) {
            Poll::Ready(Ok(OpOut::Read(n))) => got.extend_from_slice(&buf[..n]),
            Poll::Ready(other) => panic!("b: {other:?}"),
            Poll::Pending => {}
        }
        if flushed {
            break;
        }
    }
    assert!(flushed, "flush did not complete without retransmission");
    got
}

#[test]
fn request_flush_completes_over_buffering_link_without_retransmission() {
    let clock = Clock::default();
    let (la, lb) = buf_pair();
    let (ca, cb) = (la.ctl.clone(), lb.ctl.clone());
    let (mut a, mut b) = (buf_arq(la, &clock), buf_arq(lb, &clock));
    let got = transfer(&mut a, &mut b, b"ping");
    assert_eq!(got, b"ping");
    assert_eq!(data_count(&ca), 1, "sent more than the original frame");
    assert!(cb.borrow().flushes > 0, "ACK was never flushed");
    assert_eq!(clock.0.borrow().now, Duration::ZERO);
}

#[test]
fn multi_frame_transfer_with_buffered_writes() {
    let clock = Clock::default();
    let (la, lb) = buf_pair();
    let (ca, _cb) = (la.ctl.clone(), lb.ctl.clone());
    let (mut a, mut b) = (buf_arq(la, &clock), buf_arq(lb, &clock));
    let data: Vec<u8> = (0..600u32).map(|i| (i % 251) as u8).collect();
    let mut got = transfer(&mut a, &mut b, &data);
    for _ in 0..20 {
        let mut buf = [0u8; 64];
        let mut op = Op::Read { buf: &mut buf };
        if let Poll::Ready(Ok(OpOut::Read(n))) = poll(&mut b, &mut op) {
            got.extend_from_slice(&buf[..n]);
        }
    }
    assert_eq!(got, data);
    let frames = parse_stream(&ca.borrow().log);
    let mut sns: Vec<u16> = frames
        .iter()
        .filter_map(|f| match f {
            Frame::Dat(d) | Frame::DatAckReq(d) => Some(d.sn()),
            _ => None,
        })
        .collect();
    sns.dedup();
    assert_eq!(sns, [0, 1, 2], "frames delivered once each, in order");
}

#[test]
fn shutdown_delivers_fin_over_buffering_link() {
    let clock = Clock::default();
    let (la, lb) = buf_pair();
    let (mut a, mut b) = (buf_arq(la, &clock), buf_arq(lb, &clock));
    let mut a_done = false;
    let mut b_eof = false;
    for _ in 0..2000 {
        if !a_done {
            let mut op = Op::Shutdown;
            match poll(&mut a, &mut op) {
                Poll::Ready(Ok(OpOut::Done)) => a_done = true,
                Poll::Ready(other) => panic!("a: {other:?}"),
                Poll::Pending => {}
            }
        }
        let mut buf = [0u8; 8];
        let mut op = Op::Read { buf: &mut buf };
        match poll(&mut b, &mut op) {
            Poll::Ready(Ok(OpOut::Read(0))) => b_eof = true,
            Poll::Ready(other) => panic!("b: {other:?}"),
            Poll::Pending => {}
        }
        if a_done && b_eof {
            break;
        }
    }
    assert!(a_done && b_eof, "shutdown={a_done} eof={b_eof}");
    assert_eq!(clock.0.borrow().now, Duration::ZERO);
}

#[test]
fn stalled_flush_neither_retransmits_nor_starts_the_timer() {
    let clock = Clock::default();
    let (la, _lb) = buf_pair();
    let ctl = la.ctl.clone();
    ctl.borrow_mut().pending_flushes = 2;
    let mut a = buf_arq(la, &clock);
    let mut op = Op::Write { buf: b"abc" };
    assert!(matches!(
        poll(&mut a, &mut op),
        Poll::Ready(Ok(OpOut::Write(3)))
    ));
    let mut buf = [0u8; 4];
    for _ in 0..2 {
        clock.advance(Duration::from_secs(10));
        let mut op = Op::Read { buf: &mut buf };
        assert!(poll(&mut a, &mut op).is_pending());
        assert!(clock.starts().is_empty(), "timer started before the flush");
    }
    assert_eq!(
        a.channel.staged,
        [wire_dat(0, b"abc")],
        "one frame staged, once"
    );
    let mut op = Op::Read { buf: &mut buf };
    assert!(poll(&mut a, &mut op).is_pending());
    assert_eq!(ctl.borrow().flushes, 1);
    assert_eq!(clock.starts(), [Duration::from_millis(250)]);
    assert_eq!(data_count(&ctl), 1);
}

#[test]
fn lower_flush_error_propagates() {
    let clock = Clock::default();
    let (la, _lb) = buf_pair();
    la.ctl.borrow_mut().fail_flush = true;
    let mut a = buf_arq(la, &clock);
    let mut op = Op::Write { buf: b"abc" };
    assert!(matches!(
        poll(&mut a, &mut op),
        Poll::Ready(Ok(OpOut::Write(3)))
    ));
    let mut op = Op::Flush;
    assert!(matches!(
        poll(&mut a, &mut op),
        Poll::Ready(Err(ArqError::Io(e))) if e == "flush failed"
    ));
}

fn ack_count(ctl: &Rc<RefCell<Ctl>>) -> usize {
    parse_stream(&ctl.borrow().log)
        .iter()
        .filter(|f| matches!(f, Frame::Ack(_)))
        .count()
}

fn poll_ready(arq: &mut BArq, mk: impl Fn() -> Op<'static>) -> OpOut {
    for _ in 0..100 {
        if let Poll::Ready(r) = poll(arq, &mut mk()) {
            return r.expect("arq error");
        }
    }
    panic!("operation did not complete");
}

fn read_eof(arq: &mut BArq) -> bool {
    let mut buf = [0u8; 8];
    matches!(
        poll(arq, &mut Op::Read { buf: &mut buf }),
        Poll::Ready(Ok(OpOut::Read(0)))
    )
}

#[test]
fn lost_fin_ack_is_recovered_after_simultaneous_shutdown() {
    let clock = Clock::default();
    let (la, lb) = buf_pair();
    let (ca, cb) = (la.ctl.clone(), lb.ctl.clone());
    ca.borrow_mut().drop_acks = 1;
    let (mut a, mut b) = (buf_arq(la, &clock), buf_arq(lb, &clock));
    let (mut a_done, mut b_done, mut a_eof) = (false, false, false);
    for _ in 0..200 {
        if !a_done {
            a_done = matches!(
                poll(&mut a, &mut Op::Shutdown),
                Poll::Ready(Ok(OpOut::Done))
            );
        }
        if !b_done {
            b_done = matches!(
                poll(&mut b, &mut Op::Shutdown),
                Poll::Ready(Ok(OpOut::Done))
            );
        }
        if a_done {
            a_eof = read_eof(&mut a);
        }
    }
    assert!(a_done && a_eof && !b_done);
    assert_eq!(a.state, State::Done);
    assert_eq!(ack_count(&ca), 0, "the only ACK from a was lost");

    clock.advance(Duration::from_secs(10));
    for _ in 0..200 {
        if !b_done {
            b_done = matches!(
                poll(&mut b, &mut Op::Shutdown),
                Poll::Ready(Ok(OpOut::Done))
            );
        }
        assert!(read_eof(&mut a));
    }
    assert!(b_done, "retransmitted FIN was never acknowledged");
    assert_eq!(a.state, State::Done);
    assert_eq!(ack_count(&ca), 1, "exactly one replacement ACK");
    assert_eq!(data_count(&ca), 1, "a sent only its own FIN");
    assert_eq!(data_count(&cb), 2, "FIN and one retransmission");
}

#[test]
fn duplicate_fin_ack_survives_buffered_writes_and_pending_flush() {
    let clock = Clock::default();
    let (la, _lb) = buf_pair();
    let (ctl, rx) = (la.ctl.clone(), la.rx.clone());
    let mut a = buf_arq(la, &clock);
    rx.borrow_mut().push_back(wire_fin(0, b""));
    for _ in 0..20 {
        assert!(poll(&mut a, &mut Op::Shutdown).is_pending());
    }
    rx.borrow_mut().push_back(wire_ack(1));
    assert!(matches!(poll_ready(&mut a, || Op::Shutdown), OpOut::Done));
    assert!(read_eof(&mut a));
    assert_eq!(a.state, State::Done);
    assert_eq!(ack_count(&ctl), 1);

    ctl.borrow_mut().pending_flushes = 2;
    rx.borrow_mut().push_back(wire_fin(0, b""));
    let mut buf = [0u8; 8];
    assert!(poll(&mut a, &mut Op::Read { buf: &mut buf }).is_pending());
    assert_eq!(
        ack_count(&ctl),
        1,
        "ACK must not be delivered before its flush"
    );
    // A different operation resumes the same ACK rather than abandoning it.
    assert!(poll(&mut a, &mut Op::Flush).is_pending());
    assert!(matches!(poll_ready(&mut a, || Op::Flush), OpOut::Done));
    assert!(read_eof(&mut a));
    assert_eq!(ack_count(&ctl), 2, "one replacement ACK, none duplicated");
    assert_eq!(data_count(&ctl), 1);
    assert_eq!(a.state, State::Done);
}
