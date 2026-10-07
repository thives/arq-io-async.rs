use core::cell::RefCell;
use core::convert::Infallible;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::collections::VecDeque;
use std::rc::Rc;
use std::{string::String, vec, vec::Vec};

use crate::Transport;
use crate::ack_codec::BchAckCodec;
use crate::arq::Arq;
use crate::arq::{ArqLayer, MAX_FRAME, Op, OpOut, State, dist};
use crate::error::{ArqError, FrameError};
use crate::frame::{AckFrame, DatFrame, Frame, TYPE_ACK, TYPE_FIN};
use crate::timer::Timer;

mod asyncio;
mod bch_limit;
mod collision;
mod duplex;
mod framed;
mod integrated;
mod lower_flush;
mod recovery;
mod transport;
mod wake;
mod wrap;

type Crc16X25 = ::crc::Crc<u16>;
type TestArq = Arq<4, 16, MockLink, Crc16X25, BchAckCodec, ManualTimer>;

fn crc16() -> Crc16X25 {
    Crc16X25::new(&::crc::CRC_16_IBM_SDLC)
}

fn make_arq() -> TestArq {
    ArqLayer::<4, Crc16X25, BchAckCodec>::new().build(MockLink::new(), Clock::default().timer())
}

fn new_arq<L: Transport>(link: L) -> Arq<4, 16, L, Crc16X25, BchAckCodec, ManualTimer> {
    ArqLayer::<4, Crc16X25, BchAckCodec>::new().build(link, Clock::default().timer())
}

#[derive(Default)]
struct ClockState {
    now: Duration,
    waiters: Vec<Option<(Duration, Waker)>>,
    starts: Vec<Duration>,
}

#[derive(Clone, Default)]
struct Clock(Rc<RefCell<ClockState>>);

impl Clock {
    fn timer(&self) -> ManualTimer {
        let mut s = self.0.borrow_mut();
        s.waiters.push(None);
        ManualTimer {
            clock: self.clone(),
            id: s.waiters.len() - 1,
            deadline: None,
        }
    }

    fn advance(&self, d: Duration) {
        let now = self.0.borrow().now + d;
        self.set(now);
    }

    fn advance_to_next(&self) -> bool {
        let next = self
            .0
            .borrow()
            .waiters
            .iter()
            .flatten()
            .map(|(d, _)| *d)
            .min();
        match next {
            Some(d) => {
                let now = self.0.borrow().now.max(d);
                self.set(now);
                true
            }
            None => false,
        }
    }

    fn set(&self, now: Duration) {
        let mut ready = Vec::new();
        {
            let mut s = self.0.borrow_mut();
            s.now = now;
            for slot in s.waiters.iter_mut() {
                if matches!(slot, Some((d, _)) if *d <= now) {
                    ready.push(slot.take().unwrap().1);
                }
            }
        }
        for w in ready {
            w.wake();
        }
    }

    fn starts(&self) -> Vec<Duration> {
        self.0.borrow().starts.clone()
    }
}

struct ManualTimer {
    clock: Clock,
    id: usize,
    deadline: Option<Duration>,
}

impl Timer for ManualTimer {
    fn start(&mut self, timeout: Duration) {
        let mut s = self.clock.0.borrow_mut();
        self.deadline = Some(s.now + timeout);
        s.waiters[self.id] = None;
        s.starts.push(timeout);
    }

    fn stop(&mut self) {
        self.deadline = None;
        self.clock.0.borrow_mut().waiters[self.id] = None;
    }

    fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(deadline) = self.deadline else {
            return Poll::Pending;
        };
        let mut s = self.clock.0.borrow_mut();
        if s.now >= deadline {
            return Poll::Ready(());
        }
        s.waiters[self.id] = Some((deadline, cx.waker().clone()));
        Poll::Pending
    }
}

fn expire<L: Transport>(arq: &Arq<4, 16, L, Crc16X25, BchAckCodec, ManualTimer>) {
    arq.timer.clock.advance(arq.rto);
}

/// Queue of whole frames waiting to be read by the engine.
#[derive(Clone, Default, PartialEq, Debug)]
struct FrameQueue(VecDeque<Vec<u8>>);

impl FrameQueue {
    /// Queues one frame exactly as given.
    fn push(&mut self, frame: Vec<u8>) {
        self.0.push_back(frame);
    }

    /// Queues the frames of a concatenation of valid wire frames. A remainder
    /// whose length cannot be determined is queued as one frame.
    fn extend(&mut self, bytes: Vec<u8>) {
        let mut rest = bytes.as_slice();
        while !rest.is_empty() {
            let len = split_len(rest).unwrap_or(rest.len()).min(rest.len());
            self.0.push_back(rest[..len].to_vec());
            rest = &rest[len..];
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

struct MockLink {
    rx: FrameQueue,
    /// Every frame written, concatenated.
    tx: Vec<u8>,
    /// Every frame written, one entry each.
    sent: Vec<Vec<u8>>,
    eof: bool,
}

impl MockLink {
    fn new() -> Self {
        Self {
            rx: FrameQueue::default(),
            tx: Vec::new(),
            sent: Vec::new(),
            eof: false,
        }
    }
}

impl Transport for MockLink {
    type Error = Infallible;

    fn poll_write(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        self.tx.extend_from_slice(buf);
        self.sent.push(buf.to_vec());
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_read(
        &mut self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        match self.rx.0.pop_front() {
            Some(frame) => {
                assert!(frame.len() <= buf.len(), "oversized frame in the harness");
                buf[..frame.len()].copy_from_slice(&frame);
                Poll::Ready(Ok(frame.len()))
            }
            None if self.eof => Poll::Ready(Ok(0)),
            None => Poll::Pending,
        }
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
}

struct Peer {
    next: u16,
    seen: Vec<u16>,
    silent: bool,
    tx_to_us: Vec<Vec<u8>>,
}

impl Peer {
    fn new() -> Self {
        Self {
            next: 0,
            seen: Vec::new(),
            silent: false,
            tx_to_us: Vec::new(),
        }
    }

    fn push(&mut self, bytes: Vec<u8>) {
        self.tx_to_us.push(bytes);
    }

    fn respond(&mut self, arq: &mut TestArq, offset: &mut usize) {
        let fresh = &arq.channel.tx[*offset..];
        *offset = arq.channel.tx.len();
        if !self.silent {
            for frame in parse_stream(fresh) {
                match frame {
                    Frame::Dat(d) | Frame::DatAckReq(d) | Frame::Fin(d) => {
                        let sn = d.sn();
                        let dup = self.seen.contains(&sn);
                        if !dup {
                            self.seen.push(sn);
                        }
                        if sn == self.next || dup {
                            self.push(wire_ack(sn.wrapping_add(1)));
                            if sn == self.next {
                                self.next = sn.wrapping_add(1);
                            }
                        }
                    }
                    Frame::Ack(_) => {}
                }
            }
        }
        for frame in core::mem::take(&mut self.tx_to_us) {
            arq.channel.rx.push(frame);
        }
    }
}

fn noop_cx() -> Context<'static> {
    static W: std::sync::OnceLock<&'static Waker> = std::sync::OnceLock::new();
    Context::from_waker(W.get_or_init(Waker::noop))
}

fn drive(
    arq: &mut TestArq,
    op: &mut Op,
    peer: &mut Peer,
    offset: &mut usize,
    max_steps: usize,
) -> Option<Result<OpOut, ArqError<Infallible>>> {
    let mut cx = noop_cx();
    for _ in 0..max_steps {
        match arq.poll_op(&mut cx, op) {
            Poll::Ready(r) => return Some(r),
            Poll::Pending => peer.respond(arq, offset),
        }
    }
    None
}

fn drive_out(
    arq: &mut TestArq,
    op: &mut Op,
    peer: &mut Peer,
    offset: &mut usize,
    max_steps: usize,
) -> OpOut {
    match drive(arq, op, peer, offset, max_steps) {
        Some(Ok(out)) => out,
        Some(Err(e)) => panic!("arq error: {e:?}"),
        None => panic!("no progress within {max_steps} steps"),
    }
}

fn encode_frame(frame: &Frame) -> Vec<u8> {
    let mut buf = [0u8; 256];
    let n = frame
        .to_bytes::<BchAckCodec, 16>(&mut buf)
        .expect("frame encode");
    buf[..n].to_vec()
}

fn wire_ack(an: u16) -> Vec<u8> {
    encode_frame(&Frame::Ack(AckFrame::new(&crc16(), an).unwrap()))
}

fn wire_dat(sn: u16, payload: &[u8]) -> Vec<u8> {
    encode_frame(&Frame::Dat(DatFrame::new_dat(&crc16(), sn, payload)))
}

fn wire_dat_ack_req(sn: u16, payload: &[u8]) -> Vec<u8> {
    encode_frame(&Frame::DatAckReq(DatFrame::new_dat_ack_req(
        &crc16(),
        sn,
        payload,
    )))
}

fn wire_fin(sn: u16, payload: &[u8]) -> Vec<u8> {
    encode_frame(&Frame::Fin(DatFrame::new_fin(&crc16(), sn, payload)))
}

/// Length of the valid wire frame at the start of `bytes`, from its header.
fn split_len(bytes: &[u8]) -> Option<usize> {
    match bytes.first()? & 0b11 {
        TYPE_ACK => Some(1 + 16),
        _ => Some(5 + *bytes.get(2)? as usize),
    }
}

fn wire_len(bytes: &[u8]) -> Result<usize, FrameError> {
    split_len(bytes).ok_or(FrameError::TooShort(bytes.len()))
}

fn decode_frame(bytes: &[u8]) -> Result<Frame, FrameError> {
    Frame::from_bytes::<BchAckCodec, 16, _>(&crc16(), bytes)
}

fn parse_stream(bytes: &[u8]) -> Vec<Frame> {
    let mut out = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let len = wire_len(rest).expect("wire len");
        let frame = decode_frame(&rest[..len]).expect("frame decode");
        out.push(frame);
        rest = &rest[len..];
    }
    out
}

fn write_until(arq: &mut TestArq, data: &[u8], peer: &mut Peer, offset: &mut usize) {
    let mut off = 0usize;
    while off < data.len() {
        let mut op = Op::Write { buf: &data[off..] };
        match drive_out(arq, &mut op, peer, offset, 100) {
            OpOut::Write(n) => off += n,
            other => panic!("unexpected write result: {other:?}"),
        }
    }
}

#[test]
fn codec_roundtrip() {
    let crc = crc16();
    let mut buf = [0u8; 256];
    let f = DatFrame::new_dat(&crc, 42, b"hello");
    let n = f.to_bytes(&mut buf);
    let g = DatFrame::from_bytes(&crc, &buf[..n]).unwrap();
    assert_eq!(g.sn(), 42);
    assert_eq!(g.payload(), b"hello");
    assert!(!g.is_fin());
    let f = DatFrame::new_fin(&crc, 7, b"x");
    let n = f.to_bytes(&mut buf);
    let g = DatFrame::from_bytes(&crc, &buf[..n]).unwrap();
    assert_eq!(g.sn(), 7);
    assert_eq!(g.payload(), b"x");
    assert!(g.is_fin());
    assert!(!g.requests_ack());
    let f = DatFrame::new_dat_ack_req(&crc, 300, b"req");
    let n = f.to_bytes(&mut buf);
    assert_eq!(buf[0] & 0b11, 0b00);
    match Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &buf[..n]).unwrap() {
        Frame::DatAckReq(g) => {
            assert_eq!(g.sn(), 300);
            assert_eq!(g.payload(), b"req");
            assert!(g.requests_ack());
            assert!(!g.is_fin());
        }
        other => panic!("unexpected frame: {other:?}"),
    }
    let plain = DatFrame::new_dat(&crc, 300, b"req");
    assert!(!plain.requests_ack());
    let marked = plain.to_ack_req(&crc);
    assert!(marked.requests_ack());
    assert_eq!(encode_frame(&Frame::DatAckReq(marked)), buf[..n].to_vec());
    let a = AckFrame::new(&crc, 99).unwrap();
    let n = Frame::Ack(a).to_bytes::<BchAckCodec, 16>(&mut buf).unwrap();
    match Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &buf[..n]).unwrap() {
        Frame::Ack(a2) => assert_eq!(a2.an(), 99),
        other => panic!("unexpected frame: {other:?}"),
    }
}

#[test]
fn framing_errors() {
    let crc = crc16();
    let mut bytes = wire_dat(0, b"hello");
    bytes[9] ^= 0xFF;
    assert!(Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &bytes).is_err());
    let mut bytes = wire_dat(0, b"hi");
    bytes[0] = 0x01;
    assert!(matches!(
        Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &bytes),
        Err(FrameError::LengthMismatch(17, _))
    ));
    let mut bytes = wire_dat(0, b"hi");
    bytes[0] &= !0b11;
    assert!(matches!(
        Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &bytes),
        Err(FrameError::CrcMismatch(..))
    ));
    let bytes = wire_dat(0, b"hello");
    assert!(matches!(
        Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &bytes[..4]),
        Err(FrameError::TooShort(_))
    ));
}

#[test]
fn wire_len_covers_all_data_types() {
    for bytes in [
        wire_dat(3, b"abc"),
        wire_dat_ack_req(3, b"abc"),
        wire_fin(3, b"abc"),
    ] {
        assert_eq!(wire_len(&bytes).unwrap(), 8);
    }
    assert_eq!(wire_len(&wire_ack(3)).unwrap(), 17);
    let mut stream = wire_dat_ack_req(0, b"x");
    stream.extend(wire_dat(1, b"yz"));
    let frames = parse_stream(&stream);
    assert!(matches!(frames[0], Frame::DatAckReq(d) if d.sn() == 0));
    assert!(matches!(frames[1], Frame::Dat(d) if d.sn() == 1));
}

#[test]
fn bidirectional_transfer() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    let mut op = Op::Shutdown;
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 100),
        OpOut::Done
    ));
    peer.push(wire_dat(0, b"xy"));
    peer.push(wire_fin(1, b"z"));
    peer.respond(&mut arq, &mut off);
    let got = read_to_end(&mut arq, &mut peer, &mut off);
    assert_eq!(got, b"xyz");
    assert_eq!(arq.state, State::Done);
}

#[test]
fn large_transfer_reassembly() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let data: Vec<u8> = (0..600u16).map(|i| (i % 251) as u8).collect();
    peer.push(wire_dat(0, &data[0..250]));
    peer.push(wire_dat(1, &data[250..500]));
    peer.push(wire_fin(2, &data[500..600]));
    peer.respond(&mut arq, &mut off);
    let mut got = Vec::new();
    loop {
        let mut buf = [0u8; 100];
        let mut op = Op::Read { buf: &mut buf };
        match drive_out(&mut arq, &mut op, &mut peer, &mut off, 100) {
            OpOut::Read(0) => break,
            OpOut::Read(n) => got.extend_from_slice(&buf[..n]),
            other => panic!("unexpected read result: {other:?}"),
        }
    }
    assert_eq!(got, data);
}

#[test]
fn out_of_order_frames() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(wire_dat(1, b"bc"));
    peer.respond(&mut arq, &mut off);
    let mut buf = [0u8; 4];
    let mut op = Op::Read { buf: &mut buf };
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 6).is_none());
    peer.push(wire_dat(0, b"a"));
    peer.push(wire_fin(2, b"d"));
    peer.respond(&mut arq, &mut off);
    match drive_out(&mut arq, &mut op, &mut peer, &mut off, 100) {
        OpOut::Read(4) => assert_eq!(&buf[..], b"abcd"),
        other => panic!("unexpected read result: {other:?}"),
    }
}

#[test]
fn retransmit_on_lost_ack() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    peer.silent = true;
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 20).is_none());
    expire(&arq);
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 20).is_none());
    let frames = parse_stream(&arq.channel.tx);
    let copies = frames
        .iter()
        .filter(|f| matches!(f, Frame::DatAckReq(d) if d.sn() == 0))
        .count();
    assert_eq!(copies, 2, "expected one retransmission, saw {frames:?}");
    peer.silent = false;
    expire(&arq);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 20),
        OpOut::Done
    ));
    assert_eq!(arq.w, 0);
}

#[test]
fn window_full_backpressure() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let data: Vec<u8> = (0..1250u16).map(|i| (i % 252) as u8).collect();
    write_until(&mut arq, &data, &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 200),
        OpOut::Done
    ));
    let frames = parse_stream(&arq.channel.tx);
    let sns: Vec<u16> = frames
        .iter()
        .filter_map(|f| match f {
            Frame::Dat(d) | Frame::DatAckReq(d) => Some(d.sn()),
            _ => None,
        })
        .collect();
    for sn in 0..5u16 {
        assert!(sns.contains(&sn), "missing frame {sn}, saw {sns:?}");
    }
}

#[test]
fn shutdown_with_no_data_sends_fin() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let mut op = Op::Shutdown;
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 100),
        OpOut::Done
    ));
    let frames = parse_stream(&arq.channel.tx);
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        Frame::Fin(d) => {
            assert_eq!(d.sn(), 0);
            assert_eq!(d.len(), 0);
        }
        other => panic!("unexpected frame: {other:?}"),
    }
}

#[test]
fn retransmit_cycles_window() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let data: Vec<u8> = vec![0u8; 1000];
    write_until(&mut arq, &data, &mut peer, &mut off);
    peer.silent = true;
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 20).is_none());
    for _ in 0..2 {
        expire(&arq);
        assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 20).is_none());
    }
    let frames = parse_stream(&arq.channel.tx);
    let sns: Vec<u16> = frames
        .iter()
        .filter_map(|f| match f {
            Frame::Dat(d) | Frame::Fin(d) => Some(d.sn()),
            _ => None,
        })
        .collect();
    assert_eq!(
        sns,
        [0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3],
        "each timeout retransmits the window once, in order"
    );
}

#[test]
fn duplicate_in_order_frame_ignored() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(wire_dat(0, b"a"));
    peer.push(wire_dat(0, b"a"));
    peer.push(wire_fin(1, b"b"));
    peer.respond(&mut arq, &mut off);
    let got = read_to_end(&mut arq, &mut peer, &mut off);
    assert_eq!(got, b"ab");
}

fn read_to_end(arq: &mut TestArq, peer: &mut Peer, off: &mut usize) -> Vec<u8> {
    let mut got = Vec::new();
    loop {
        let mut buf = [0u8; 64];
        let mut op = Op::Read { buf: &mut buf };
        match drive_out(arq, &mut op, peer, off, 100) {
            OpOut::Read(0) => return got,
            OpOut::Read(n) => got.extend_from_slice(&buf[..n]),
            other => panic!("unexpected read result: {other:?}"),
        }
    }
}

#[test]
fn corrupted_dat_is_discarded_and_not_terminal() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let mut bytes = wire_dat(0, b"hello world!");
    bytes[9] ^= 0xFF;
    peer.push(bytes);
    peer.respond(&mut arq, &mut off);
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 9).is_none());
    assert!(!arq.failed);
    assert!(arq.channel.rx.is_empty(), "the frame was consumed");
    assert_eq!(arq.rn, 0);
    assert!(arq.channel.tx.is_empty(), "no ACK for a discarded frame");
}

#[test]
fn valid_frames_after_malformed_frame_are_delivered() {
    let mut corrupt_dat = wire_dat(0, b"hello");
    corrupt_dat[7] ^= 0x10;
    let mut oversized = vec![0x02, 0x00, 0xFF];
    oversized.resize(16, 0);
    for (label, malformed) in [
        ("corrupt DAT", corrupt_dat),
        ("uncorrectable ACK", vec![0x01; 17]),
        ("oversized length", oversized),
    ] {
        let mut arq = make_arq();
        let mut peer = Peer::new();
        let mut off = 0usize;
        peer.push(malformed);
        peer.push(wire_dat(0, b"a"));
        peer.push(wire_fin(1, b"b"));
        peer.respond(&mut arq, &mut off);
        assert_eq!(read_to_end(&mut arq, &mut peer, &mut off), b"ab", "{label}");
        assert_eq!(arq.rn, 2, "{label}");
        assert!(!arq.failed, "{label}");
    }
}

#[test]
fn corrupt_frame_before_ack_does_not_block_the_ack() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10).is_none());
    let mut bad = wire_dat(5, b"eleven byte");
    bad[6] ^= 0x01;
    peer.push(bad);
    peer.push(wire_ack(1));
    peer.respond(&mut arq, &mut off);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 20),
        OpOut::Done
    ));
    assert_eq!((arq.sb, arq.w), (1, 0));
}

/// A DAT whose length byte is corrupted to zero. The bytes the original
/// length covered are an ACK for sequence 1 and a valid DAT.
fn length_corrupted_dat() -> Vec<u8> {
    let mut inner = wire_ack(1);
    inner.extend(wire_dat(0, b"forged"));
    let mut bytes = wire_dat(0, &inner);
    assert_eq!(bytes[2] as usize, inner.len());
    bytes[2] = 0;
    bytes
}

/// A FIN whose type bits are corrupted to ACK. The payload is an ACK for
/// sequence 1 and a valid DAT.
fn type_corrupted_fin() -> Vec<u8> {
    let mut inner = wire_ack(1);
    inner.extend(wire_dat(0, b"forged"));
    let mut bytes = wire_fin(0, &inner);
    bytes[0] ^= TYPE_FIN ^ TYPE_ACK;
    bytes
}

fn stream_with_outstanding_write() -> TestArq {
    let mut arq = make_arq();
    let mut cx = noop_cx();
    assert!(matches!(
        arq.poll_op(&mut cx, &mut Op::Write { buf: b"abc" }),
        Poll::Ready(Ok(OpOut::Write(3)))
    ));
    assert!(arq.poll_op(&mut cx, &mut Op::Flush).is_pending());
    assert_eq!(arq.w, 1);
    arq.channel.tx.clear();
    arq
}

#[test]
fn header_corruption_cannot_acknowledge_outstanding_data() {
    let mut cx = noop_cx();
    for bad in [length_corrupted_dat(), type_corrupted_fin()] {
        let mut arq = stream_with_outstanding_write();
        arq.channel.rx.push(bad);
        arq.channel.rx.push(wire_dat(0, b"after"));
        for _ in 0..2 {
            assert!(arq.poll_op(&mut cx, &mut Op::Flush).is_pending());
        }
        assert!(arq.channel.rx.is_empty());
        assert_eq!((arq.sb, arq.w), (0, 1));
        assert!(!arq.fin_acked && !arq.tx_done);
        assert!(!arq.failed);
        assert_eq!(arq.rn, 1, "only the valid frame advanced the receiver");
        assert_eq!(arq.read_buf.len(), 5);
    }
}

#[test]
fn header_corruption_cannot_deliver_embedded_data() {
    let mut cx = noop_cx();
    for bad in [length_corrupted_dat(), type_corrupted_fin()] {
        let mut arq = make_arq();
        arq.channel.rx.push(bad);
        let mut buf = [0u8; 64];
        for _ in 0..2 {
            assert!(
                arq.poll_op(&mut cx, &mut Op::Read { buf: &mut buf })
                    .is_pending()
            );
        }
        assert_eq!(buf, [0u8; 64], "data was delivered");
        assert!(arq.channel.rx.is_empty());
        assert!(arq.channel.tx.is_empty(), "ACK was sent");
        assert!(arq.read_buf.is_empty());
        assert_eq!(arq.rn, 0);
        assert!(!arq.rx_finished);
        assert!(!arq.failed);
    }
}

#[test]
fn truncated_frame_at_eof_is_closed() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let bytes = wire_dat(0, b"hello world");
    peer.push(bytes[..9].to_vec());
    peer.respond(&mut arq, &mut off);
    arq.channel.eof = true;
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(
        drive(&mut arq, &mut op, &mut peer, &mut off, 10),
        Some(Err(ArqError::Closed))
    ));
    assert_eq!(arq.rn, 0);
}

#[test]
fn late_frame_after_eof_reacks() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(wire_dat(0, b"a"));
    peer.push(wire_fin(1, b""));
    peer.respond(&mut arq, &mut off);
    let got = read_to_end(&mut arq, &mut peer, &mut off);
    assert_eq!(got, b"a");
    let acks_before = ack_numbers(&arq.channel.tx).len();
    peer.push(wire_dat(0, b"a"));
    peer.respond(&mut arq, &mut off);
    let mut op = Op::Flush;
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 100),
        OpOut::Done
    ));
    let acks_after = ack_numbers(&arq.channel.tx).len();
    assert_eq!(acks_after, acks_before + 1);
    let mut buf = [0u8; 16];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 100),
        OpOut::Read(0)
    ));
}

#[test]
fn read_buffer_wraps_and_keeps_order() {
    let mut arq = make_arq();
    let chunk = [0xAAu8; 250];
    while arq.read_buf.push(&chunk) {}
    let rem = crate::arq::ReadBuf::<4>::CAPACITY - arq.read_buf.len();
    assert!(arq.read_buf.push(&vec![0xAA; rem]));
    assert_eq!(arq.read_buf.len(), crate::arq::ReadBuf::<4>::CAPACITY);
    assert!(!arq.read_buf.push(&[0u8; 1]));
    let mut buf = vec![0u8; 1000];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(arq.service_op(&mut op), Some(OpOut::Read(1000))));
    assert!(arq.read_buf.push(&[0xBB; 250]));
    let mut buf = vec![0u8; 1258];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(arq.service_op(&mut op), Some(OpOut::Read(1258))));
    assert!(buf[..1008].iter().all(|&b| b == 0xAA));
    assert!(buf[1008..].iter().all(|&b| b == 0xBB));
    let mut buf = [0u8; 250];
    let mut op = Op::Read { buf: &mut buf };
    assert!(arq.service_op(&mut op).is_none());
    assert!(arq.read_buf.push(&[0xCC; 250]));
    assert_eq!(arq.read_buf.len(), 250);
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(arq.service_op(&mut op), Some(OpOut::Read(250))));
    assert!(buf.iter().all(|&b| b == 0xCC));
}

/// The ACK numbers among the frames in `tx`.
fn ack_numbers(tx: &[u8]) -> Vec<u16> {
    parse_stream(tx)
        .iter()
        .filter_map(|f| match f {
            Frame::Ack(a) => Some(a.an()),
            _ => None,
        })
        .collect()
}

fn sent_acks(arq: &TestArq) -> Vec<u16> {
    ack_numbers(&arq.channel.tx)
}

fn sent_data(arq: &TestArq) -> Vec<Frame> {
    parse_stream(&arq.channel.tx)
        .into_iter()
        .filter(|f| !matches!(f, Frame::Ack(_)))
        .collect()
}

fn sent_sns(arq: &TestArq) -> Vec<u16> {
    sent_data(arq)
        .iter()
        .map(|f| match f {
            Frame::Dat(d) | Frame::DatAckReq(d) | Frame::Fin(d) => d.sn(),
            Frame::Ack(_) => unreachable!(),
        })
        .collect()
}

fn read_some(arq: &mut TestArq, peer: &mut Peer, off: &mut usize) -> Option<Vec<u8>> {
    let mut buf = [0u8; 64];
    let mut op = Op::Read { buf: &mut buf };
    match drive(arq, &mut op, peer, off, 10)? {
        Ok(OpOut::Read(n)) => Some(buf[..n].to_vec()),
        other => panic!("unexpected read result: {other:?}"),
    }
}

#[test]
fn ack_request_is_acked_immediately() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(wire_dat_ack_req(0, b"x"));
    peer.respond(&mut arq, &mut off);
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"x");
    assert_eq!(sent_acks(&arq), [1]);
}

#[test]
fn bulk_frames_still_batch_acks() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    for sn in 0..3u16 {
        peer.push(wire_dat(sn, &[b'a' + sn as u8]));
    }
    peer.respond(&mut arq, &mut off);
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"abc");
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    assert!(sent_acks(&arq).is_empty(), "ordinary DATs must batch");
    peer.push(wire_dat(3, b"d"));
    peer.respond(&mut arq, &mut off);
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"d");
    assert_eq!(sent_acks(&arq), [4]);
}

#[test]
fn flush_requests_ack_only_on_final_frame() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    let data: Vec<u8> = (0..600u32).map(|i| i as u8).collect();
    write_until(&mut arq, &data, &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 20).is_none());
    let frames = sent_data(&arq);
    assert!(matches!(frames[0], Frame::Dat(d) if d.sn() == 0));
    assert!(matches!(frames[1], Frame::Dat(d) if d.sn() == 1));
    assert!(matches!(frames[2], Frame::DatAckReq(d) if d.sn() == 2));
    assert_eq!(frames.len(), 3);
    peer.push(wire_ack(3));
    peer.respond(&mut arq, &mut off);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 20),
        OpOut::Done
    ));
    assert_eq!(arq.state, State::Active);
}

#[test]
fn flush_marks_already_sent_final_frame() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    assert!(matches!(sent_data(&arq)[..], [Frame::Dat(d)] if d.sn() == 0));
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 50).is_none());
    let frames = sent_data(&arq);
    assert_eq!(frames.len(), 2, "final frame re-sent once: {frames:?}");
    assert!(matches!(frames[1], Frame::DatAckReq(d) if d.sn() == 0 && d.payload() == b"abcd"));
    assert!(arq.sbuf.get(0).unwrap().requests_ack());
    peer.push(wire_ack(1));
    peer.respond(&mut arq, &mut off);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 20),
        OpOut::Done
    ));
}

#[test]
fn ack_request_property_survives_retransmission() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10).is_none());
    expire(&arq);
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10).is_none());
    let frames = sent_data(&arq);
    assert_eq!(frames.len(), 2);
    assert!(
        frames
            .iter()
            .all(|f| matches!(f, Frame::DatAckReq(d) if d.sn() == 0))
    );
}

#[test]
fn duplicate_after_lost_ack_is_reacked() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(wire_dat_ack_req(0, b"a"));
    peer.respond(&mut arq, &mut off);
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"a");
    assert_eq!(sent_acks(&arq), [1]);
    peer.push(wire_dat(0, b"a"));
    peer.respond(&mut arq, &mut off);
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    assert_eq!(sent_acks(&arq), [1, 1], "duplicate must be re-ACKed");
    assert_eq!(arq.rn, 1);
    assert!(arq.read_buf.is_empty());
    peer.push(wire_fin(1, b"b"));
    peer.respond(&mut arq, &mut off);
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"b");
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"");
}

#[test]
fn duplicate_buffered_out_of_order_is_reacked() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(wire_dat(1, b"b"));
    peer.respond(&mut arq, &mut off);
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    assert!(sent_acks(&arq).is_empty());
    peer.push(wire_dat(1, b"b"));
    peer.respond(&mut arq, &mut off);
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    assert_eq!(sent_acks(&arq), [0]);
    assert_eq!(arq.rn, 0);
    peer.push(wire_dat(0, b"a"));
    peer.respond(&mut arq, &mut off);
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"ab");
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    assert_eq!(sent_acks(&arq), [0, 2]);
    assert_eq!(arq.rn, 2);
}

#[test]
fn no_retransmit_before_deadline() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 50).is_none());
    assert_eq!(sent_data(&arq).len(), 1);
    arq.timer.clock.advance(arq.rto - Duration::from_millis(1));
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 50).is_none());
    assert_eq!(sent_data(&arq).len(), 1, "retransmitted before deadline");
    arq.timer.clock.advance(Duration::from_millis(1));
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 50).is_none());
    assert_eq!(sent_data(&arq).len(), 2, "no retransmit after deadline");
}

#[test]
fn retransmit_backs_off_and_ack_resets() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10).is_none());
    for _ in 0..6 {
        expire(&arq);
        assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10).is_none());
    }
    let ms = Duration::from_millis;
    assert_eq!(
        arq.timer.clock.starts(),
        [
            ms(250),
            ms(500),
            ms(1000),
            ms(2000),
            ms(4000),
            ms(4000),
            ms(4000)
        ]
    );
    assert_eq!(sent_data(&arq).len(), 7);
    peer.push(wire_ack(1));
    peer.respond(&mut arq, &mut off);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 10),
        OpOut::Done
    ));
    assert_eq!(arq.rto, ms(250));
    assert!(!arq.timer_running);
}

#[test]
fn delayed_peer_writes_do_not_scale_with_polls() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, &[7u8; 1000], &mut peer, &mut off);
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    let first = arq.channel.tx.len();
    assert_eq!(sent_sns(&arq), [0, 1, 2, 3]);
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10_000).is_none());
    assert_eq!(arq.channel.tx.len(), first, "polls alone must not send");
    expire(&arq);
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10_000).is_none());
    assert_eq!(arq.channel.tx.len(), 2 * first, "one round per timeout");
    peer.silent = false;
    peer.seen = vec![0, 1, 2, 3];
    peer.next = 4;
    expire(&arq);
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 100).is_none());
    assert_eq!(arq.w, 0, "lost ACKs recovered via re-ACKed duplicates");
    assert!(!arq.timer_running);
}

#[test]
fn ack_during_retransmit_round_skips_acked_frames() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, &[7u8; 1000], &mut peer, &mut off);
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    expire(&arq);
    let mut cx = noop_cx();
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(arq.poll_op(&mut cx, &mut op).is_pending());
    assert_eq!(sent_sns(&arq), [0, 1, 2, 3, 0]);
    arq.channel.rx.extend(wire_ack(2));
    for _ in 0..10 {
        assert!(arq.poll_op(&mut cx, &mut op).is_pending());
    }
    assert_eq!(sent_sns(&arq), [0, 1, 2, 3, 0, 2, 3]);
    assert!(arq.timer_running);
}

#[test]
fn lost_fin_is_retransmitted() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    let mut op = Op::Shutdown;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 20).is_none());
    assert!(matches!(sent_data(&arq)[..], [Frame::Fin(d)] if d.sn() == 0));
    peer.silent = false;
    expire(&arq);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 20),
        OpOut::Done
    ));
    assert_eq!(sent_sns(&arq), [0, 0]);
}

#[test]
fn channel_eof_is_closed() {
    let mut arq = new_arq(MockLink {
        eof: true,
        ..MockLink::new()
    });
    let mut cx = noop_cx();
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Err(ArqError::Closed))
    ));
}

struct FailLink {
    fail_recv: bool,
    fail_send: bool,
    send_zero: bool,
}

impl Transport for FailLink {
    type Error = String;

    fn poll_write(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, String>> {
        if self.fail_send {
            Poll::Ready(Err("send failed".into()))
        } else if self.send_zero {
            Poll::Ready(Ok(0))
        } else {
            Poll::Ready(Ok(buf.len()))
        }
    }

    fn poll_read(&mut self, _cx: &mut Context<'_>, _buf: &mut [u8]) -> Poll<Result<usize, String>> {
        if self.fail_recv {
            Poll::Ready(Err("recv failed".into()))
        } else {
            Poll::Pending
        }
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), String>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn channel_recv_error_propagates() {
    let mut arq = new_arq(FailLink {
        fail_recv: true,
        fail_send: false,
        send_zero: false,
    });
    let mut cx = noop_cx();
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Err(ArqError::Io(e))) if e == "recv failed"
    ));
}

#[test]
fn channel_send_error_propagates() {
    let mut arq = new_arq(FailLink {
        fail_recv: false,
        fail_send: true,
        send_zero: false,
    });
    let mut cx = noop_cx();
    let data = [7u8; 10];
    let mut op = Op::Write { buf: &data };
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Ok(OpOut::Write(10)))
    ));
    let mut op = Op::Flush;
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Err(ArqError::Io(e))) if e == "send failed"
    ));
}

#[test]
fn channel_send_eof_is_closed() {
    let mut arq = new_arq(FailLink {
        fail_recv: false,
        fail_send: false,
        send_zero: true,
    });
    let mut cx = noop_cx();
    let data = [9u8; 10];
    let mut op = Op::Write { buf: &data };
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Ok(OpOut::Write(10)))
    ));
    let mut op = Op::Flush;
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Err(ArqError::Closed))
    ));
    assert!(arq.failed);
}

/// A link whose write side holds at most `cap` bytes; a frame that does not
/// fit whole is not accepted at all.
struct CapLink {
    rx: FrameQueue,
    tx: Vec<u8>,
    cap: usize,
}

impl CapLink {
    fn new(cap: usize) -> Self {
        Self {
            rx: FrameQueue::default(),
            tx: Vec::new(),
            cap,
        }
    }
}

impl Transport for CapLink {
    type Error = Infallible;

    fn poll_write(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        if self.tx.len() + buf.len() > self.cap {
            return Poll::Pending;
        }
        self.tx.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_read(
        &mut self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        match self.rx.0.pop_front() {
            Some(frame) => {
                buf[..frame.len()].copy_from_slice(&frame);
                Poll::Ready(Ok(frame.len()))
            }
            None => Poll::Pending,
        }
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
}

struct PartialPeer {
    rest: Vec<u8>,
    next_rx: u16,
    next_tx: u16,
    to_us: Vec<u8>,
}

impl PartialPeer {
    fn new() -> Self {
        Self {
            rest: Vec::new(),
            next_rx: 0,
            next_tx: 0,
            to_us: Vec::new(),
        }
    }

    fn poll(&mut self, wire: &mut Vec<u8>) {
        self.rest.extend(core::mem::take(wire));
        while let Ok(len) = wire_len(&self.rest) {
            if self.rest.len() < len {
                break;
            }
            let frame = Frame::from_bytes::<BchAckCodec, 16, _>(&crc16(), &self.rest[..len])
                .expect("peer frame decode");
            self.rest.drain(..len);
            if let Frame::Dat(d) | Frame::DatAckReq(d) | Frame::Fin(d) = frame {
                if d.sn() == self.next_rx {
                    self.to_us.extend(wire_ack(self.next_rx.wrapping_add(1)));
                    self.to_us.extend(wire_dat(self.next_tx, d.payload()));
                    self.next_tx = self.next_tx.wrapping_add(1);
                    self.next_rx = d.sn().wrapping_add(1);
                }
            }
        }
    }

    fn drain(&mut self) -> Vec<u8> {
        core::mem::take(&mut self.to_us)
    }
}

#[test]
fn blocked_write_resumes_across_polls() {
    let mut arq = new_arq(CapLink::new(MAX_FRAME));
    let mut peer = PartialPeer::new();
    let mut cx = noop_cx();
    let data: Vec<u8> = (0..600u32).map(|i| (i % 251) as u8).collect();
    let mut off = 0usize;
    for _ in 0..10_000 {
        if off == data.len() {
            break;
        }
        let mut op = Op::Write { buf: &data[off..] };
        match arq.poll_op(&mut cx, &mut op) {
            Poll::Ready(Ok(OpOut::Write(n))) => off += n,
            Poll::Ready(Ok(other)) => panic!("unexpected op out: {other:?}"),
            Poll::Ready(Err(e)) => panic!("arq error: {e:?}"),
            Poll::Pending => {
                peer.poll(&mut arq.channel.tx);
                arq.channel.rx.extend(peer.drain());
            }
        }
    }
    assert_eq!(off, data.len(), "write did not complete");
    let mut op = Op::Flush;
    let mut flushed = false;
    for _ in 0..10_000 {
        match arq.poll_op(&mut cx, &mut op) {
            Poll::Ready(Ok(OpOut::Done)) => {
                flushed = true;
                break;
            }
            Poll::Ready(Ok(other)) => panic!("unexpected op out: {other:?}"),
            Poll::Ready(Err(e)) => panic!("arq error: {e:?}"),
            Poll::Pending => {
                peer.poll(&mut arq.channel.tx);
                arq.channel.rx.extend(peer.drain());
            }
        }
    }
    assert!(flushed, "flush did not complete");
    let mut got = Vec::new();
    for _ in 0..10_000 {
        if got.len() == data.len() {
            break;
        }
        let mut buf = [0u8; 64];
        let mut op = Op::Read { buf: &mut buf };
        match arq.poll_op(&mut cx, &mut op) {
            Poll::Ready(Ok(OpOut::Read(n))) => got.extend_from_slice(&buf[..n]),
            Poll::Ready(Ok(other)) => panic!("unexpected op out: {other:?}"),
            Poll::Ready(Err(e)) => panic!("arq error: {e:?}"),
            Poll::Pending => {}
        }
    }
    assert_eq!(got, data);
}

fn read_all(arq: &mut TestArq, size: usize) -> (Vec<u8>, Result<(), ArqError<Infallible>>) {
    let mut peer = Peer::new();
    let mut off = 0usize;
    let mut got = Vec::new();
    for _ in 0..100 {
        let mut buf = vec![0u8; size];
        let mut op = Op::Read { buf: &mut buf };
        match drive(arq, &mut op, &mut peer, &mut off, 20) {
            Some(Ok(OpOut::Read(0))) => return (got, Ok(())),
            Some(Ok(OpOut::Read(n))) => got.extend_from_slice(&buf[..n]),
            Some(Ok(other)) => panic!("unexpected read result: {other:?}"),
            Some(Err(e)) => return (got, Err(e)),
            None => panic!("read made no progress"),
        }
    }
    panic!("read did not terminate");
}

#[test]
fn lower_eof_returns_accepted_payload_then_the_expected_end() {
    let mut dat_then_partial = wire_dat(0, b"hello");
    dat_then_partial.extend_from_slice(&wire_dat(1, b"world")[..9]);
    let mut dat_fin = wire_dat(0, b"hello");
    dat_fin.extend(wire_fin(1, b" world"));
    // (label, lower bytes, read sizes, payload, ends with application EOF)
    let cases = [
        (
            "DAT then FIN",
            dat_fin,
            &[1, 3, 16][..],
            &b"hello world"[..],
            true,
        ),
        (
            "FIN with payload",
            wire_fin(0, b"bye"),
            &[2][..],
            &b"bye"[..],
            true,
        ),
        (
            "DAT without FIN",
            wire_dat(0, b"hello"),
            &[1, 16][..],
            &b"hello"[..],
            false,
        ),
        (
            "DAT then partial frame",
            dat_then_partial,
            &[16][..],
            &b"hello"[..],
            false,
        ),
    ];
    for (label, lower, sizes, payload, app_eof) in cases {
        for &size in sizes {
            let mut arq = make_arq();
            arq.channel.rx.extend(lower.clone());
            arq.channel.eof = true;
            let (got, end) = read_all(&mut arq, size);
            assert_eq!(got, payload, "{label}, read size {size}");
            if app_eof {
                assert!(end.is_ok(), "{label}, read size {size}: {end:?}");
            } else {
                assert!(
                    matches!(end, Err(ArqError::Closed)),
                    "{label}, read size {size}: {end:?}"
                );
            }
        }
    }
}

#[test]
fn lower_eof_keeps_writes_going_but_flush_fails_without_possible_ack() {
    let mut arq = make_arq();
    arq.channel.eof = true;
    let mut cx = noop_cx();
    let mut op = Op::Write { buf: b"abc" };
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Ok(OpOut::Write(3)))
    ));
    for _ in 0..3 {
        match arq.poll_op(&mut cx, &mut Op::Flush) {
            Poll::Ready(Err(ArqError::Closed)) => {
                assert_eq!(
                    parse_stream(&arq.channel.tx).len(),
                    1,
                    "the data frame must still be sent after lower EOF"
                );
                return;
            }
            Poll::Ready(other) => panic!("unexpected flush result: {other:?}"),
            Poll::Pending => {}
        }
    }
    panic!("flush waits forever for an ACK that cannot arrive");
}

#[test]
fn shutdown_fails_once_lower_eof_makes_the_ack_impossible() {
    let mut arq = make_arq();
    arq.channel.eof = true;
    let mut cx = noop_cx();
    for _ in 0..3 {
        match arq.poll_op(&mut cx, &mut Op::Shutdown) {
            Poll::Ready(Err(ArqError::Closed)) => return,
            Poll::Ready(other) => panic!("unexpected shutdown result: {other:?}"),
            Poll::Pending => {}
        }
    }
    panic!("shutdown waits forever for an ACK that cannot arrive");
}

/// Polls `op` once with the peer's frames already waiting on the link.
fn poll_with_waiting(
    arq: &mut TestArq,
    rx: Vec<u8>,
    op: &mut Op,
) -> Poll<Result<OpOut, ArqError<Infallible>>> {
    arq.channel.rx.extend(rx);
    arq.poll_op(&mut noop_cx(), op)
}

#[test]
fn dat_and_ack_received_together_complete_without_retransmission() {
    for (label, flush) in [("flush", true), ("shutdown", false)] {
        let op = || if flush { Op::Flush } else { Op::Shutdown };
        let clock = Clock::default();
        let mut arq: TestArq =
            ArqLayer::<4, Crc16X25, BchAckCodec>::new().build(MockLink::new(), clock.timer());
        let mut cx = noop_cx();
        if flush {
            let mut write = Op::Write { buf: b"abc" };
            assert!(matches!(
                arq.poll_op(&mut cx, &mut write),
                Poll::Ready(Ok(OpOut::Write(3)))
            ));
        }
        assert!(arq.poll_op(&mut cx, &mut op()).is_pending(), "{label}");
        let mut both = wire_dat(0, b"peer");
        both.extend(wire_ack(1));
        let res = poll_with_waiting(&mut arq, both, &mut op());
        assert!(
            matches!(res, Poll::Ready(Ok(OpOut::Done))),
            "{label}: {res:?}"
        );
        assert_eq!(clock.0.borrow().now, Duration::ZERO, "{label}");
        assert_eq!(arq.retx, 0, "{label}");
    }
}

fn reach_done(arq: &mut TestArq) {
    let mut cx = noop_cx();
    arq.channel.rx.extend(wire_fin(0, b"x"));
    let mut buf = [0u8; 8];
    assert!(matches!(
        arq.poll_op(&mut cx, &mut Op::Read { buf: &mut buf }),
        Poll::Ready(Ok(OpOut::Read(1)))
    ));
    assert!(arq.poll_op(&mut cx, &mut Op::Shutdown).is_pending());
    arq.channel.rx.extend(wire_ack(1));
    assert!(matches!(
        arq.poll_op(&mut cx, &mut Op::Shutdown),
        Poll::Ready(Ok(OpOut::Done))
    ));
    assert!(matches!(
        arq.poll_op(&mut cx, &mut Op::Read { buf: &mut buf }),
        Poll::Ready(Ok(OpOut::Read(0)))
    ));
    assert_eq!(arq.state, State::Done);
}

#[test]
fn duplicate_fin_after_read_eof_is_reacked() {
    let mut arq = make_arq();
    let mut cx = noop_cx();
    arq.channel.rx.extend(wire_fin(0, b"bye"));
    let mut buf = [0u8; 16];
    assert!(matches!(
        arq.poll_op(&mut cx, &mut Op::Read { buf: &mut buf }),
        Poll::Ready(Ok(OpOut::Read(3)))
    ));
    assert_eq!(sent_acks(&arq), [1]);
    arq.channel.tx.clear();
    assert!(matches!(
        arq.poll_op(&mut cx, &mut Op::Read { buf: &mut buf }),
        Poll::Ready(Ok(OpOut::Read(0)))
    ));
    assert_eq!(arq.state, State::RecvDone);

    arq.channel.rx.extend(wire_fin(0, b"bye"));
    for _ in 0..3 {
        assert!(matches!(
            arq.poll_op(&mut cx, &mut Op::Read { buf: &mut buf }),
            Poll::Ready(Ok(OpOut::Read(0)))
        ));
    }
    assert_eq!(sent_acks(&arq), [1], "duplicate FIN re-acknowledged once");
    assert_eq!(arq.rn, 1);
    assert!(arq.read_buf.is_empty());
}

#[test]
fn completed_endpoint_reacks_duplicate_fin_without_reopening() {
    let mut arq = make_arq();
    let mut cx = noop_cx();
    reach_done(&mut arq);
    arq.channel.tx.clear();
    arq.channel.rx.extend(wire_fin(0, b"x"));
    let mut buf = [0u8; 8];
    assert!(matches!(
        arq.poll_op(&mut cx, &mut Op::Read { buf: &mut buf }),
        Poll::Ready(Ok(OpOut::Read(0)))
    ));
    assert_eq!(sent_acks(&arq), [1]);
    assert!(sent_data(&arq).is_empty());
    assert_eq!(arq.rn, 1);
    assert!(arq.read_buf.is_empty());
    assert_eq!(arq.state, State::Done);
    assert!(matches!(
        arq.poll_op(&mut cx, &mut Op::Write { buf: b"no" }),
        Poll::Ready(Err(ArqError::Closed))
    ));
}

#[test]
fn repeated_completion_operations_are_inert() {
    let mut arq = make_arq();
    let mut cx = noop_cx();
    reach_done(&mut arq);
    let sent = arq.channel.tx.len();
    let mut buf = [0u8; 8];
    for _ in 0..3 {
        assert!(matches!(
            arq.poll_op(&mut cx, &mut Op::Read { buf: &mut buf }),
            Poll::Ready(Ok(OpOut::Read(0)))
        ));
        assert!(matches!(
            arq.poll_op(&mut cx, &mut Op::Flush),
            Poll::Ready(Ok(OpOut::Done))
        ));
        assert!(matches!(
            arq.poll_op(&mut cx, &mut Op::Shutdown),
            Poll::Ready(Ok(OpOut::Done))
        ));
    }
    assert_eq!(arq.channel.tx.len(), sent, "nothing new transmitted");
    assert_eq!(sent_sns(&arq), [0], "a single FIN");
    assert_eq!(arq.state, State::Done);
}
