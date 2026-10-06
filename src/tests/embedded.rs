use core::future::Future;
use core::pin::{Pin, pin};
use std::collections::VecDeque;

use embedded_io_async::{ErrorType, Read, Write};

use super::*;
use crate::embedded_io::{EiaPoll, PollTransport};

#[derive(Default)]
struct Script {
    rx: VecDeque<u8>,
    tx: Vec<u8>,
    /// Bytes of `tx` covered by a completed flush.
    flushed: usize,
    write_chunk: usize,
    read_chunk: usize,
    write_polls: usize,
    read_polls: usize,
    flush_polls: usize,
    write_starts: usize,
    write_done: usize,
    read_starts: usize,
    read_done: usize,
    flush_starts: usize,
    flush_done: usize,
}

/// A transport that keeps each operation in flight across polls: every read and
/// write needs two polls, every flush three, and transfers are cut to chunks.
#[derive(Clone)]
struct Multi(Rc<RefCell<Script>>);

fn multi(write_chunk: usize, read_chunk: usize) -> Multi {
    Multi(Rc::new(RefCell::new(Script {
        write_chunk,
        read_chunk,
        ..Script::default()
    })))
}

impl ErrorType for Multi {
    type Error = Infallible;
}

impl PollTransport for Multi {
    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        let mut s = self.0.borrow_mut();
        if s.rx.is_empty() {
            return Poll::Pending;
        }
        if s.read_polls == 0 {
            s.read_starts += 1;
        }
        s.read_polls += 1;
        if s.read_polls < 2 {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        s.read_polls = 0;
        s.read_done += 1;
        let n = s.rx.len().min(buf.len()).min(s.read_chunk);
        for (dst, src) in buf.iter_mut().zip(s.rx.drain(..n)) {
            *dst = src;
        }
        Poll::Ready(Ok(n))
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        let mut s = self.0.borrow_mut();
        if s.write_polls == 0 {
            s.write_starts += 1;
        }
        s.write_polls += 1;
        if s.write_polls < 2 {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        s.write_polls = 0;
        s.write_done += 1;
        let n = buf.len().min(s.write_chunk);
        s.tx.extend_from_slice(&buf[..n]);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        let mut s = self.0.borrow_mut();
        if s.flush_polls == 0 {
            s.flush_starts += 1;
        }
        s.flush_polls += 1;
        if s.flush_polls < 3 {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        s.flush_polls = 0;
        s.flush_done += 1;
        s.flushed = s.tx.len();
        Poll::Ready(Ok(()))
    }
}

type EArq = Arq<4, 16, { r::<4>() }, EiaPoll<Multi>, Crc16X25, BchAckCodec, ManualTimer>;

fn earq(link: &Multi) -> EArq {
    new_arq(EiaPoll(link.clone()))
}

fn poll_once<F: Future>(fut: &mut Pin<&mut F>) -> Poll<F::Output> {
    fut.as_mut().poll(&mut noop_cx())
}

fn run<F: Future>(fut: F, max_polls: usize) -> F::Output {
    let mut fut = pin!(fut);
    for _ in 0..max_polls {
        if let Poll::Ready(out) = poll_once(&mut fut) {
            return out;
        }
    }
    panic!("no completion within {max_polls} polls");
}

/// The operations were never abandoned and restarted.
fn assert_no_restarts(link: &Multi) {
    let s = link.0.borrow();
    assert_eq!(s.write_starts, s.write_done, "write restarted");
    assert_eq!(s.read_starts, s.read_done, "read restarted");
    assert_eq!(s.flush_starts, s.flush_done, "flush restarted");
}

#[test]
fn write_and_flush_needing_many_polls_complete_without_restarts() {
    let link = multi(5, 64);
    let mut arq = earq(&link);
    assert_eq!(run(arq.write(b"abc"), 4).unwrap(), 3);
    let mut flush = pin!(arq.flush());
    for polls in 0..200 {
        {
            let mut s = link.0.borrow_mut();
            if s.flushed >= 8 && s.rx.is_empty() && s.tx.len() == 8 {
                s.rx.extend(wire_ack(1));
            }
        }
        if let Poll::Ready(res) = poll_once(&mut flush) {
            res.unwrap();
            let s = link.0.borrow();
            let frames = parse_stream(&s.tx);
            assert!(matches!(&frames[..], [Frame::DatAckReq(d)] if d.payload() == b"abc"));
            assert_eq!(s.write_done, 2, "8 bytes in 5-byte chunks, once each");
            drop(s);
            assert_no_restarts(&link);
            return;
        }
        assert!(polls < 199, "flush did not complete");
    }
}

#[test]
fn progress_survives_a_change_of_driving_operation() {
    let link = multi(1024, 64);
    let mut arq = earq(&link);
    assert_eq!(run(arq.write(b"abc"), 4).unwrap(), 3);
    {
        let mut flush = pin!(arq.flush());
        assert!(poll_once(&mut flush).is_pending());
    }
    assert_eq!(link.0.borrow().write_starts, 1);
    assert!(link.0.borrow().tx.is_empty(), "write completed too early");
    let mut buf = [0u8; 4];
    let mut read = pin!(arq.read(&mut buf));
    for _ in 0..10 {
        assert!(poll_once(&mut read).is_pending());
    }
    let s = link.0.borrow();
    assert_eq!(s.write_starts, 1, "the in-flight write was restarted");
    assert_eq!(parse_stream(&s.tx).len(), 1);
    assert_eq!(s.flush_starts, s.flush_done, "flush restarted");
}

#[test]
fn partial_reads_are_reassembled_into_frames() {
    let link = multi(1024, 3);
    link.0.borrow_mut().rx.extend(wire_dat(0, b"hello world"));
    let mut arq = earq(&link);
    let mut got = Vec::new();
    while got.len() < 11 {
        let mut buf = [0u8; 64];
        let n = run(arq.read(&mut buf), 50).unwrap();
        assert!(n > 0);
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got, b"hello world");
    assert_no_restarts(&link);
}
