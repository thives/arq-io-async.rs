use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Wake;

use super::*;

pub(super) struct Counter(AtomicUsize);

impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) fn counter() -> (Arc<Counter>, Waker) {
    let c = Arc::new(Counter(AtomicUsize::new(0)));
    (c.clone(), Waker::from(c))
}

pub(super) fn woken(c: &Arc<Counter>) -> usize {
    c.0.load(Ordering::SeqCst)
}

#[derive(Default)]
struct State {
    rx: VecDeque<Vec<u8>>,
    rx_waker: Option<Waker>,
    tx_blocked: bool,
    tx_waker: Option<Waker>,
    flush_blocked: bool,
    flush_waker: Option<Waker>,
    /// Every frame accepted by the lower write, concatenated.
    sent: Vec<u8>,
    /// Number of lower write calls that were attempted.
    writes: usize,
}

/// A lower link with one waker slot per resource, like a real transport.
#[derive(Clone, Default)]
struct WLink(Rc<RefCell<State>>);

impl WLink {
    fn push_rx(&self, frame: Vec<u8>) {
        let mut st = self.0.borrow_mut();
        st.rx.push_back(frame);
        let w = st.rx_waker.take();
        drop(st);
        if let Some(w) = w {
            w.wake();
        }
    }

    fn release_tx(&self) {
        let mut st = self.0.borrow_mut();
        st.tx_blocked = false;
        let w = st.tx_waker.take();
        drop(st);
        if let Some(w) = w {
            w.wake();
        }
    }

    fn release_flush(&self) {
        let mut st = self.0.borrow_mut();
        st.flush_blocked = false;
        let w = st.flush_waker.take();
        drop(st);
        if let Some(w) = w {
            w.wake();
        }
    }
}

impl Transport for WLink {
    type Error = Infallible;

    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        let mut st = self.0.borrow_mut();
        st.writes += 1;
        if st.tx_blocked {
            st.tx_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        st.sent.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        let mut st = self.0.borrow_mut();
        let Some(frame) = st.rx.pop_front() else {
            st.rx_waker = Some(cx.waker().clone());
            return Poll::Pending;
        };
        buf[..frame.len()].copy_from_slice(&frame);
        Poll::Ready(Ok(frame.len()))
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        let mut st = self.0.borrow_mut();
        if st.flush_blocked {
            st.flush_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }
}

type WArq = Arq<4, 16, WLink, Crc16X25, BchAckCodec, ManualTimer>;

fn setup() -> (WArq, WLink, Clock) {
    let link = WLink::default();
    let clock = Clock::default();
    let arq = ArqLayer::<4, Crc16X25, BchAckCodec>::new().build(link.clone(), clock.timer());
    (arq, link, clock)
}

fn read(arq: &mut WArq, w: &Waker) -> Poll<Vec<u8>> {
    let mut storage = [0u8; 32];
    match arq.poll_read(&mut Context::from_waker(w), &mut storage) {
        Poll::Ready(res) => Poll::Ready(storage[..res.unwrap()].to_vec()),
        Poll::Pending => Poll::Pending,
    }
}

fn write(arq: &mut WArq, w: &Waker, data: &[u8]) -> Poll<usize> {
    arq.poll_write(&mut Context::from_waker(w), data)
        .map(|res| res.unwrap())
}

fn flush(arq: &mut WArq, w: &Waker) -> Poll<()> {
    arq.poll_flush(&mut Context::from_waker(w))
        .map(|res| res.unwrap())
}

fn shutdown(arq: &mut WArq, w: &Waker) -> Poll<()> {
    arq.poll_close(&mut Context::from_waker(w))
        .map(|res| res.unwrap())
}

/// The ACK numbers among the bytes accepted by the lower write.
fn sent_acks(link: &WLink) -> Vec<u16> {
    ack_numbers(&link.0.borrow().sent)
}

/// Completes a local shutdown that the peer acknowledged.
fn shutdown_acked(arq: &mut WArq, link: &WLink, w: &Waker) {
    assert!(shutdown(arq, w).is_pending());
    link.push_rx(wire_ack(1));
    assert!(shutdown(arq, w).is_ready());
}

#[test]
fn peer_data_wakes_the_pending_reader() {
    let (mut arq, link, _clock) = setup();
    let (c, w) = counter();
    assert!(read(&mut arq, &w).is_pending());
    assert_eq!(write(&mut arq, &w, b"hi"), Poll::Ready(2));
    link.push_rx(wire_dat_ack_req(0, b"yo"));
    assert!(woken(&c) >= 1, "reader was not woken by peer data");
    assert_eq!(read(&mut arq, &w), Poll::Ready(b"yo".to_vec()));
}

#[test]
fn latest_waker_is_the_one_woken() {
    let (mut arq, link, _clock) = setup();
    let (c1, w1) = counter();
    let (c2, w2) = counter();
    assert!(read(&mut arq, &w1).is_pending());
    assert!(read(&mut arq, &w2).is_pending());
    link.push_rx(wire_dat_ack_req(0, b"z"));
    assert!(woken(&c2) >= 1, "latest waker must be woken");
    assert_eq!(woken(&c1), 0);
}

#[test]
fn blocked_lower_write_wakes_the_task_on_release() {
    let (mut arq, link, _clock) = setup();
    link.0.borrow_mut().tx_blocked = true;
    let (c, w) = counter();
    assert_eq!(write(&mut arq, &w, b"x"), Poll::Ready(1));
    assert!(flush(&mut arq, &w).is_pending());
    assert!(read(&mut arq, &w).is_pending());
    link.release_tx();
    assert!(woken(&c) >= 1, "task was not woken by the lower write");
}

#[test]
fn pending_lower_flush_wakes_the_task_on_release() {
    let (mut arq, link, _clock) = setup();
    link.0.borrow_mut().flush_blocked = true;
    let (c, w) = counter();
    assert_eq!(write(&mut arq, &w, b"x"), Poll::Ready(1));
    assert!(flush(&mut arq, &w).is_pending());
    assert!(read(&mut arq, &w).is_pending());
    link.release_flush();
    assert!(woken(&c) >= 1);
}

#[test]
fn timer_expiry_wakes_the_task() {
    let (mut arq, _link, clock) = setup();
    let (c, w) = counter();
    assert_eq!(write(&mut arq, &w, b"x"), Poll::Ready(1));
    assert!(flush(&mut arq, &w).is_pending());
    let before = woken(&c);
    clock.advance(arq.rto);
    assert!(woken(&c) > before, "timer did not wake the task");
}

#[test]
fn idle_link_causes_no_wake_loop() {
    let (mut arq, _link, _clock) = setup();
    let (c, w) = counter();
    for _ in 0..50 {
        assert!(read(&mut arq, &w).is_pending());
        assert!(flush(&mut arq, &w).is_ready());
    }
    assert_eq!(woken(&c), 0);
}

#[test]
fn stalled_lower_write_does_not_hide_buffered_data() {
    let (mut arq, link, _clock) = setup();
    link.0.borrow_mut().tx_blocked = true;
    let (_c, w) = counter();
    link.push_rx(wire_dat(0, b"abc"));
    assert_eq!(read(&mut arq, &w), Poll::Ready(b"abc".to_vec()));
    assert!(read(&mut arq, &w).is_pending());
    assert!(sent_acks(&link).is_empty());
}

#[test]
fn peer_fin_waits_for_ack_while_lower_write_is_blocked() {
    let (mut arq, link, _clock) = setup();
    link.0.borrow_mut().tx_blocked = true;
    let (c, w) = counter();
    link.push_rx(wire_fin(0, b""));
    for _ in 0..2 {
        assert!(
            read(&mut arq, &w).is_pending(),
            "EOF before the ACK was sent"
        );
    }
    assert!(sent_acks(&link).is_empty());
    let before = woken(&c);
    link.release_tx();
    assert!(
        woken(&c) > before,
        "reader was not woken by the lower write"
    );
    assert_eq!(read(&mut arq, &w), Poll::Ready(Vec::new()));
    assert_eq!(sent_acks(&link), [1]);
    assert_eq!(read(&mut arq, &w), Poll::Ready(Vec::new()));
    assert_eq!(sent_acks(&link), [1]);
}

#[test]
fn peer_fin_after_local_shutdown_waits_for_ack_while_lower_write_is_blocked() {
    let (mut arq, link, _clock) = setup();
    let (c, w) = counter();
    shutdown_acked(&mut arq, &link, &w);
    link.0.borrow_mut().tx_blocked = true;
    link.push_rx(wire_fin(0, b""));
    assert!(
        read(&mut arq, &w).is_pending(),
        "EOF before the ACK was sent"
    );
    assert!(
        flush(&mut arq, &w).is_pending(),
        "flush succeeded with an unsent ACK"
    );
    assert!(sent_acks(&link).is_empty());
    let before = woken(&c);
    link.release_tx();
    assert!(woken(&c) > before, "task was not woken by the lower write");
    assert!(flush(&mut arq, &w).is_ready());
    assert_eq!(sent_acks(&link), [1]);
    assert_eq!(read(&mut arq, &w), Poll::Ready(Vec::new()));
    assert!(flush(&mut arq, &w).is_ready());
    assert!(shutdown(&mut arq, &w).is_ready());
    assert_eq!(sent_acks(&link), [1]);
}

#[test]
fn shutdown_completing_with_peer_fin_waits_for_ack() {
    let (mut arq, link, _clock) = setup();
    let (c, w) = counter();
    assert!(shutdown(&mut arq, &w).is_pending());
    link.0.borrow_mut().tx_blocked = true;
    link.push_rx(wire_ack(1));
    link.push_rx(wire_fin(0, b""));
    assert!(
        shutdown(&mut arq, &w).is_pending(),
        "shutdown succeeded with an unsent ACK"
    );
    assert!(sent_acks(&link).is_empty());
    let before = woken(&c);
    link.release_tx();
    assert!(woken(&c) > before, "task was not woken by the lower write");
    assert!(shutdown(&mut arq, &w).is_ready());
    assert_eq!(sent_acks(&link), [1]);
}

#[test]
fn peer_fin_waits_for_ack_while_lower_flush_is_blocked() {
    let (mut arq, link, _clock) = setup();
    link.0.borrow_mut().flush_blocked = true;
    let (c, w) = counter();
    link.push_rx(wire_fin(0, b""));
    assert!(
        read(&mut arq, &w).is_pending(),
        "EOF before the ACK was flushed"
    );
    assert_eq!(
        sent_acks(&link),
        [1],
        "the ACK should be written, not flushed"
    );
    assert!(
        read(&mut arq, &w).is_pending(),
        "EOF before the ACK was flushed"
    );
    assert_eq!(sent_acks(&link), [1], "the ACK must not be written twice");
    let before = woken(&c);
    link.release_flush();
    assert!(
        woken(&c) > before,
        "reader was not woken by the lower flush"
    );
    assert_eq!(read(&mut arq, &w), Poll::Ready(Vec::new()));
    assert_eq!(sent_acks(&link), [1]);
}

/// `n` valid ACK frames that acknowledge nothing.
fn stale_acks(link: &WLink, n: usize) {
    for _ in 0..n {
        link.push_rx(wire_ack(0));
    }
}

/// Polls `poll_once` until it is ready, requiring a continuation wake for
/// every pending result and more than one poll in total.
fn poll_with_continuation_wakes<T>(
    c: &Arc<Counter>,
    max_polls: usize,
    mut poll_once: impl FnMut() -> Poll<T>,
) -> T {
    let mut polls = 0;
    let out = loop {
        let before = woken(c);
        polls += 1;
        match poll_once() {
            Poll::Ready(out) => break out,
            Poll::Pending => assert!(woken(c) > before, "no continuation wake on poll {polls}"),
        }
        assert!(polls < max_polls, "input never drained");
    };
    assert!(polls > 1, "one poll processed unbounded input");
    out
}

#[test]
fn read_with_more_input_than_the_budget_yields_with_a_continuation_wake() {
    let (mut arq, link, _clock) = setup();
    let (c, w) = counter();
    stale_acks(&link, 100);
    link.push_rx(wire_dat_ack_req(0, b"end"));
    let got = poll_with_continuation_wakes(&c, 20, || read(&mut arq, &w));
    assert_eq!(got, b"end");
}

#[test]
fn flush_with_more_input_than_the_budget_yields_with_a_continuation_wake() {
    let (mut arq, link, _clock) = setup();
    let (c, w) = counter();
    assert_eq!(write(&mut arq, &w, b"x"), Poll::Ready(1));
    assert!(flush(&mut arq, &w).is_pending());
    stale_acks(&link, 2000);
    link.push_rx(wire_ack(1));
    poll_with_continuation_wakes(&c, 80, || flush(&mut arq, &w));
}

#[test]
fn corrupt_input_flood_yields_with_continuation_wakes_then_delivers() {
    let (mut arq, link, _clock) = setup();
    let (c, w) = counter();
    let mut corrupt = wire_dat(0, b"zz");
    *corrupt.last_mut().unwrap() ^= 0xFF;
    for _ in 0..2000 {
        link.push_rx(corrupt.clone());
    }
    link.push_rx(wire_dat_ack_req(0, b"ok"));
    let got = poll_with_continuation_wakes(&c, 200, || read(&mut arq, &w));
    assert_eq!(got, b"ok");
    assert!(!arq.failed);
}
