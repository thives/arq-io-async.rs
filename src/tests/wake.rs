use core::pin::Pin;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Wake;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::*;

struct Counter(AtomicUsize);

impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counter() -> (Arc<Counter>, Waker) {
    let c = Arc::new(Counter(AtomicUsize::new(0)));
    (c.clone(), Waker::from(c))
}

fn woken(c: &Arc<Counter>) -> usize {
    c.0.load(Ordering::SeqCst)
}

#[derive(Default)]
struct State {
    rx: VecDeque<u8>,
    rx_waker: Option<Waker>,
    tx_blocked: bool,
    tx_waker: Option<Waker>,
    flush_blocked: bool,
    flush_waker: Option<Waker>,
    /// Every byte accepted by the lower write, in order.
    sent: Vec<u8>,
}

/// A lower link with one waker slot per resource, like a real transport.
#[derive(Clone, Default)]
struct WLink(Rc<RefCell<State>>);

impl WLink {
    fn push_rx(&self, bytes: Vec<u8>) {
        let mut st = self.0.borrow_mut();
        st.rx.extend(bytes);
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

impl FrameIo for WLink {
    type Error = Infallible;

    fn poll_send(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        let mut st = self.0.borrow_mut();
        if st.tx_blocked {
            st.tx_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        st.sent.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        let mut st = self.0.borrow_mut();
        if st.rx.is_empty() {
            st.rx_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let n = st.rx.len().min(buf.len());
        for (dst, src) in buf.iter_mut().zip(st.rx.drain(..n)) {
            *dst = src;
        }
        Poll::Ready(Ok(n))
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

type WArq = Arq<4, 16, { r::<4>() }, WLink, Crc16X25, BchAckCodec, ManualTimer>;

fn setup() -> (WArq, WLink, Clock) {
    let link = WLink::default();
    let clock = Clock::default();
    let arq =
        ArqLayer::<4, Crc16X25, BchAckCodec>::new().build_with_timer(link.clone(), clock.timer());
    (arq, link, clock)
}

fn read<R: AsyncRead + Unpin>(r: &mut R, w: &Waker) -> Poll<Vec<u8>> {
    let mut storage = [0u8; 32];
    let mut rb = ReadBuf::new(&mut storage);
    match Pin::new(r).poll_read(&mut Context::from_waker(w), &mut rb) {
        Poll::Ready(res) => {
            res.unwrap();
            Poll::Ready(rb.filled().to_vec())
        }
        Poll::Pending => Poll::Pending,
    }
}

fn write<W: AsyncWrite + Unpin>(wr: &mut W, w: &Waker, data: &[u8]) -> Poll<usize> {
    match Pin::new(wr).poll_write(&mut Context::from_waker(w), data) {
        Poll::Ready(res) => Poll::Ready(res.unwrap()),
        Poll::Pending => Poll::Pending,
    }
}

fn flush<W: AsyncWrite + Unpin>(wr: &mut W, w: &Waker) -> Poll<()> {
    Pin::new(wr)
        .poll_flush(&mut Context::from_waker(w))
        .map(|res| res.unwrap())
}

fn shutdown<W: AsyncWrite + Unpin>(wr: &mut W, w: &Waker) -> Poll<()> {
    Pin::new(wr)
        .poll_shutdown(&mut Context::from_waker(w))
        .map(|res| res.unwrap())
}

/// The ACK numbers among the bytes accepted by the lower write.
fn sent_acks(link: &WLink) -> Vec<u16> {
    parse_stream(&link.0.borrow().sent)
        .iter()
        .filter_map(|f| match f {
            Frame::Ack(a) => Some(a.an()),
            _ => None,
        })
        .collect()
}

/// Completes a local shutdown that the peer acknowledged.
fn shutdown_acked(arq: &mut WArq, link: &WLink, w: &Waker) {
    assert!(shutdown(arq, w).is_pending());
    link.push_rx(wire_ack(1));
    assert!(shutdown(arq, w).is_ready());
}

#[test]
fn small_write_does_not_steal_the_readers_wakeup() {
    let (mut arq, link, _clock) = setup();
    let (rc, rw) = counter();
    let (wc, ww) = counter();
    assert!(read(&mut arq, &rw).is_pending());
    assert_eq!(write(&mut arq, &ww, b"hi"), Poll::Ready(2));
    link.push_rx(wire_dat_ack_req(0, b"yo"));
    assert!(woken(&rc) >= 1, "reader was not woken by peer data");
    assert_eq!(read(&mut arq, &rw), Poll::Ready(b"yo".to_vec()));
    let _ = woken(&wc);
}

#[test]
fn write_after_reader_registration_with_other_wakers_each_poll() {
    let (mut arq, link, _clock) = setup();
    let (_c1, w1) = counter();
    let (c2, w2) = counter();
    assert!(read(&mut arq, &w1).is_pending());
    assert!(read(&mut arq, &w2).is_pending());
    link.push_rx(wire_dat_ack_req(0, b"z"));
    assert!(woken(&c2) >= 1, "latest waker must be woken");
}

#[test]
fn blocked_write_is_driven_by_a_read_poll() {
    let (mut arq, link, _clock) = setup();
    link.0.borrow_mut().tx_blocked = true;
    let (_rc, rw) = counter();
    let (wc, ww) = counter();
    assert_eq!(write(&mut arq, &ww, b"x"), Poll::Ready(1));
    assert!(flush(&mut arq, &ww).is_pending());
    assert!(read(&mut arq, &rw).is_pending());
    link.release_tx();
    assert!(woken(&wc) >= 1, "pending flush task was not woken");
}

#[test]
fn pending_lower_flush_wakes_the_flushing_task_after_a_read_poll() {
    let (mut arq, link, _clock) = setup();
    link.0.borrow_mut().flush_blocked = true;
    let (_rc, rw) = counter();
    let (wc, ww) = counter();
    assert_eq!(write(&mut arq, &ww, b"x"), Poll::Ready(1));
    assert!(flush(&mut arq, &ww).is_pending());
    assert!(read(&mut arq, &rw).is_pending());
    link.release_flush();
    assert!(woken(&wc) >= 1);
}

#[test]
fn dropped_writer_does_not_strand_the_reader() {
    let (mut arq, link, _clock) = setup();
    let (rc, rw) = counter();
    assert!(read(&mut arq, &rw).is_pending());
    {
        let (_wc, ww) = counter();
        assert_eq!(write(&mut arq, &ww, b"x"), Poll::Ready(1));
        assert!(flush(&mut arq, &ww).is_pending());
    }
    link.push_rx(wire_ack(1));
    link.push_rx(wire_dat(0, b"r"));
    assert!(woken(&rc) >= 1);
}

#[test]
fn timer_expiry_wakes_a_live_task_after_the_driver_changes() {
    let (mut arq, _link, clock) = setup();
    let (rc, rw) = counter();
    assert!(read(&mut arq, &rw).is_pending());
    {
        let (_wc, ww) = counter();
        assert_eq!(write(&mut arq, &ww, b"x"), Poll::Ready(1));
        assert!(flush(&mut arq, &ww).is_pending());
    }
    let before = woken(&rc);
    clock.advance(arq.rto);
    assert!(woken(&rc) > before, "timer wake went to the dropped writer");
}

#[test]
fn tokio_split_halves_are_both_woken() {
    let (arq, link, _clock) = setup();
    let (mut rd, mut wr) = tokio::io::split(arq);
    let (rc, rw) = counter();
    let (_wc, ww) = counter();
    assert!(read(&mut rd, &rw).is_pending());
    assert_eq!(write(&mut wr, &ww, b"hi"), Poll::Ready(2));
    link.push_rx(wire_dat_ack_req(0, b"yo"));
    assert!(woken(&rc) >= 1);
    assert_eq!(read(&mut rd, &rw), Poll::Ready(b"yo".to_vec()));
}

#[test]
fn idle_link_causes_no_wake_loop() {
    let (mut arq, _link, _clock) = setup();
    let (rc, rw) = counter();
    let (wc, ww) = counter();
    for _ in 0..50 {
        assert!(read(&mut arq, &rw).is_pending());
        assert!(flush(&mut arq, &ww).is_ready());
    }
    assert_eq!((woken(&rc), woken(&wc)), (0, 0));
}

#[test]
fn peer_fin_waits_for_ack_while_lower_write_is_blocked() {
    let (mut arq, link, _clock) = setup();
    link.0.borrow_mut().tx_blocked = true;
    let (rc, rw) = counter();
    link.push_rx(wire_fin(0, b""));
    assert!(
        read(&mut arq, &rw).is_pending(),
        "EOF before the ACK was sent"
    );
    assert!(
        read(&mut arq, &rw).is_pending(),
        "EOF before the ACK was sent"
    );
    assert!(sent_acks(&link).is_empty());
    let before = woken(&rc);
    link.release_tx();
    assert!(
        woken(&rc) > before,
        "reader was not woken by the lower write"
    );
    assert_eq!(read(&mut arq, &rw), Poll::Ready(Vec::new()));
    assert_eq!(sent_acks(&link), [1]);
    assert_eq!(read(&mut arq, &rw), Poll::Ready(Vec::new()));
    assert_eq!(sent_acks(&link), [1]);
}

#[test]
fn peer_fin_after_local_shutdown_waits_for_ack_while_lower_write_is_blocked() {
    let (mut arq, link, _clock) = setup();
    let (wc, ww) = counter();
    let (_rc, rw) = counter();
    shutdown_acked(&mut arq, &link, &ww);
    link.0.borrow_mut().tx_blocked = true;
    link.push_rx(wire_fin(0, b""));
    assert!(
        read(&mut arq, &rw).is_pending(),
        "EOF before the ACK was sent"
    );
    assert!(
        flush(&mut arq, &ww).is_pending(),
        "flush succeeded with an unsent ACK"
    );
    assert!(sent_acks(&link).is_empty());
    let before = woken(&wc);
    link.release_tx();
    assert!(
        woken(&wc) > before,
        "flushing task was not woken by the lower write"
    );
    assert!(flush(&mut arq, &ww).is_ready());
    assert_eq!(sent_acks(&link), [1]);
    assert_eq!(read(&mut arq, &rw), Poll::Ready(Vec::new()));
    assert!(flush(&mut arq, &ww).is_ready());
    assert!(shutdown(&mut arq, &ww).is_ready());
    assert_eq!(sent_acks(&link), [1]);
}

#[test]
fn shutdown_completing_with_peer_fin_waits_for_ack() {
    let (mut arq, link, _clock) = setup();
    let (wc, ww) = counter();
    assert!(shutdown(&mut arq, &ww).is_pending());
    link.0.borrow_mut().tx_blocked = true;
    let mut both = wire_ack(1);
    both.extend(wire_fin(0, b""));
    link.push_rx(both);
    assert!(
        shutdown(&mut arq, &ww).is_pending(),
        "shutdown succeeded with an unsent ACK"
    );
    assert!(sent_acks(&link).is_empty());
    let before = woken(&wc);
    link.release_tx();
    assert!(
        woken(&wc) > before,
        "shutdown task was not woken by the lower write"
    );
    assert!(shutdown(&mut arq, &ww).is_ready());
    assert_eq!(sent_acks(&link), [1]);
}

#[test]
fn peer_fin_waits_for_ack_while_lower_flush_is_blocked() {
    let (mut arq, link, _clock) = setup();
    link.0.borrow_mut().flush_blocked = true;
    let (rc, rw) = counter();
    link.push_rx(wire_fin(0, b""));
    assert!(
        read(&mut arq, &rw).is_pending(),
        "EOF before the ACK was flushed"
    );
    assert_eq!(
        sent_acks(&link),
        [1],
        "the ACK should be written, not flushed"
    );
    assert!(
        read(&mut arq, &rw).is_pending(),
        "EOF before the ACK was flushed"
    );
    assert_eq!(sent_acks(&link), [1], "the ACK must not be written twice");
    let before = woken(&rc);
    link.release_flush();
    assert!(
        woken(&rc) > before,
        "reader was not woken by the lower flush"
    );
    assert_eq!(read(&mut arq, &rw), Poll::Ready(Vec::new()));
    assert_eq!(sent_acks(&link), [1]);
}

/// `n` valid ACK frames that acknowledge nothing.
fn stale_acks(n: usize) -> Vec<u8> {
    wire_ack(0).repeat(n)
}

#[test]
fn read_with_more_input_than_the_budget_yields_with_a_continuation_wake() {
    let (mut arq, link, _clock) = setup();
    let (rc, rw) = counter();
    let mut bytes = stale_acks(100);
    bytes.extend(wire_dat_ack_req(0, b"end"));
    link.push_rx(bytes);
    let mut polls = 0;
    let got = loop {
        let before = woken(&rc);
        polls += 1;
        match read(&mut arq, &rw) {
            Poll::Ready(got) => break got,
            Poll::Pending => assert!(woken(&rc) > before, "no continuation wake on poll {polls}"),
        }
        assert!(polls < 20, "input never drained");
    };
    assert_eq!(got, b"end");
    assert!(polls > 1, "one poll processed unbounded input");
}

#[test]
fn flush_with_more_input_than_the_budget_yields_with_a_continuation_wake() {
    let (mut arq, link, _clock) = setup();
    let (wc, ww) = counter();
    assert_eq!(write(&mut arq, &ww, b"x"), Poll::Ready(1));
    assert!(flush(&mut arq, &ww).is_pending());
    let mut bytes = stale_acks(2000);
    bytes.extend(wire_ack(1));
    link.push_rx(bytes);
    let mut polls = 0;
    loop {
        let before = woken(&wc);
        polls += 1;
        match flush(&mut arq, &ww) {
            Poll::Ready(()) => break,
            Poll::Pending => assert!(woken(&wc) > before, "no continuation wake on poll {polls}"),
        }
        assert!(polls < 20, "input never drained");
    }
    assert!(polls > 1, "one poll processed unbounded input");
}

fn read_result<R: AsyncRead + Unpin>(r: &mut R, w: &Waker) -> Poll<std::io::Result<Vec<u8>>> {
    let mut storage = [0u8; 32];
    let mut rb = ReadBuf::new(&mut storage);
    Pin::new(r)
        .poll_read(&mut Context::from_waker(w), &mut rb)
        .map(|res| res.map(|()| rb.filled().to_vec()))
}

fn flush_result<W: AsyncWrite + Unpin>(wr: &mut W, w: &Waker) -> Poll<std::io::Result<()>> {
    Pin::new(wr).poll_flush(&mut Context::from_waker(w))
}

#[test]
fn invalid_input_fails_the_stream_without_a_wake_loop() {
    let (mut arq, link, _clock) = setup();
    let (rc, rw) = counter();
    let mut corrupt = wire_dat(0, b"zz");
    *corrupt.last_mut().unwrap() ^= 0xFF;
    link.push_rx(corrupt.repeat(2000));
    for _ in 0..3 {
        assert!(matches!(
            read_result(&mut arq, &rw),
            Poll::Ready(Err(e)) if e.kind() == std::io::ErrorKind::InvalidData
        ));
    }
    assert_eq!(woken(&rc), 0, "terminal failure must not wake the task");
    assert!(
        link.0.borrow().rx.len() > 1000,
        "failed stream must not keep consuming the lower channel"
    );
}

#[test]
fn length_corruption_cannot_acknowledge_through_the_tokio_path() {
    let (mut arq, link, _clock) = setup();
    let (_wc, ww) = counter();
    assert_eq!(write(&mut arq, &ww, b"x"), Poll::Ready(1));
    assert!(flush(&mut arq, &ww).is_pending());
    link.push_rx(length_corrupted_dat());
    assert!(matches!(
        flush_result(&mut arq, &ww),
        Poll::Ready(Err(e)) if e.kind() == std::io::ErrorKind::InvalidData
    ));
    assert_eq!((arq.sb, arq.w), (0, 1), "frame must stay outstanding");
}

#[test]
fn length_corruption_cannot_deliver_data_through_the_tokio_path() {
    let (mut arq, link, _clock) = setup();
    let (_rc, rw) = counter();
    link.push_rx(length_corrupted_dat());
    for _ in 0..2 {
        assert!(matches!(
            read_result(&mut arq, &rw),
            Poll::Ready(Err(e)) if e.kind() == std::io::ErrorKind::InvalidData
        ));
    }
    assert_eq!(arq.rn, 0);
}
