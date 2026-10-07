use super::*;
use crate::frame::MAX_PAYLOAD;

#[derive(Default)]
struct ScriptState {
    rx: VecDeque<Vec<u8>>,
    /// Frames accepted by the lower write.
    sent: Vec<Vec<u8>>,
    /// Every frame offered to the lower write, accepted or not.
    offered: Vec<Vec<u8>>,
    flushes: usize,
    write_pending: bool,
    flush_pending: bool,
    /// Added to the length a successful lower write reports.
    write_len_delta: isize,
    waker: Option<Waker>,
}

#[derive(Clone, Default)]
struct ScriptLink(Rc<RefCell<ScriptState>>);

impl ScriptLink {
    fn wake(&self) {
        let w = self.0.borrow_mut().waker.take();
        if let Some(w) = w {
            w.wake();
        }
    }
}

impl Transport for ScriptLink {
    type Error = Infallible;

    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        let mut st = self.0.borrow_mut();
        st.offered.push(buf.to_vec());
        if st.write_pending {
            st.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        st.sent.push(buf.to_vec());
        Poll::Ready(Ok(buf.len().wrapping_add_signed(st.write_len_delta)))
    }

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        let mut st = self.0.borrow_mut();
        let Some(frame) = st.rx.pop_front() else {
            st.waker = Some(cx.waker().clone());
            return Poll::Pending;
        };
        buf[..frame.len()].copy_from_slice(&frame);
        Poll::Ready(Ok(frame.len()))
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        let mut st = self.0.borrow_mut();
        if st.flush_pending {
            st.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        st.flushes += 1;
        Poll::Ready(Ok(()))
    }
}

type SArq = Arq<4, 16, ScriptLink, Crc16X25, BchAckCodec, ManualTimer>;

fn setup() -> (SArq, ScriptLink) {
    let link = ScriptLink::default();
    let arq =
        ArqLayer::<4, Crc16X25, BchAckCodec>::new().build(link.clone(), Clock::default().timer());
    (arq, link)
}

fn acked_frames(link: &ScriptLink) -> usize {
    link.0.borrow().sent.len()
}

#[test]
fn empty_buffers_complete_immediately() {
    let (mut arq, _link) = setup();
    let mut cx = noop_cx();
    assert!(matches!(
        arq.poll_read(&mut cx, &mut []),
        Poll::Ready(Ok(0))
    ));
    assert!(matches!(arq.poll_write(&mut cx, &[]), Poll::Ready(Ok(0))));
}

#[test]
fn write_accepts_a_prefix_of_the_application_bytes() {
    let (mut arq, _link) = setup();
    let mut cx = noop_cx();
    let data = [7u8; 3 * MAX_PAYLOAD];
    match arq.poll_write(&mut cx, &data) {
        Poll::Ready(Ok(n)) => assert!(0 < n && n <= MAX_PAYLOAD, "accepted {n}"),
        other => panic!("unexpected write result: {other:?}"),
    }
}

#[test]
fn small_reads_return_prefixes_of_in_order_bytes() {
    let (mut arq, link) = setup();
    let mut cx = noop_cx();
    link.0.borrow_mut().rx.push_back(wire_dat(0, b"abcdef"));
    let mut buf = [0u8; 4];
    assert!(matches!(
        arq.poll_read(&mut cx, &mut buf),
        Poll::Ready(Ok(4))
    ));
    assert_eq!(&buf, b"abcd");
    assert!(matches!(
        arq.poll_read(&mut cx, &mut buf),
        Poll::Ready(Ok(2))
    ));
    assert_eq!(&buf[..2], b"ef");
    assert!(arq.poll_read(&mut cx, &mut buf).is_pending());
}

#[test]
fn repeated_flush_after_acknowledgement_sends_nothing_more() {
    let (mut arq, link) = setup();
    let mut cx = noop_cx();
    assert!(matches!(
        arq.poll_write(&mut cx, b"abc"),
        Poll::Ready(Ok(3))
    ));
    assert!(arq.poll_flush(&mut cx).is_pending());
    link.0.borrow_mut().rx.push_back(wire_ack(1));
    assert!(matches!(arq.poll_flush(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(link.0.borrow().sent, [wire_dat_ack_req(0, b"abc")]);
    let sent = acked_frames(&link);
    for _ in 0..3 {
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Ready(Ok(()))));
    }
    assert_eq!(acked_frames(&link), sent);
}

#[test]
fn short_lower_write_is_a_contract_error_without_a_second_transmission() {
    for delta in [-1isize, 1] {
        let (mut arq, link) = setup();
        let mut cx = noop_cx();
        link.0.borrow_mut().write_len_delta = delta;
        assert!(matches!(
            arq.poll_write(&mut cx, b"abc"),
            Poll::Ready(Ok(3))
        ));
        assert!(matches!(
            arq.poll_flush(&mut cx),
            Poll::Ready(Err(ArqError::WriteLength))
        ));
        assert_eq!(link.0.borrow().offered.len(), 1, "delta {delta}");
        for _ in 0..2 {
            assert!(matches!(
                arq.poll_flush(&mut cx),
                Poll::Ready(Err(ArqError::Closed))
            ));
        }
        assert_eq!(link.0.borrow().offered.len(), 1, "delta {delta}");
    }
}

#[test]
fn pending_lower_write_is_retried_with_the_same_frame() {
    let (mut arq, link) = setup();
    let (c, w) = super::wake::counter();
    let mut cx = Context::from_waker(&w);
    link.0.borrow_mut().write_pending = true;
    assert!(matches!(
        arq.poll_write(&mut cx, b"abc"),
        Poll::Ready(Ok(3))
    ));
    for _ in 0..3 {
        assert!(arq.poll_flush(&mut cx).is_pending());
    }
    {
        let st = link.0.borrow();
        assert!(st.sent.is_empty());
        assert!(st.offered.windows(2).all(|p| p[0] == p[1]), "frame changed");
        assert_eq!(st.offered[0], wire_dat_ack_req(0, b"abc"));
    }
    link.0.borrow_mut().write_pending = false;
    link.wake();
    assert!(super::wake::woken(&c) >= 1);
    assert!(arq.poll_flush(&mut cx).is_pending());
    let st = link.0.borrow();
    assert_eq!(st.sent[0], wire_dat_ack_req(0, b"abc"));
    assert_eq!(
        st.sent
            .iter()
            .filter(|f| **f == wire_dat_ack_req(0, b"abc"))
            .count(),
        1,
        "the stalled frame was transmitted twice"
    );
}

#[test]
fn pending_lower_flush_never_writes_the_frame_again() {
    let (mut arq, link) = setup();
    let mut cx = noop_cx();
    link.0.borrow_mut().flush_pending = true;
    assert!(matches!(
        arq.poll_write(&mut cx, b"abc"),
        Poll::Ready(Ok(3))
    ));
    for _ in 0..4 {
        assert!(arq.poll_flush(&mut cx).is_pending());
    }
    assert_eq!(link.0.borrow().offered.len(), 1);
    link.0.borrow_mut().flush_pending = false;
    assert!(arq.poll_flush(&mut cx).is_pending());
    let st = link.0.borrow();
    assert_eq!(st.offered[0], wire_dat_ack_req(0, b"abc"));
    assert_eq!(
        st.offered
            .iter()
            .filter(|f| **f == wire_dat_ack_req(0, b"abc"))
            .count(),
        1
    );
}
