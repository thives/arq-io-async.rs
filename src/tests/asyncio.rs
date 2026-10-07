use core::future::{Future, poll_fn};
use core::pin::pin;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Wake;

use super::*;

/// `async` wrappers over the poll methods, for driving instances from tasks.
pub(super) trait TransportExt: Transport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        poll_fn(|cx| self.poll_read(cx, buf)).await
    }

    async fn write_all(&mut self, mut data: &[u8]) -> Result<(), Self::Error> {
        while !data.is_empty() {
            let n = poll_fn(|cx| self.poll_write(cx, data)).await?;
            data = &data[n..];
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        poll_fn(|cx| self.poll_flush(cx)).await
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
        let mut done = 0;
        while done < buf.len() {
            match self.read(&mut buf[done..]).await? {
                0 => panic!("unexpected end of stream"),
                n => done += n,
            }
        }
        Ok(())
    }

    async fn read_to_end(&mut self) -> Result<Vec<u8>, Self::Error> {
        let mut out = Vec::new();
        let mut buf = [0u8; 64];
        loop {
            match self.read(&mut buf).await? {
                0 => return Ok(out),
                n => out.extend_from_slice(&buf[..n]),
            }
        }
    }
}

impl<T: Transport> TransportExt for T {}

struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Polls each future only when its own waker fired; `on_stall` advances time
/// and returns whether anything can still happen.
pub(super) fn run_pair<A: Future, B: Future>(
    a: A,
    b: B,
    mut on_stall: impl FnMut() -> bool,
    max_polls: usize,
) -> (A::Output, B::Output) {
    let mut a = pin!(a);
    let mut b = pin!(b);
    let fa = Arc::new(WakeFlag(AtomicBool::new(true)));
    let fb = Arc::new(WakeFlag(AtomicBool::new(true)));
    let wa = Waker::from(fa.clone());
    let wb = Waker::from(fb.clone());
    let mut ra = None;
    let mut rb = None;
    for _ in 0..max_polls {
        let mut polled = false;
        if ra.is_none() && fa.0.swap(false, Ordering::SeqCst) {
            polled = true;
            if let Poll::Ready(v) = a.as_mut().poll(&mut Context::from_waker(&wa)) {
                ra = Some(v);
            }
        }
        if rb.is_none() && fb.0.swap(false, Ordering::SeqCst) {
            polled = true;
            if let Poll::Ready(v) = b.as_mut().poll(&mut Context::from_waker(&wb)) {
                rb = Some(v);
            }
        }
        if ra.is_some() && rb.is_some() {
            return (ra.take().unwrap(), rb.take().unwrap());
        }
        if !polled && !on_stall() {
            panic!(
                "stalled: no task woken (a done: {}, b done: {})",
                ra.is_some(),
                rb.is_some()
            );
        }
    }
    panic!("no completion within {max_polls} polls");
}

/// One end of a framed duplex link that wakes the other end's reader.
pub(super) struct AsyncPipe {
    rx: Rc<RefCell<VecDeque<Vec<u8>>>>,
    tx: Rc<RefCell<VecDeque<Vec<u8>>>>,
    rx_waker: Rc<RefCell<Option<Waker>>>,
    peer_waker: Rc<RefCell<Option<Waker>>>,
    /// Indices of writes (counted from zero) that are accepted but lost.
    drop_writes: Vec<usize>,
    writes: usize,
}

pub(super) fn async_pipe(drop_a: &[usize], drop_b: &[usize]) -> (AsyncPipe, AsyncPipe) {
    let ab = Rc::new(RefCell::new(VecDeque::new()));
    let ba = Rc::new(RefCell::new(VecDeque::new()));
    let wa = Rc::new(RefCell::new(None));
    let wb = Rc::new(RefCell::new(None));
    (
        AsyncPipe {
            rx: ba.clone(),
            tx: ab.clone(),
            rx_waker: wa.clone(),
            peer_waker: wb.clone(),
            drop_writes: drop_a.to_vec(),
            writes: 0,
        },
        AsyncPipe {
            rx: ab,
            tx: ba,
            rx_waker: wb,
            peer_waker: wa,
            drop_writes: drop_b.to_vec(),
            writes: 0,
        },
    )
}

impl Transport for AsyncPipe {
    type Error = Infallible;

    fn poll_write(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        let idx = self.writes;
        self.writes += 1;
        if !self.drop_writes.contains(&idx) {
            self.tx.borrow_mut().push_back(buf.to_vec());
            if let Some(w) = self.peer_waker.borrow_mut().take() {
                w.wake();
            }
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        let Some(frame) = self.rx.borrow_mut().pop_front() else {
            *self.rx_waker.borrow_mut() = Some(cx.waker().clone());
            return Poll::Pending;
        };
        buf[..frame.len()].copy_from_slice(&frame);
        Poll::Ready(Ok(frame.len()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
}
