//! Asynchronous ARQ (Automatic Repeat reQuest) layer for unreliable point-to-point links.
//!
//! The layer wraps a duplex byte-stream channel and exposes a reliable,
//! in-order, bidirectional byte stream. Frames are retransmitted until they are
//! acknowledged and out-of-order frames are buffered until their predecessors
//! arrive, so the reader sees exactly the bytes the peer wrote, in order.
//!
//! The layer is a single polled state machine without internal tasks; it makes
//! progress whenever it is polled through one of its two async interfaces:
//!
//! - `tokio::io::AsyncRead` / `tokio::io::AsyncWrite`, enabled by the `tokio`
//!   feature (default). Any `AsyncRead + AsyncWrite + Unpin` stream can be used
//!   as the channel. Errors are returned as `std::io::Error` whose kind
//!   reflects the cause and whose payload is the original [`ArqError`].
//! - `embedded_io_async::Read` / `embedded_io_async::Write`, enabled by the
//!   `embedded-io` feature. Implement `embedded_io::PollTransport` for your
//!   stream and wrap it in `embedded_io::EiaPoll` to use it as the channel.
//!
//! One instance is one link: the peer must run its own instance on the other
//! end of the channel.
//!
//! ## Building an instance
//!
//! Instances are built through [`ArqLayer`], which fixes the retransmission
//! window `N` and carries the CRC algorithm and ACK codec:
//!
//! ```no_run
//! # #[cfg(feature = "tokio")]
//! # {
//! use arq_io_async::{ArqLayer, r};
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//!
//! let (mut rx, _tx) = tokio::io::duplex(1024);
//! let layer = ArqLayer::<8, _, _>::new();
//! let mut arq = layer.build::<16, { r::<8>() }, _>(rx);
//!
//! let _ = async {
//!     arq.write_all(b"hello").await?;
//!     arq.flush().await?;
//!     let mut buf = [0u8; 5];
//!     arq.read_exact(&mut buf).await
//! };
//! # }
//! ```
//!
//! `16` is the codeword length of the default ACK codec, and `r::<8>()` is the
//! minimum read buffer size for a window of `8`.
//!
//! ## Retransmission timer
//!
//! Unacknowledged frames are retransmitted when a retransmission timeout
//! expires, never merely because the layer is polled. The timeout starts at
//! 250 ms, doubles on every expiry without progress up to 4 s, and returns to
//! its initial value whenever an ACK acknowledges new data. Change the bounds
//! with [`ArqLayer::with_retransmit_timeout`].
//!
//! The time source is supplied through the [`Timer`] trait:
//!
//! - With the `std` feature (enabled by `tokio`), `ArqLayer::build` uses
//!   `StdTimer`, which works with any executor.
//! - Without `std`, implement [`Timer`] for your platform and use
//!   [`ArqLayer::build_with_timer`].
//!
//! ## Flushing
//!
//! `flush` completes once every written byte has been acknowledged by the peer
//! and every ACK the layer owes the peer has been written and flushed on the
//! lower channel.
//! The last data frame of the flushed burst asks the peer to acknowledge it
//! immediately, so a short write followed by `flush` does not wait for more
//! traffic. Ordinary writes are still acknowledged in batches. `flush` does not
//! close the link and can be called any number of times.
//!
//! ## Corrupt and lost frames
//!
//! The channel is treated as a plain byte stream, so the length field in the
//! frame header is the only frame boundary. Input with an unknown frame type,
//! an impossible length, or a complete frame that fails validation (such as a
//! bad CRC) cannot be resynchronized, because its length cannot be trusted, and
//! fails the link with [`ArqError::Framing`]. With `embedded-io`, use
//! `embedded_io::EiaFramed` over a `embedded_io::ReadFrame` transport to
//! preserve authoritative boundaries instead. There an invalid frame is
//! discarded without retaining bytes or consuming any part of the next frame,
//! and it is recovered by retransmission.
//!
//! Lost or corrupt ACKs over a framing transport are recovered the same way,
//! and so are lost ACKs on any channel: the retransmitted frame is a duplicate,
//! and the peer answers duplicates with a fresh ACK. This includes a duplicate
//! `FIN` received after the stream has completed; see "Closing the link".
//!
//! ## Closing the link
//!
//! `AsyncWrite::shutdown` flushes any pending data in a final `FIN` frame and
//! signals end-of-stream to the peer. Afterwards `write` fails with
//! [`ArqError::Closed`], and the peer's `read` returns `0` bytes once its
//! stream is drained.
//!
//! The reader sees end-of-stream only after the layer has sent and flushed the
//! ACK for the peer's `FIN`. `shutdown` likewise completes only after the ACK
//! for a received `FIN` has been delivered.
//!
//! Completion means that application data transfer is finished, not that the
//! layer can no longer receive. If the ACK for a `FIN` is lost, the peer
//! retransmits the `FIN`, and the completed layer acknowledges the duplicate
//! whenever it is polled. It never sends new data or reopens writing, and no
//! fixed linger time is used, because no finite delay can prove that the peer
//! received the last ACK.
//!
//! Recovery therefore works only while the instance keeps being polled. Once
//! polling stops or the instance is dropped, a lost final ACK cannot be
//! recovered.
//!
//! ## Lower end-of-stream
//!
//! If the lower channel's read side ends, frames already received are still
//! delivered. `read` returns the accepted data first, then `0` bytes if a
//! valid `FIN` was received, otherwise [`ArqError::Closed`]. Writing is not
//! disabled, but `flush` and `shutdown` fail with [`ArqError::Closed`] once
//! they would have to wait for an ACK that can no longer arrive.
//!
//! ## Work per poll
//!
//! Each poll processes a bounded amount of received input, including corrupt
//! frames, and then services transmission. When input remains, the polled task
//! is woken so processing continues in a later poll. This also holds after the
//! stream has completed, where received frames are still processed to
//! acknowledge duplicates.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(test)]
extern crate std;

mod ack_codec;
mod bch;
mod crc;
#[cfg(feature = "embedded-io")]
pub mod embedded_io;
mod error;
mod frame;
mod timer;
#[cfg(feature = "tokio")]
mod tokio;
mod transport;

#[cfg(test)]
mod tests;

use ::futures::task::AtomicWaker;
use core::marker::PhantomData;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
#[cfg(feature = "std")]
use std::sync::Arc;

pub use crate::ack_codec::{AckCodec, BchAckCodec};
pub use crate::crc::Crc16;
pub use crate::error::{AckError, ArqError, FrameError};
pub use crate::frame::{AckFrame, MAX_SEQ};
#[cfg(feature = "std")]
pub use crate::timer::StdTimer;
pub use crate::timer::Timer;

use crate::frame::{DatFrame, Frame, MAX_PAYLOAD};
use crate::transport::FrameIo;

/// Maximum wire-frame length, including the header.
pub const MAX_FRAME: usize = 256;

const DEFAULT_RTO_INITIAL: Duration = Duration::from_millis(250);
const DEFAULT_RTO_MAX: Duration = Duration::from_secs(4);

/// Work units (lower reads and decoded or discarded frames) one receive batch
/// may spend before yielding.
const RX_BUDGET: usize = 32;
/// Receive/transmit rounds a flush or shutdown poll may run before yielding.
const MAX_ROUNDS: usize = 32;

/// Minimum size, in bytes, of the read buffer for a retransmission window of `N`.
///
/// [`ArqLayer::build_with_timer`] requires `R >= r::<N>()`.
pub const fn r<const N: usize>() -> usize {
    2 * N * MAX_PAYLOAD
}

fn dist(a: u16, b: u16) -> u16 {
    b.wrapping_sub(a) & (MAX_SEQ - 1)
}

/// Progress of the stream, derived by `transition` from `tx_done` and
/// `rx_complete`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Neither direction has finished.
    Active,
    /// Our `FIN` and all data before it were acknowledged.
    SendDone,
    /// The peer's `FIN` was received, its data drained and its ACK flushed.
    RecvDone,
    /// Both directions finished. Only ACKs for duplicate frames are still
    /// serviced, by `poll_completed`.
    Done,
}

/// What `service_tx` may send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxPolicy {
    /// Only ACKs.
    Acks,
    /// ACKs, retransmissions and data, requesting an ACK for the last frame.
    Flush,
    /// ACKs, retransmissions and data.
    Full,
}

/// Wakers of the operations waiting on the engine.
///
/// With `std`, the lower channel and the timer are given one stable waker that
/// wakes both of these, so readiness reaches a task that can drive the
/// engine whichever operation polled it last. Without `std` there is no
/// allocation to hold a shared wake object, and the lower I/O gets the
/// caller's waker: `embedded_io_async` operations borrow the instance mutably,
/// so only one operation is ever waiting.
#[derive(Debug, Default)]
struct Wakers {
    read: AtomicWaker,
    write: AtomicWaker,
}

#[cfg(feature = "std")]
impl std::task::Wake for Wakers {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.read.wake();
        self.write.wake();
    }
}

#[cfg(feature = "std")]
type SharedWakers = Arc<Wakers>;
#[cfg(not(feature = "std"))]
type SharedWakers = Wakers;

pub(crate) enum Op<'a> {
    Read {
        buf: &'a mut [u8],
    },
    Write {
        buf: &'a [u8],
    },
    Flush,
    #[cfg_attr(not(feature = "tokio"), allow(dead_code))]
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpOut {
    Read(usize),
    Write(usize),
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutgoingKind {
    Ack,
    Data,
}

#[derive(Debug, Clone, Copy)]
struct Outgoing {
    kind: OutgoingKind,
    buf: [u8; MAX_FRAME],
    off: usize,
    total: usize,
    /// All bytes were accepted by the channel; the channel flush is pending.
    flushing: bool,
    /// Arm the retransmission timer once the frame is flushed.
    arm_timer: bool,
}

/// The outcome of one attempt to take a frame from the lower channel.
#[allow(clippy::large_enum_variant)]
enum Recv {
    Frame(Frame),
    /// No complete frame is available; the lower read registered its waker.
    Pending,
    /// The lower read side ended and every buffered frame was consumed.
    Eof,
    /// The work budget ran out; more input may be buffered.
    Budget,
}

struct RecvBatch {
    frames: usize,
    /// The batch stopped on its budget rather than on an idle input.
    more: bool,
}

#[derive(Debug, Clone, Copy)]
struct Pending {
    buf: [u8; MAX_PAYLOAD],
    len: usize,
}

/// The frames of a window of `N` sequence numbers starting at `base`.
///
/// Slots are addressed by distance from `base`, so the mapping holds across
/// sequence-number wrap for any `N`.
#[derive(Debug, Clone, Copy)]
struct Ring<const N: usize> {
    slots: [Option<DatFrame>; N],
    base: u16,
    head: usize,
}

impl<const N: usize> Ring<N> {
    fn new() -> Self {
        Self {
            slots: [None; N],
            base: 0,
            head: 0,
        }
    }

    fn slot(&self, sn: u16) -> Option<usize> {
        let off = dist(self.base, sn) as usize;
        (off < N).then_some((self.head + off) % N)
    }

    fn get(&self, sn: u16) -> Option<DatFrame> {
        let f = self.slots[self.slot(sn)?]?;
        debug_assert_eq!(f.sn(), sn);
        Some(f)
    }

    /// Stores `f` in its slot, unless the slot is outside the window or holds
    /// another frame.
    fn insert(&mut self, f: DatFrame) -> bool {
        let Some(i) = self.slot(f.sn()) else {
            return false;
        };
        match self.slots[i] {
            Some(old) if old.sn() != f.sn() => {
                debug_assert!(false, "slot of {} holds {}", f.sn(), old.sn());
                false
            }
            _ => {
                self.slots[i] = Some(f);
                true
            }
        }
    }

    /// Removes the frame at `base`, if any, and moves the window forward by one.
    fn advance(&mut self) -> Option<DatFrame> {
        let f = self.slots[self.head].take();
        self.head = (self.head + 1) % N;
        self.base = (self.base + 1) % MAX_SEQ;
        f
    }
}

/// A reliable, in-order byte stream over an unreliable duplex channel.
///
/// `Arq` has no methods of its own; drive it through
/// `tokio::io::AsyncRead` / `tokio::io::AsyncWrite` (feature `tokio`) or
/// `embedded_io_async::Read` / `embedded_io_async::Write` (feature
/// `embedded-io`). Construct instances with [`ArqLayer::build_with_timer`], or
/// with `ArqLayer::build` when the `std` feature is enabled.
///
/// Parameters:
///
/// - `N`: retransmission window, in frames. Must be even and in `2..=32`.
/// - `M`: ACK codeword length, in bytes. Must match the `AckCodecType` in
///   use, i.e. [`AckCodec<M>`].
/// - `R`: read buffer size, in bytes. Must be at least `r::<N>()`.
/// - `Channel`: the underlying byte-stream channel. With feature `tokio` this
///   is any `AsyncRead + AsyncWrite + Unpin` stream; with feature
///   `embedded-io`, wrap a `PollTransport` in `embedded_io::EiaPoll`.
/// - `Crc`: the [`Crc16`] algorithm used to protect frames.
/// - `AckCodecType`: the [`AckCodec`] used to protect ACK frames.
/// - `Tmr`: the [`Timer`] that schedules retransmissions.
///
/// Reading returns `Ok(0)` when the peer has closed its stream and the buffer
/// is drained. Writing after the link is closed fails with [`ArqError::Closed`].
#[derive(Debug)]
pub struct Arq<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, Tmr>
where
    AckCodecType: AckCodec<M>,
{
    channel: Channel,
    crc: Crc,
    timer: Tmr,
    timer_running: bool,
    rto: Duration,
    rto_initial: Duration,
    rto_max: Duration,
    retx: usize,
    state: State,
    sb: u16,
    w: usize,
    r: u16,
    sbuf: Ring<N>,
    fin_armed: bool,
    fin_sent: bool,
    fin_acked: bool,
    tx_done: bool,
    pending: Pending,
    outgoing: Option<Outgoing>,
    ack_pending: Option<u16>,
    rn: u16,
    acount: usize,
    rx_finished: bool,
    rbuf: Ring<N>,
    rx_pending: [u8; MAX_FRAME],
    rx_len: usize,
    rx_eof: bool,
    read_buf: [u8; R],
    read_head: usize,
    read_tail: usize,
    wakers: SharedWakers,
    #[cfg(feature = "std")]
    lower_waker: Waker,
    spin_waker: AtomicWaker,
    p_ack_codec: PhantomData<fn() -> AckCodecType>,
}

impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, Tmr>
    Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>
where
    AckCodecType: AckCodec<M>,
{
    fn new(channel: Channel, crc: Crc, timer: Tmr) -> Self {
        let wakers = SharedWakers::default();
        Self {
            channel,
            crc,
            timer,
            timer_running: false,
            rto: DEFAULT_RTO_INITIAL,
            rto_initial: DEFAULT_RTO_INITIAL,
            rto_max: DEFAULT_RTO_MAX,
            retx: 0,
            state: State::Active,
            sb: 0,
            w: 0,
            r: 0,
            sbuf: Ring::new(),
            fin_armed: false,
            fin_sent: false,
            fin_acked: false,
            tx_done: false,
            pending: Pending {
                buf: [0; MAX_PAYLOAD],
                len: 0,
            },
            outgoing: None,
            ack_pending: None,
            rn: 0,
            acount: 0,
            rx_finished: false,
            rbuf: Ring::new(),
            rx_pending: [0; MAX_FRAME],
            rx_len: 0,
            rx_eof: false,
            read_buf: [0; R],
            read_head: 0,
            read_tail: 0,
            #[cfg(feature = "std")]
            lower_waker: Waker::from(wakers.clone()),
            wakers,
            spin_waker: AtomicWaker::new(),
            p_ack_codec: PhantomData,
        }
    }

    #[cfg(test)]
    fn set_seq(&mut self, sn: u16) {
        self.sb = sn;
        self.r = sn;
        self.rn = sn;
        self.sbuf.base = sn;
        self.rbuf.base = sn;
    }
}

#[allow(private_bounds)]
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, Tmr>
    Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo,
    Tmr: Timer,
{
    pub(crate) fn poll_op(
        &mut self,
        cx: &mut Context<'_>,
        op: &mut Op<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        // Register before driving the engine so readiness that arrives while
        // polling wakes this operation.
        self.register(cx.waker(), op);
        #[cfg(feature = "std")]
        let lower = self.lower_waker.clone();
        #[cfg(feature = "std")]
        let mut lower_cx = Context::from_waker(&lower);
        #[cfg(not(feature = "std"))]
        let mut lower_cx = Context::from_waker(cx.waker());
        let res = self.poll_op_lower(cx.waker(), &mut lower_cx, op);
        // Frames processed while driving wake the registered waker, which
        // would leave a pending operation without one.
        if res.is_pending() {
            self.register(cx.waker(), op);
        }
        res
    }

    fn register(&self, waker: &Waker, op: &Op<'_>) {
        match op {
            Op::Read { .. } => self.wakers.read.register(waker),
            _ => self.wakers.write.register(waker),
        }
    }

    fn poll_op_lower(
        &mut self,
        caller: &Waker,
        cx: &mut Context<'_>,
        op: &mut Op<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        if self.state == State::Done {
            return self.poll_completed(caller, cx, op);
        }
        match op {
            Op::Read { buf: [] } => Poll::Ready(Ok(OpOut::Read(0))),
            Op::Write { buf: [] } => Poll::Ready(Ok(OpOut::Write(0))),
            Op::Read { .. } => {
                let tx = if self.state == State::SendDone {
                    TxPolicy::Acks
                } else {
                    TxPolicy::Full
                };
                self.poll_engine(caller, cx, op, tx)
            }
            Op::Write { .. } => {
                if self.fin_armed {
                    Poll::Ready(Err(ArqError::Closed))
                } else {
                    self.poll_engine(caller, cx, op, TxPolicy::Full)
                }
            }
            Op::Flush => {
                let tx = if self.state == State::SendDone {
                    TxPolicy::Acks
                } else {
                    TxPolicy::Flush
                };
                self.poll_flush(caller, cx, tx)
            }
            Op::Shutdown => self.poll_shutdown(caller, cx),
        }
    }

    fn poll_engine(
        &mut self,
        caller: &Waker,
        cx: &mut Context<'_>,
        op: &mut Op<'_>,
        tx: TxPolicy,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        let batch = self.service_recv(cx)?;
        let mut acted = batch.frames > 0 || batch.more;

        self.poll_timer(cx);
        match self.service_tx(cx, tx) {
            // A stalled lower write or flush must not starve the operation:
            // data already received can still be read.
            Poll::Pending => {}
            Poll::Ready(Ok(sent)) => acted |= sent,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
        }

        self.transition();

        if let Some(out) = self.service_op(op) {
            return Poll::Ready(Ok(out));
        }

        match op {
            Op::Read { .. } => {
                if self.rx_complete() {
                    return Poll::Ready(Ok(OpOut::Read(0)));
                }
                if self.rx_eof && !self.rx_finished {
                    return Poll::Ready(Err(ArqError::Closed));
                }
            }
            Op::Write { .. } => {
                if self.rx_eof && self.w > 0 && self.outgoing.is_none() {
                    return Poll::Ready(Err(ArqError::Closed));
                }
            }
            Op::Flush | Op::Shutdown => {}
        }

        if acted {
            self.spin_waker.register(caller);
            self.spin_waker.wake();
        }
        Poll::Pending
    }

    fn poll_flush(
        &mut self,
        caller: &Waker,
        cx: &mut Context<'_>,
        tx: TxPolicy,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        for _ in 0..MAX_ROUNDS {
            let batch = self.service_recv(cx)?;
            self.poll_timer(cx);
            match self.service_tx(cx, tx) {
                Poll::Pending => {
                    if batch.more {
                        caller.wake_by_ref();
                    }
                    return Poll::Pending;
                }
                Poll::Ready(Ok(true)) => continue,
                Poll::Ready(Ok(false)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
            if batch.more {
                continue;
            }
            let stalled = self.pending.len > 0 || self.w > 0 || (self.fin_armed && !self.fin_sent);
            if stalled {
                return if self.rx_eof {
                    Poll::Ready(Err(ArqError::Closed))
                } else {
                    Poll::Pending
                };
            }
            return self.flush_channel(cx);
        }
        caller.wake_by_ref();
        Poll::Pending
    }

    fn poll_shutdown(
        &mut self,
        caller: &Waker,
        cx: &mut Context<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        self.fin_armed = true;
        for _ in 0..MAX_ROUNDS {
            let batch = self.service_recv(cx)?;
            self.poll_timer(cx);
            let tx = if self.tx_done {
                TxPolicy::Acks
            } else {
                TxPolicy::Flush
            };
            match self.service_tx(cx, tx) {
                Poll::Pending => {
                    if batch.more {
                        caller.wake_by_ref();
                    }
                    return Poll::Pending;
                }
                Poll::Ready(Ok(true)) => continue,
                Poll::Ready(Ok(false)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
            if batch.more {
                continue;
            }
            if self.tx_done {
                return self.flush_channel(cx);
            }
            return if self.rx_eof {
                Poll::Ready(Err(ArqError::Closed))
            } else {
                Poll::Pending
            };
        }
        caller.wake_by_ref();
        Poll::Pending
    }

    /// Application data transfer is complete. Frames are still received so a
    /// duplicate FIN, which means the peer lost our ACK, is acknowledged again.
    fn poll_completed(
        &mut self,
        caller: &Waker,
        cx: &mut Context<'_>,
        op: &mut Op<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        if matches!(op, Op::Write { .. }) {
            return Poll::Ready(Err(ArqError::Closed));
        }
        for _ in 0..MAX_ROUNDS {
            let batch = self.service_recv(cx)?;
            match self.service_tx(cx, TxPolicy::Acks) {
                Poll::Pending => {
                    if batch.more {
                        caller.wake_by_ref();
                    }
                    return Poll::Pending;
                }
                Poll::Ready(Ok(true)) => continue,
                Poll::Ready(Ok(false)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
            if batch.more {
                continue;
            }
            return match op {
                Op::Read { .. } => Poll::Ready(Ok(OpOut::Read(0))),
                _ => self.flush_channel(cx),
            };
        }
        caller.wake_by_ref();
        Poll::Pending
    }

    fn flush_channel(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        self.channel
            .poll_flush(cx)
            .map(|r| r.map(|()| OpOut::Done).map_err(ArqError::Io))
    }

    /// Takes frames from the lower channel until it is idle, at its end, or
    /// the work budget is spent.
    fn service_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Result<RecvBatch, ArqError<Channel::Error>> {
        let mut budget = RX_BUDGET;
        let mut frames = 0;
        loop {
            match self.poll_recv_frame(cx, &mut budget)? {
                Recv::Frame(frame) => {
                    self.on_frame(frame);
                    frames += 1;
                }
                Recv::Pending | Recv::Eof => {
                    return Ok(RecvBatch {
                        frames,
                        more: false,
                    });
                }
                Recv::Budget => return Ok(RecvBatch { frames, more: true }),
            }
        }
    }

    fn poll_recv_frame(
        &mut self,
        cx: &mut Context<'_>,
        budget: &mut usize,
    ) -> Result<Recv, ArqError<Channel::Error>> {
        if Channel::FRAMED_RECV {
            // Bound corrupt-frame work so noise cannot starve retransmissions.
            loop {
                if self.rx_eof {
                    return Ok(Recv::Eof);
                }
                if *budget == 0 {
                    return Ok(Recv::Budget);
                }
                *budget -= 1;
                match self.channel.poll_recv(cx, &mut self.rx_pending) {
                    Poll::Pending => return Ok(Recv::Pending),
                    Poll::Ready(Ok(0)) => self.rx_eof = true,
                    Poll::Ready(Ok(n)) => {
                        let Some(bytes) = self.rx_pending.get(..n) else {
                            continue;
                        };
                        if let Ok(frame) = Frame::from_bytes::<AckCodecType, M, _>(&self.crc, bytes)
                        {
                            return Ok(Recv::Frame(frame));
                        }
                        // No rx_len is retained: the entire invalid frame is gone.
                    }
                    Poll::Ready(Err(e)) => return Err(ArqError::Io(e)),
                }
            }
        }
        loop {
            match Frame::wire_len::<M>(&self.rx_pending[..self.rx_len]) {
                Err(FrameError::TooShort(_)) => {}
                Err(e) => return Err(ArqError::Framing(e)),
                Ok(len) if self.rx_len >= len => {
                    if *budget == 0 {
                        return Ok(Recv::Budget);
                    }
                    *budget -= 1;
                    // The length comes from an unprotected header, so it is only
                    // a candidate until the frame validates. A failed candidate
                    // is kept: its remainder cannot be trusted to start a frame,
                    // so reception stays failed and the lower channel is no
                    // longer read.
                    let frame =
                        Frame::from_bytes::<AckCodecType, M, _>(&self.crc, &self.rx_pending[..len])
                            .map_err(ArqError::Framing)?;
                    self.rx_pending.copy_within(len..self.rx_len, 0);
                    self.rx_len -= len;
                    return Ok(Recv::Frame(frame));
                }
                Ok(_) => {}
            }
            // Complete frames are consumed above, so a remainder at lower EOF
            // is a frame cut short, which is never delivered.
            if self.rx_eof {
                return Ok(Recv::Eof);
            }
            if *budget == 0 {
                return Ok(Recv::Budget);
            }
            *budget -= 1;
            match self
                .channel
                .poll_recv(cx, &mut self.rx_pending[self.rx_len..])
            {
                Poll::Pending => return Ok(Recv::Pending),
                Poll::Ready(Ok(0)) => self.rx_eof = true,
                Poll::Ready(Ok(n)) => self.rx_len += n,
                Poll::Ready(Err(e)) => return Err(ArqError::Io(e)),
            }
        }
    }

    /// Sends the next frame, if there is one. Returns whether a frame was
    /// completed; a frame that was started stays in `outgoing` across polls.
    fn service_tx(
        &mut self,
        cx: &mut Context<'_>,
        tx: TxPolicy,
    ) -> Poll<Result<bool, ArqError<Channel::Error>>> {
        let picked = match self.pick_next(tx) {
            Ok(picked) => picked,
            Err(e) => return Poll::Ready(Err(e)),
        };
        if !picked && self.outgoing.is_none() {
            return Poll::Ready(Ok(false));
        }
        self.send_one(cx).map_ok(|()| true)
    }

    fn pick_next(&mut self, tx: TxPolicy) -> Result<bool, ArqError<Channel::Error>> {
        if self.outgoing.is_some() {
            return Ok(false);
        }
        if let Some(an) = self.ack_pending.take() {
            let ack = AckFrame::new(&self.crc, an)?;
            return self.arm_outgoing(&Frame::Ack(ack), false).map(|_| true);
        }
        if tx == TxPolicy::Acks {
            return Ok(false);
        }
        if self.retx > 0 {
            let Some(f) = self.sbuf.get(self.r) else {
                debug_assert!(false, "retransmit slot empty");
                self.retx = 0;
                return Ok(false);
            };
            self.r = (self.r + 1) % MAX_SEQ;
            self.retx -= 1;
            return self
                .arm_outgoing(&Frame::from_dat(f), self.retx == 0)
                .map(|_| true);
        }
        if self.fin_armed && !self.fin_sent && self.w < N {
            let sn = (self.sb + self.w as u16) % MAX_SEQ;
            let fin = DatFrame::new_fin(&self.crc, sn, &self.pending.buf[..self.pending.len]);
            let stored = self.sbuf.insert(fin);
            debug_assert!(stored);
            self.fin_sent = true;
            self.pending.len = 0;
            self.w += 1;
            return self.arm_outgoing(&Frame::Fin(fin), true).map(|_| true);
        }
        if self.w < N && self.pending.len > 0 {
            let sn = (self.sb + self.w as u16) % MAX_SEQ;
            let payload = &self.pending.buf[..self.pending.len];
            let dat = if tx == TxPolicy::Flush {
                DatFrame::new_dat_ack_req(&self.crc, sn, payload)
            } else {
                DatFrame::new_dat(&self.crc, sn, payload)
            };
            let stored = self.sbuf.insert(dat);
            debug_assert!(stored);
            self.pending.len = 0;
            self.w += 1;
            return self.arm_outgoing(&Frame::from_dat(dat), true).map(|_| true);
        }
        if tx == TxPolicy::Flush
            && self.w > 0
            && self.pending.len == 0
            && !(self.fin_armed && !self.fin_sent)
        {
            let last = (self.sb + self.w as u16 - 1) % MAX_SEQ;
            match self.sbuf.get(last) {
                Some(f) if !f.is_fin() && !f.requests_ack() => {
                    let f = f.to_ack_req(&self.crc);
                    self.sbuf.insert(f);
                    return self.arm_outgoing(&Frame::DatAckReq(f), false).map(|_| true);
                }
                _ => {}
            }
        }
        Ok(false)
    }

    fn arm_timer(&mut self) {
        if !self.timer_running {
            self.restart_timer();
        }
    }

    fn restart_timer(&mut self) {
        self.timer.start(self.rto);
        self.timer_running = true;
    }

    fn stop_timer(&mut self) {
        if self.timer_running {
            self.timer.stop();
            self.timer_running = false;
        }
    }

    fn poll_timer(&mut self, cx: &mut Context<'_>) {
        if !self.timer_running || self.w == 0 {
            return;
        }
        if self.timer.poll_expired(cx).is_pending() {
            return;
        }
        self.timer_running = false;
        self.r = self.sb;
        self.retx = self.w;
        self.rto = self.rto.saturating_mul(2).min(self.rto_max);
    }

    fn arm_outgoing(
        &mut self,
        frame: &Frame,
        arm_timer: bool,
    ) -> Result<(), ArqError<Channel::Error>> {
        let mut buf = [0u8; MAX_FRAME];
        let n = frame
            .to_bytes::<AckCodecType, M>(&mut buf)
            .map_err(ArqError::InvalidAck)?;
        let kind = match frame {
            Frame::Ack(_) => OutgoingKind::Ack,
            _ => OutgoingKind::Data,
        };
        self.outgoing = Some(Outgoing {
            kind,
            buf,
            off: 0,
            total: n,
            flushing: false,
            arm_timer,
        });
        Ok(())
    }

    /// Writes the remaining bytes of the outgoing frame, then flushes the
    /// channel. The frame is delivered only once the flush completes.
    fn send_one(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ArqError<Channel::Error>>> {
        loop {
            let Some(out) = self.outgoing.as_mut() else {
                return Poll::Ready(Ok(()));
            };
            if !out.flushing {
                if out.off == out.total {
                    out.flushing = true;
                    continue;
                }
                match self.channel.poll_send(cx, &out.buf[out.off..out.total]) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(0)) => return Poll::Ready(Err(ArqError::Closed)),
                    Poll::Ready(Ok(n)) => out.off += n,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(ArqError::Io(e))),
                }
                continue;
            }
            match self.channel.poll_flush(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(ArqError::Io(e))),
                Poll::Ready(Ok(())) => {
                    let arm = out.arm_timer;
                    self.outgoing = None;
                    if arm && self.w > 0 {
                        self.arm_timer();
                    }
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    fn on_frame(&mut self, frame: Frame) {
        match frame {
            Frame::Ack(ack) => self.on_ack(ack.an()),
            Frame::Dat(d) | Frame::DatAckReq(d) | Frame::Fin(d) => self.on_dat(d),
        }
    }

    fn on_ack(&mut self, an: u16) {
        let d = dist(self.sb, an);
        if d == 0 || d as usize > self.w {
            return;
        }
        if self.retx > 0 && dist(self.sb, self.r) < d {
            self.retx -= (dist(self.r, an) as usize).min(self.retx);
            self.r = an;
        }
        for _ in 0..d as usize {
            debug_assert_eq!(self.sbuf.base, self.sb);
            if let Some(f) = self.sbuf.advance() {
                if f.is_fin() {
                    self.fin_acked = true;
                }
            }
            self.sb = (self.sb + 1) % MAX_SEQ;
            self.w -= 1;
        }
        self.rto = self.rto_initial;
        if self.w == 0 {
            self.stop_timer();
        } else if self.retx == 0 {
            self.restart_timer();
        }
        self.tx_done = self.w == 0 && self.fin_acked;
        if self.w < N {
            self.wakers.write.wake();
        }
    }

    fn schedule_ack(&mut self) {
        self.ack_pending = Some(self.rn);
        self.acount = 0;
    }

    fn on_dat(&mut self, f: DatFrame) {
        let is_fin = f.is_fin();
        if self.rx_finished {
            self.schedule_ack();
            return;
        }
        let sn = f.sn();
        let d = dist(self.rn, sn);
        if d == 0 {
            if !self.push_read(f.payload()) {
                return;
            }
            // Drop any copy of this frame left over from a full read buffer.
            self.rbuf.advance();
            self.rn = (self.rn + 1) % MAX_SEQ;
            self.acount += 1;
            self.wakers.read.wake();
            if is_fin {
                self.rx_finished = true;
                self.schedule_ack();
                return;
            }
            let mut drained = false;
            while let Some(next) = self.rbuf.get(self.rn) {
                if !self.push_read(next.payload()) {
                    break;
                }
                self.rbuf.advance();
                self.rn = (self.rn + 1) % MAX_SEQ;
                self.acount += 1;
                drained = true;
                self.wakers.read.wake();
                if next.is_fin() {
                    self.rx_finished = true;
                    break;
                }
            }
            if self.rx_finished || drained || self.acount >= N || f.requests_ack() {
                self.schedule_ack();
            }
        } else if (d as usize) < N {
            match self.rbuf.get(sn) {
                Some(b) if b.sn() == sn => self.schedule_ack(),
                _ => {
                    self.rbuf.insert(f);
                    if f.requests_ack() {
                        self.schedule_ack();
                    }
                }
            }
        } else if dist(sn, self.rn) as usize <= N {
            self.schedule_ack();
        }
    }

    fn push_read(&mut self, data: &[u8]) -> bool {
        if self.read_head == self.read_tail {
            self.read_head = 0;
            self.read_tail = 0;
        } else if self.read_head > 0 {
            self.read_buf.copy_within(self.read_head..self.read_tail, 0);
            self.read_tail -= self.read_head;
            self.read_head = 0;
        }
        if self.read_tail + data.len() > self.read_buf.len() {
            return false;
        }
        self.read_buf[self.read_tail..self.read_tail + data.len()].copy_from_slice(data);
        self.read_tail += data.len();
        true
    }

    fn service_op(&mut self, op: &mut Op<'_>) -> Option<OpOut> {
        match op {
            Op::Read { buf } => {
                if buf.is_empty() {
                    return Some(OpOut::Read(0));
                }
                let avail = self.read_tail - self.read_head;
                if avail == 0 {
                    return None;
                }
                let n = avail.min(buf.len());
                buf[..n].copy_from_slice(&self.read_buf[self.read_head..self.read_head + n]);
                self.read_head += n;
                Some(OpOut::Read(n))
            }
            Op::Write { buf } => {
                if buf.is_empty() {
                    return Some(OpOut::Write(0));
                }
                let space = MAX_PAYLOAD - self.pending.len;
                if space == 0 {
                    return None;
                }
                let n = space.min(buf.len());
                self.pending.buf[self.pending.len..self.pending.len + n].copy_from_slice(&buf[..n]);
                self.pending.len += n;
                Some(OpOut::Write(n))
            }
            Op::Flush | Op::Shutdown => None,
        }
    }

    /// An ACK is queued or still being written or flushed.
    fn ack_work_pending(&self) -> bool {
        self.ack_pending.is_some()
            || matches!(&self.outgoing, Some(out) if out.kind == OutgoingKind::Ack)
    }

    /// The peer's FIN was received and acknowledged, and its data is drained.
    fn rx_complete(&self) -> bool {
        self.rx_finished && self.read_head == self.read_tail && !self.ack_work_pending()
    }

    fn transition(&mut self) {
        self.state = match (self.tx_done, self.rx_complete()) {
            (true, true) if self.outgoing.is_none() => State::Done,
            (true, _) => State::SendDone,
            (false, true) => State::RecvDone,
            (false, false) => State::Active,
        };
    }
}

/// Builder for [`Arq`] instances.
///
/// Carries the retransmission window `N`, the frame CRC algorithm, and the ACK
/// codec type. [`ArqLayer::new`] uses the defaults: CRC-16/X-25 and the
/// built-in error-correcting ACK codec.
pub struct ArqLayer<const N: usize, Crc, AckCodecType>
where
    Crc: Clone,
{
    crc: Crc,
    rto_initial: Duration,
    rto_max: Duration,
    p_ack_codec: PhantomData<fn() -> AckCodecType>,
}

impl<const N: usize> ArqLayer<N, ::crc::Crc<u16>, BchAckCodec> {
    /// Creates a layer with a retransmission window of `N` and the default CRC
    /// and ACK codec.
    ///
    /// Panics if `N` is not even or not in `2..=32`.
    pub fn new() -> Self {
        assert!((2..=32).contains(&N), "N must be between 2 and 32");
        assert!(N.is_multiple_of(2), "N must be even");
        ArqLayer {
            crc: ::crc::Crc::<u16>::new(&::crc::CRC_16_IBM_SDLC),
            rto_initial: DEFAULT_RTO_INITIAL,
            rto_max: DEFAULT_RTO_MAX,
            p_ack_codec: PhantomData,
        }
    }
}

/// Equivalent to [`ArqLayer::new`].
impl<const N: usize> Default for ArqLayer<N, ::crc::Crc<u16>, BchAckCodec> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize, Crc> ArqLayer<N, Crc, BchAckCodec>
where
    Crc: Clone,
{
    /// Replaces the ACK codec type with `AckCodecType`.
    ///
    /// `M`, the codeword length, is chosen at [`ArqLayer::build_with_timer`]
    /// and must match the codec.
    pub fn with_ack_codec_type<AckCodecType>(self) -> ArqLayer<N, Crc, AckCodecType> {
        ArqLayer {
            crc: self.crc,
            rto_initial: self.rto_initial,
            rto_max: self.rto_max,
            p_ack_codec: PhantomData,
        }
    }
}

impl<const N: usize, Crc, AckCodecType> ArqLayer<N, Crc, AckCodecType>
where
    Crc: Clone,
{
    /// Replaces the frame CRC algorithm with `crc`.
    pub fn with_crc<NewCrc>(self, crc: NewCrc) -> ArqLayer<N, NewCrc, AckCodecType>
    where
        NewCrc: Crc16 + Clone,
    {
        ArqLayer {
            crc,
            rto_initial: self.rto_initial,
            rto_max: self.rto_max,
            p_ack_codec: PhantomData,
        }
    }

    /// Sets the retransmission timeout bounds.
    ///
    /// Unacknowledged frames are retransmitted `initial` after they are sent.
    /// Each further expiry without an acknowledgement doubles the timeout, up
    /// to `max`, and an acknowledgement of new data resets it to `initial`.
    /// Defaults to 250 ms and 4 s.
    ///
    /// Choose `initial` above the link's round-trip time for a full window of
    /// frames; a shorter timeout causes needless retransmissions.
    ///
    /// Panics if `initial` is zero or `max < initial`.
    pub fn with_retransmit_timeout(mut self, initial: Duration, max: Duration) -> Self {
        assert!(
            !initial.is_zero(),
            "initial retransmit timeout must be non-zero"
        );
        assert!(max >= initial, "max retransmit timeout must be >= initial");
        self.rto_initial = initial;
        self.rto_max = max;
        self
    }
}

impl<const N: usize, Crc, AckCodecType> ArqLayer<N, Crc, AckCodecType>
where
    Crc: Crc16 + Clone,
{
    /// Builds an [`Arq`] over `channel`, using a [`StdTimer`] for
    /// retransmissions.
    ///
    /// `M` must be the codeword length of `AckCodecType`, and `R` must be at
    /// least `r::<N>()`. Requires the `std` feature; otherwise use
    /// [`ArqLayer::build_with_timer`].
    ///
    /// Panics if `R < r::<N>()`.
    #[cfg(feature = "std")]
    pub fn build<const M: usize, const R: usize, Channel>(
        &self,
        channel: Channel,
    ) -> Arq<N, M, R, Channel, Crc, AckCodecType, StdTimer>
    where
        AckCodecType: AckCodec<M>,
    {
        self.build_with_timer(channel, StdTimer::new())
    }

    /// Builds an [`Arq`] over `channel`, using `timer` for retransmissions.
    ///
    /// `M` must be the codeword length of `AckCodecType`, and `R` must be at
    /// least `r::<N>()`. Each instance needs its own timer.
    ///
    /// Panics if `R < r::<N>()`.
    pub fn build_with_timer<const M: usize, const R: usize, Channel, Tmr>(
        &self,
        channel: Channel,
        timer: Tmr,
    ) -> Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>
    where
        AckCodecType: AckCodec<M>,
        Tmr: Timer,
    {
        assert!(R >= r::<N>());
        let mut arq = Arq::new(channel, self.crc.clone(), timer);
        arq.rto = self.rto_initial;
        arq.rto_initial = self.rto_initial;
        arq.rto_max = self.rto_max;
        arq
    }
}
