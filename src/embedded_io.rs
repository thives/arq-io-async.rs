//! The `embedded_io_async` interface for [`Arq`].
//!
//! `Arq` implements [`embedded_io_async::Read`] and
//! [`embedded_io_async::Write`]. The lower channel is a [`PollTransport`]
//! wrapped in [`EiaPoll`], or a frame-oriented transport wrapped in
//! [`EiaFramed`].
//!
//! Without the `std` feature, build instances with
//! [`ArqLayer::build_with_timer`](crate::ArqLayer::build_with_timer) and a
//! [`Timer`] for your platform.
//!
//! `Arq` is a polled state machine and cannot keep an `async` operation of the
//! lower stream alive between polls. [`PollTransport`] therefore exposes poll
//! methods, so a transport keeps an operation in progress in its own state.
//!
//! A `read` that returns `0` marks the end of the peer's stream, not a point
//! after which polling may stop: a lost ACK for the final frame is recovered
//! only while the instance keeps being polled.

use core::future::poll_fn;
use core::pin::Pin;
use core::task::{Context, Poll};

use crate::Arq;
use crate::ack_codec::AckCodec;
use crate::crc::Crc16;
use crate::error::ArqError;
use crate::timer::Timer;
use crate::transport::FrameIo;
use crate::{Op, OpOut};

/// A byte-stream transport whose operations are polled.
///
/// This is the lower-channel interface for [`EiaPoll`]. It follows stream
/// semantics and makes no assumption about packet size: use the returned
/// lengths to determine how much was transferred.
///
/// An operation that returns [`Poll::Pending`] stays in progress inside the
/// transport. `Arq` may poll a different operation before polling it again,
/// and the transport must resume it, without losing or repeating bytes, when
/// it is polled again. Each method must arrange for the waker in `cx` to be
/// woken when it can make progress.
pub trait PollTransport: embedded_io_async::ErrorType {
    /// Reads into `buf` and returns the number of bytes read. `Ok(0)` for a
    /// nonempty `buf` means the read side has ended.
    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>>;
    /// Writes a prefix of `buf` and returns the number of bytes accepted.
    /// `Ok(0)` for a nonempty `buf` is treated as the link being closed.
    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>>;
    /// Completes once every accepted byte has been delivered to the medium.
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}

/// Adapts a [`PollTransport`] to the channel interface required by [`Arq`].
///
/// The wrapped transport's operations are polled directly, never recreated,
/// so transports whose operations need several polls are supported.
pub struct EiaPoll<S>(pub S);

impl<S: PollTransport> FrameIo for EiaPoll<S> {
    type Error = S::Error;

    fn poll_send(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>> {
        self.0.poll_write(cx, buf)
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        self.0.poll_read(cx, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_flush(cx)
    }
}

/// Receives complete, bounded frames from a framing transport.
///
/// Each call returns one nonempty frame in `buf`, or zero at end-of-stream.
/// Empty frames must be skipped. Frames must never be truncated, split, or
/// joined. A successful nonempty read must return the exact number of bytes
/// copied into `buf`, never more than [`crate::MAX_FRAME`]. Oversized frames
/// must be rejected with an error rather than truncated.
///
/// The operation must be cancel-safe: dropping a pending future must not lose
/// any received bytes or frame-boundary state; a later call must resume and
/// return the same complete frame.
#[allow(async_fn_in_trait)]
pub trait ReadFrame: embedded_io_async::ErrorType {
    /// Read the next complete frame (at most [`crate::MAX_FRAME`] bytes).
    async fn read_frame(&mut self, buf: &mut [u8; crate::MAX_FRAME]) -> Result<usize, Self::Error>;
}

/// Adapts a frame-oriented transport to [`Arq`].
///
/// Unlike [`EiaPoll`], receive boundaries are authoritative: ARQ validates
/// exact lengths, types, CRCs and ACK codewords within each frame, discarding
/// the whole frame on failure. A reported receive length greater than
/// [`crate::MAX_FRAME`] is also discarded, without decoding a truncated prefix.
///
/// # Transport requirements
///
/// Implementing the traits alone is not sufficient; the inner transport must
/// satisfy these contracts:
///
/// - Receives must obey [`ReadFrame`]'s complete-frame, length, EOF, and
///   cancellation requirements. Empty frames must be skipped, not reported as
///   EOF. ARQ cannot detect lost, split, or joined frame boundaries reliably.
/// - Every successful nonempty write must accept the entire supplied ARQ frame
///   and report its full length. Ordinary writers that return successful partial
///   writes are not suitable. Transmission may proceed in fragments internally,
///   but after a pending write is cancelled, a later call must finish that same
///   frame without duplicating bytes or emitting a prefix as a separate frame.
/// - Dropping a pending flush must leave the transport usable and allow a later
///   flush to complete.
///
/// On each poll this adapter creates a new read, write, or flush future, polls
/// it once, and drops it. In-progress state must therefore live in the transport,
/// not solely in the future.
pub struct EiaFramed<S>(
    /// The wrapped framing transport.
    pub S,
);

impl<S: ReadFrame + embedded_io_async::Write> FrameIo for EiaFramed<S> {
    type Error = S::Error;
    const FRAMED_RECV: bool = true;

    fn poll_send(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>> {
        let mut fut = self.0.write(buf);
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let mut fut = self.0.read_frame(buf.try_into().expect("ARQ frame buffer"));
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut fut = self.0.flush();
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }
}

/// The error type of [`Arq`] under the `embedded_io_async` interface.
#[allow(private_bounds)]
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, Tmr, E>
    embedded_io_async::ErrorType for Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo<Error = E>,
    Tmr: Timer,
    E: embedded_io_async::Error,
{
    type Error = ArqError<E>;
}

/// Reads in-order data from the peer through the ARQ layer.
#[allow(private_bounds)]
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, Tmr, E>
    embedded_io_async::Read for Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo<Error = E>,
    Tmr: Timer,
    E: embedded_io_async::Error,
{
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut op = Op::Read { buf };
        match poll_fn(|cx| self.poll_op(cx, &mut op)).await? {
            OpOut::Read(n) => Ok(n),
            _ => unreachable!(),
        }
    }
}

/// Writes data to the peer through the ARQ layer.
#[allow(private_bounds)]
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, Tmr, E>
    embedded_io_async::Write for Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo<Error = E>,
    Tmr: Timer,
    E: embedded_io_async::Error,
{
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        let mut op = Op::Write { buf };
        match poll_fn(|cx| self.poll_op(cx, &mut op)).await? {
            OpOut::Write(n) => Ok(n),
            _ => unreachable!(),
        }
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        poll_fn(|cx| self.poll_op(cx, &mut Op::Flush))
            .await
            .map(drop)
    }
}

/// Maps [`ArqError`] variants to `embedded_io_async::ErrorKind`.
impl<E: embedded_io_async::Error> embedded_io_async::Error for ArqError<E> {
    fn kind(&self) -> embedded_io_async::ErrorKind {
        match self {
            ArqError::Io(e) => e.kind(),
            ArqError::Framing(_) | ArqError::InvalidAck(_) => {
                embedded_io_async::ErrorKind::InvalidData
            }
            ArqError::Timeout => embedded_io_async::ErrorKind::TimedOut,
            ArqError::Closed => embedded_io_async::ErrorKind::BrokenPipe,
        }
    }
}
