//! The `embedded_io_async` interface for [`Arq`].
//!
//! `Arq` implements [`embedded_io_async::Read`] and
//! [`embedded_io_async::Write`], and [`EiaLower`] adapts an
//! `embedded_io_async` stream to the channel interface.

use core::pin::Pin;
use core::task::{Context, Poll};

use crate::Arq;
use crate::ack_codec::AckCodec;
use crate::crc::Crc16;
use crate::error::ArqError;
use crate::transport::FrameIo;
use crate::{Op, OpOut};

/// Adapts an `embedded_io_async` stream to the channel interface required by
/// [`Arq`].
///
/// The inner stream must implement both [`embedded_io_async::Read`] and
/// [`embedded_io_async::Write`].
pub struct EiaLower<S>(
    /// The wrapped stream.
    pub S,
);

impl<S> FrameIo for EiaLower<S>
where
    S: embedded_io_async::Read + embedded_io_async::Write,
{
    type Error = S::Error;

    fn poll_send(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>> {
        let mut fut = self.0.write(buf);
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let mut fut = self.0.read(buf);
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut fut = self.0.flush();
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }
}

/// The error type of [`Arq`] under the `embedded_io_async` interface.
#[allow(private_bounds)]
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, E>
    embedded_io_async::ErrorType for Arq<N, M, R, Channel, Crc, AckCodecType>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo<Error = E>,
    E: embedded_io_async::Error,
{
    type Error = ArqError<E>;
}

/// Reads in-order data from the peer through the ARQ layer.
#[allow(private_bounds)]
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, E>
    embedded_io_async::Read for Arq<N, M, R, Channel, Crc, AckCodecType>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo<Error = E>,
    E: embedded_io_async::Error,
{
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() {
            return Ok(0);
        }
        match (ReadDrive { arq: self, buf }).await {
            Ok(OpOut::Read(n)) => Ok(n),
            Ok(_) => unreachable!(),
            Err(e) => Err(e),
        }
    }
}

/// Writes data to the peer through the ARQ layer.
#[allow(private_bounds)]
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, E>
    embedded_io_async::Write for Arq<N, M, R, Channel, Crc, AckCodecType>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo<Error = E>,
    E: embedded_io_async::Error,
{
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        match (WriteDrive { arq: self, buf }).await {
            Ok(OpOut::Write(n)) => Ok(n),
            Ok(_) => unreachable!(),
            Err(e) => Err(e),
        }
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        match (FlushDrive { arq: self }).await {
            Ok(OpOut::Done) => Ok(()),
            Ok(_) => unreachable!(),
            Err(e) => Err(e),
        }
    }
}

#[allow(private_bounds)]
struct ReadDrive<
    'a,
    Channel,
    Crc,
    AckCodecType: AckCodec<M>,
    const N: usize,
    const M: usize,
    const R: usize,
> {
    arq: &'a mut Arq<N, M, R, Channel, Crc, AckCodecType>,
    buf: &'a mut [u8],
}

impl<Channel, Crc, AckCodecType, const N: usize, const M: usize, const R: usize>
    core::future::Future for ReadDrive<'_, Channel, Crc, AckCodecType, N, M, R>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo,
{
    type Output = Result<OpOut, ArqError<Channel::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut op = Op::Read { buf: this.buf };
        match this.arq.poll_op(cx, &mut op) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => Poll::Ready(res),
        }
    }
}

#[allow(private_bounds)]
struct WriteDrive<
    'a,
    Channel,
    Crc,
    AckCodecType: AckCodec<M>,
    const N: usize,
    const M: usize,
    const R: usize,
> {
    arq: &'a mut Arq<N, M, R, Channel, Crc, AckCodecType>,
    buf: &'a [u8],
}

impl<Channel, Crc, AckCodecType, const N: usize, const M: usize, const R: usize>
    core::future::Future for WriteDrive<'_, Channel, Crc, AckCodecType, N, M, R>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo,
{
    type Output = Result<OpOut, ArqError<Channel::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut op = Op::Write { buf: this.buf };
        match this.arq.poll_op(cx, &mut op) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => Poll::Ready(res),
        }
    }
}

#[allow(private_bounds)]
struct FlushDrive<
    'a,
    Channel,
    Crc,
    AckCodecType: AckCodec<M>,
    const N: usize,
    const M: usize,
    const R: usize,
> {
    arq: &'a mut Arq<N, M, R, Channel, Crc, AckCodecType>,
}

impl<Channel, Crc, AckCodecType, const N: usize, const M: usize, const R: usize>
    core::future::Future for FlushDrive<'_, Channel, Crc, AckCodecType, N, M, R>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo,
{
    type Output = Result<OpOut, ArqError<Channel::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut op = Op::Flush;
        match this.arq.poll_op(cx, &mut op) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => Poll::Ready(res),
        }
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
