use core::task::{Context, Poll};

/// A poll-based, framed transport.
///
/// [`Arq`](crate::Arq) both consumes this trait, as its lower layer, and
/// implements it, for the layer above. As a lower layer it must be framed:
/// each `poll_read` returns one whole frame and each `poll_write` carries one
/// whole frame.
///
/// A method that returns [`Poll::Pending`] must register the waker in `cx` and
/// stay in progress; the caller polls again with the same arguments.
pub trait Transport {
    /// The error returned by the transport.
    type Error;

    /// Reads one frame into `buf`.
    ///
    /// Returns the frame length, or `0` once the read side has ended. For the
    /// lower transport of [`Arq`](crate::Arq), `buf` is [`MAX_FRAME`](crate::MAX_FRAME)
    /// bytes long.
    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>>;

    /// Writes `buf`, returning how many bytes were accepted.
    ///
    /// For the lower transport of [`Arq`](crate::Arq), `buf` is one complete
    /// frame and the whole frame must be accepted.
    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>>;

    /// Completes once everything written so far has been delivered.
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}
