use core::task::{Context, Poll};

/// The lower channel of the layer.
///
/// Implementations make no assumption about packet size; the returned lengths
/// say how much was transferred.
pub(crate) trait FrameIo {
    type Error;

    /// Each receive returns exactly one nonempty frame, rather than stream bytes.
    const FRAMED_RECV: bool = false;

    /// Writes some of `buf` and returns how many bytes were accepted.
    fn poll_send(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>>;
    /// Reads into `buf` and returns how many bytes were received; `0` is end of
    /// the stream.
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>>;
    /// Flushes everything written so far to the peer.
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}
