use core::task::{Context, Poll};

pub(crate) trait FrameIo {
    type Error;

    fn poll_send(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>>;
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>>;
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}
