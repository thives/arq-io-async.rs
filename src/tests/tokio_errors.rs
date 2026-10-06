use core::pin::Pin;
use std::io::{self, ErrorKind};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::*;
use crate::error::AckError;
use crate::tokio::into_io;

/// A lower channel that fails each direction with a chosen kind.
struct FailIo;

impl AsyncRead for FailIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(ErrorKind::ConnectionReset.into()))
    }
}

impl AsyncWrite for FailIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(ErrorKind::BrokenPipe.into()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(ErrorKind::TimedOut.into()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug, PartialEq)]
struct Custom;

impl core::fmt::Display for Custom {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("custom")
    }
}

fn kind_of<E: 'static>(e: ArqError<E>) -> ErrorKind
where
    ArqError<E>: core::error::Error + Send + Sync + 'static,
{
    into_io(e).kind()
}

#[test]
fn lower_read_kind_is_preserved() {
    let mut arq = new_arq(FailIo);
    let mut buf = [0u8; 4];
    let mut rb = ReadBuf::new(&mut buf);
    let Poll::Ready(Err(e)) = Pin::new(&mut arq).poll_read(&mut noop_cx(), &mut rb) else {
        panic!("expected an error");
    };
    assert_eq!(e.kind(), ErrorKind::ConnectionReset);
}

#[test]
fn lower_write_kind_is_preserved() {
    struct WriteFail;
    impl AsyncRead for WriteFail {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }
    impl AsyncWrite for WriteFail {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(ErrorKind::BrokenPipe.into()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    let mut arq = new_arq(WriteFail);
    assert!(matches!(
        Pin::new(&mut arq).poll_write(&mut noop_cx(), b"abc"),
        Poll::Ready(Ok(3))
    ));
    let Poll::Ready(Err(e)) = Pin::new(&mut arq).poll_flush(&mut noop_cx()) else {
        panic!("expected an error");
    };
    assert_eq!(e.kind(), ErrorKind::BrokenPipe);
}

#[test]
fn lower_flush_kind_is_preserved() {
    struct FlushFail;
    impl AsyncRead for FlushFail {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }
    impl AsyncWrite for FlushFail {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(ErrorKind::TimedOut.into()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    let mut arq = new_arq(FlushFail);
    assert!(matches!(
        Pin::new(&mut arq).poll_write(&mut noop_cx(), b"abc"),
        Poll::Ready(Ok(3))
    ));
    let Poll::Ready(Err(e)) = Pin::new(&mut arq).poll_flush(&mut noop_cx()) else {
        panic!("expected an error");
    };
    assert_eq!(e.kind(), ErrorKind::TimedOut);
}

#[test]
fn protocol_errors_map_to_kinds() {
    type E = ArqError<io::Error>;
    assert_eq!(
        kind_of(E::Framing(FrameError::TooShort(1))),
        ErrorKind::InvalidData
    );
    assert_eq!(
        kind_of(E::InvalidAck(AckError::DecodeError)),
        ErrorKind::InvalidData
    );
    assert_eq!(kind_of(E::Timeout), ErrorKind::TimedOut);
    assert_eq!(kind_of(E::Closed), ErrorKind::BrokenPipe);
}

#[test]
fn payload_downcasts_to_arq_error() {
    let e = into_io(ArqError::<io::Error>::Closed);
    let inner = e
        .get_ref()
        .and_then(|p| p.downcast_ref::<ArqError<io::Error>>());
    assert!(matches!(inner, Some(ArqError::Closed)));
}

#[test]
fn custom_channel_error_falls_back_to_other() {
    let e = into_io(ArqError::Io(Custom));
    assert_eq!(e.kind(), ErrorKind::Other);
    let inner = e
        .get_ref()
        .and_then(|p| p.downcast_ref::<ArqError<Custom>>());
    assert_eq!(inner, Some(&ArqError::Io(Custom)));
}
