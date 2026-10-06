use core::pin::Pin;
use core::task::Poll;

use tokio::io::{AsyncRead, ReadBuf};

use super::*;

fn poll_into(arq: &mut TestArq, rb: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
    AsyncRead::poll_read(Pin::new(arq), &mut noop_cx(), rb)
}

#[test]
fn read_appends_after_prefilled_bytes() {
    let mut arq = make_arq();
    arq.channel.rx.extend(wire_dat(0, b"xy"));
    let mut storage = [0u8; 16];
    let mut rb = ReadBuf::new(&mut storage);
    rb.put_slice(b"abc");
    assert!(matches!(poll_into(&mut arq, &mut rb), Poll::Ready(Ok(()))));
    assert_eq!(rb.filled(), b"abcxy");
}

#[test]
fn eof_keeps_prefilled_length() {
    let mut arq = make_arq();
    arq.channel.rx.extend(wire_fin(0, b""));
    let mut storage = [0u8; 16];
    let mut rb = ReadBuf::new(&mut storage);
    rb.put_slice(b"abc");
    for _ in 0..10 {
        if poll_into(&mut arq, &mut rb).is_ready() && arq.state == State::RecvDone {
            break;
        }
    }
    assert!(matches!(poll_into(&mut arq, &mut rb), Poll::Ready(Ok(()))));
    assert_eq!(rb.filled(), b"abc");
}

#[test]
fn read_fills_only_remaining_capacity() {
    let mut arq = make_arq();
    arq.channel.rx.extend(wire_dat(0, b"wxyz"));
    let mut storage = [0u8; 5];
    let mut rb = ReadBuf::new(&mut storage);
    rb.put_slice(b"abc");
    assert!(matches!(poll_into(&mut arq, &mut rb), Poll::Ready(Ok(()))));
    assert_eq!(rb.filled(), b"abcwx");
    let mut storage = [0u8; 8];
    let mut rb = ReadBuf::new(&mut storage);
    rb.put_slice(b"ab");
    assert!(matches!(poll_into(&mut arq, &mut rb), Poll::Ready(Ok(()))));
    assert_eq!(rb.filled(), b"abyz");
}

#[test]
fn full_buffer_returns_immediately_unchanged() {
    let mut arq = make_arq();
    arq.channel.rx.extend(wire_dat(0, b"xy"));
    let mut storage = [0u8; 3];
    let mut rb = ReadBuf::new(&mut storage);
    rb.put_slice(b"abc");
    assert!(matches!(poll_into(&mut arq, &mut rb), Poll::Ready(Ok(()))));
    assert_eq!(rb.filled(), b"abc");
    assert_eq!(
        arq.channel.rx.len(),
        wire_dat(0, b"xy").len(),
        "nothing consumed"
    );
}

#[test]
fn pending_read_keeps_filled_region() {
    let mut arq = make_arq();
    let mut storage = [0u8; 16];
    let mut rb = ReadBuf::new(&mut storage);
    rb.put_slice(b"abc");
    assert!(poll_into(&mut arq, &mut rb).is_pending());
    assert_eq!(rb.filled(), b"abc");
}
