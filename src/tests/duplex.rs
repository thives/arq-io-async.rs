use super::asyncio::{TransportExt, async_pipe, run_pair};
use super::*;

type PipeArq = Arq<4, 16, super::asyncio::AsyncPipe, Crc16X25, BchAckCodec, ManualTimer>;

fn pair(clock: &Clock, drop_a: &[usize], drop_b: &[usize]) -> (PipeArq, PipeArq) {
    let (a, b) = async_pipe(drop_a, drop_b);
    let layer = ArqLayer::<4, Crc16X25, BchAckCodec>::new();
    (layer.build(a, clock.timer()), layer.build(b, clock.timer()))
}

#[test]
fn duplex_repeated_request_response_reuses_flush() {
    let clock = Clock::default();
    let (mut a, mut b) = pair(&clock, &[], &[]);
    let client = async {
        let mut got = Vec::new();
        for i in 0..3u8 {
            a.write_all(&[b'q', i]).await.unwrap();
            a.flush().await.unwrap();
            let mut buf = [0u8; 2];
            a.read_exact(&mut buf).await.unwrap();
            got.push(buf);
        }
        got
    };
    let server = async {
        let mut got = Vec::new();
        for _ in 0..3u8 {
            let mut buf = [0u8; 2];
            b.read_exact(&mut buf).await.unwrap();
            b.write_all(&[b'r', buf[1]]).await.unwrap();
            b.flush().await.unwrap();
            got.push(buf);
        }
        got
    };
    let (resp, req) = run_pair(client, server, || false, 10_000);
    assert_eq!(req, vec![[b'q', 0], [b'q', 1], [b'q', 2]]);
    assert_eq!(resp, vec![[b'r', 0], [b'r', 1], [b'r', 2]]);
    assert_eq!(a.state, State::Active);
    assert_eq!(b.state, State::Active);
}

async fn request(s: &mut PipeArq) -> [u8; 4] {
    s.write_all(b"ping").await.unwrap();
    s.flush().await.unwrap();
    let mut buf = [0u8; 4];
    s.read_exact(&mut buf).await.unwrap();
    core::future::poll_fn(|cx| s.poll_close(cx)).await.unwrap();
    buf
}

async fn respond(s: &mut PipeArq) -> [u8; 4] {
    let mut buf = [0u8; 4];
    s.read_exact(&mut buf).await.unwrap();
    s.write_all(b"pong").await.unwrap();
    s.flush().await.unwrap();
    let rest = s.read_to_end().await.unwrap();
    assert!(rest.is_empty());
    buf
}

#[test]
fn duplex_lost_frames_recovered_after_timeout() {
    let clock = Clock::default();
    let (mut a, mut b) = pair(&clock, &[0], &[0, 1]);
    let (resp, req) = run_pair(
        request(&mut a),
        respond(&mut b),
        || clock.advance_to_next(),
        10_000,
    );
    assert_eq!(&req, b"ping");
    assert_eq!(&resp, b"pong");
    assert!(
        clock.0.borrow().now > Duration::ZERO,
        "recovery must be timer driven"
    );
}
