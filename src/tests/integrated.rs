use core::future::poll_fn;

use super::asyncio::run_pair;
use super::lower_flush::{BufLink, buf_pair};
use super::*;
use crate::MAX_SEQ;

type IArq = Arq<6, 16, BufLink, Crc16X25, BchAckCodec, ManualTimer>;

fn payload(seed: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| ((i * seed + i / 251) % 253) as u8)
        .collect()
}

/// Sends `data`, flushes and closes while reading everything the peer sends,
/// all from one task that owns `arq`.
async fn exchange(arq: &mut IArq, data: &[u8]) -> Vec<u8> {
    let mut off = 0;
    let mut stage = 0u8;
    let mut got = Vec::new();
    let mut eof = false;
    poll_fn(|cx| {
        loop {
            let mut progressed = false;
            match stage {
                0 if off < data.len() => {
                    if let Poll::Ready(n) = arq.poll_write(cx, &data[off..]) {
                        off += n.unwrap();
                        progressed = true;
                    }
                }
                0 => {
                    stage = 1;
                    progressed = true;
                }
                1 => {
                    if let Poll::Ready(r) = arq.poll_flush(cx) {
                        r.unwrap();
                        stage = 2;
                        progressed = true;
                    }
                }
                2 => {
                    if let Poll::Ready(r) = arq.poll_close(cx) {
                        r.unwrap();
                        stage = 3;
                        progressed = true;
                    }
                }
                _ => {}
            }
            if !eof {
                let mut buf = [0u8; 64];
                if let Poll::Ready(r) = arq.poll_read(cx, &mut buf) {
                    match r.unwrap() {
                        0 => eof = true,
                        n => got.extend_from_slice(&buf[..n]),
                    }
                    progressed = true;
                }
            }
            if stage == 3 && eof {
                return Poll::Ready(());
            }
            if !progressed {
                return Poll::Pending;
            }
        }
    })
    .await;
    got
}

#[test]
fn full_duplex_transfer_with_wrap_loss_and_buffered_io() {
    let clock = Clock::default();
    let start = MAX_SEQ - 3;
    let (mut la, mut lb) = buf_pair();
    la.drop_mod = 5;
    lb.drop_mod = 4;
    let build = |link| {
        let mut arq: IArq = ArqLayer::<6, Crc16X25, BchAckCodec>::new()
            .with_retransmit_timeout(Duration::from_millis(250), Duration::from_secs(4))
            .build(link, clock.timer());
        arq.set_seq(start);
        arq
    };
    let mut a = build(la);
    let mut b = build(lb);
    let (da, db) = (payload(31, 3000), payload(17, 2500));
    let (got_a, got_b) = run_pair(
        exchange(&mut a, &da),
        exchange(&mut b, &db),
        || clock.advance_to_next(),
        200_000,
    );
    assert_eq!(got_b, da, "a -> b stream corrupted");
    assert_eq!(got_a, db, "b -> a stream corrupted");
    assert!(
        clock.0.borrow().now > Duration::ZERO,
        "loss must have forced retransmission"
    );
}
