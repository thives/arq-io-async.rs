use core::future::Future;
use core::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Wake;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::lower_flush::{BufLink, buf_pair};
use super::*;
use crate::MAX_SEQ;

type IArq = Arq<6, 16, { r::<6>() }, BufLink, Crc16X25, BchAckCodec, ManualTimer>;

struct Flag(AtomicBool);

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

type Task = Pin<Box<dyn Future<Output = ()>>>;

/// Polls each task only when its own waker fired; `on_stall` advances time.
fn run_tasks(mut tasks: Vec<Task>, mut on_stall: impl FnMut() -> bool) {
    let flags: Vec<_> = tasks
        .iter()
        .map(|_| Arc::new(Flag(AtomicBool::new(true))))
        .collect();
    let wakers: Vec<_> = flags.iter().map(|f| Waker::from(f.clone())).collect();
    let mut done = vec![false; tasks.len()];
    for _ in 0..200_000 {
        let mut polled = false;
        for i in 0..tasks.len() {
            if !done[i] && flags[i].0.swap(false, Ordering::SeqCst) {
                polled = true;
                done[i] = tasks[i]
                    .as_mut()
                    .poll(&mut Context::from_waker(&wakers[i]))
                    .is_ready();
            }
        }
        if done.iter().all(|d| *d) {
            return;
        }
        assert!(
            polled || on_stall(),
            "stalled with no task woken and no timer: {done:?}"
        );
    }
    panic!("no completion: {done:?}");
}

fn payload(seed: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| ((i * seed + i / 251) % 253) as u8)
        .collect()
}

#[test]
fn full_duplex_transfer_with_wrap_loss_and_buffered_partial_io() {
    let clock = Clock::default();
    let start = MAX_SEQ - 3;
    let (mut la, mut lb) = buf_pair(7);
    la.drop_mod = 5;
    lb.drop_mod = 4;
    let build = |link| {
        let mut arq: IArq = ArqLayer::<6, Crc16X25, BchAckCodec>::new()
            .with_retransmit_timeout(Duration::from_millis(250), Duration::from_secs(4))
            .build_with_timer(link, clock.timer());
        arq.set_seq(start);
        arq
    };
    let (ra, wa) = tokio::io::split(build(la));
    let (rb, wb) = tokio::io::split(build(lb));
    let (da, db) = (payload(31, 3000), payload(17, 2500));
    let got_a = Rc::new(RefCell::new(Vec::new()));
    let got_b = Rc::new(RefCell::new(Vec::new()));
    let ends: [Rc<RefCell<bool>>; 4] = Default::default();
    let writer =
        |mut w: tokio::io::WriteHalf<IArq>, data: Vec<u8>, end: Rc<RefCell<bool>>| -> Task {
            Box::pin(async move {
                w.write_all(&data).await.unwrap();
                w.flush().await.unwrap();
                w.shutdown().await.unwrap();
                *end.borrow_mut() = true;
            })
        };
    let reader = |mut r: tokio::io::ReadHalf<IArq>,
                  sink: Rc<RefCell<Vec<u8>>>,
                  end: Rc<RefCell<bool>>|
     -> Task {
        Box::pin(async move {
            let mut v = Vec::new();
            r.read_to_end(&mut v).await.unwrap();
            *sink.borrow_mut() = v;
            *end.borrow_mut() = true;
        })
    };
    run_tasks(
        vec![
            writer(wa, da.clone(), ends[0].clone()),
            reader(ra, got_a.clone(), ends[1].clone()),
            writer(wb, db.clone(), ends[2].clone()),
            reader(rb, got_b.clone(), ends[3].clone()),
        ],
        || clock.advance_to_next(),
    );
    assert!(ends.iter().all(|e| *e.borrow()));
    assert_eq!(*got_b.borrow(), da, "a -> b stream corrupted");
    assert_eq!(*got_a.borrow(), db, "b -> a stream corrupted");
    assert!(
        clock.0.borrow().now > Duration::ZERO,
        "loss must have forced retransmission"
    );
}
