use std::collections::VecDeque;

use super::*;
use crate::arq::{Phase, TxPolicy};
use crate::frame::MAX_PAYLOAD;
use crate::{AckCodec, AckError, Crc16, MAX_SEQ};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    Receive,
    Send,
    Flush,
}

#[derive(Default)]
struct RecoveryLink {
    rx: VecDeque<Result<Vec<u8>, Fault>>,
    tx: Vec<u8>,
    fail_send: bool,
    fail_flush: bool,
    calls: [usize; 3],
}

impl Transport for RecoveryLink {
    type Error = Fault;

    fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, Fault>> {
        self.calls[0] += 1;
        match self.rx.pop_front() {
            None => Poll::Pending,
            Some(Err(e)) => Poll::Ready(Err(e)),
            Some(Ok(bytes)) => {
                buf[..bytes.len()].copy_from_slice(&bytes);
                Poll::Ready(Ok(bytes.len()))
            }
        }
    }

    fn poll_write(&mut self, _: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Fault>> {
        self.calls[1] += 1;
        if self.fail_send {
            Poll::Ready(Err(Fault::Send))
        } else {
            self.tx.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
    }

    fn poll_flush(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.calls[2] += 1;
        if self.fail_flush {
            Poll::Ready(Err(Fault::Flush))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

fn queued_payload(link: &mut RecoveryLink) -> Vec<u8> {
    let mut expected = Vec::new();
    for sn in [0, 1, 2, 3, 4, 5, 6, 7, 9, 8] {
        link.rx
            .push_back(Ok(wire_dat(sn, &[sn as u8; MAX_PAYLOAD])));
    }
    for sn in 0..10 {
        expected.extend_from_slice(&[sn; MAX_PAYLOAD]);
    }
    expected
}

fn assert_failed<L: Transport, A: AckCodec<16>>(arq: &Arq<4, 16, L, Crc16X25, A, ManualTimer>) {
    assert!(arq.failed);
    assert!(!arq.timer_running);
    assert!(arq.timer.deadline.is_none());
    assert!(
        arq.timer
            .clock
            .0
            .borrow()
            .waiters
            .iter()
            .all(Option::is_none)
    );
}

fn drain<L: Transport, A: AckCodec<16>>(
    arq: &mut Arq<4, 16, L, Crc16X25, A, ManualTimer>,
    expected: &[u8],
) where
    L::Error: core::fmt::Debug,
{
    let mut actual = Vec::new();
    let mut buf = [0; 37];
    while actual.len() < expected.len() {
        match arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut buf }) {
            Poll::Ready(Ok(OpOut::Read(n))) => {
                assert!(n > 0);
                actual.extend_from_slice(&buf[..n]);
            }
            other => panic!("payload lost before drain completed: {other:?}"),
        }
    }
    assert_eq!(actual, expected);
}

fn assert_closed<L: Transport, A: AckCodec<16>>(arq: &mut Arq<4, 16, L, Crc16X25, A, ManualTimer>) {
    let mut buf = [0; 1];
    for mut op in [
        Op::Read { buf: &mut buf },
        Op::Write { buf: b"late" },
        Op::Flush,
        Op::Shutdown,
    ] {
        assert!(matches!(
            arq.poll_op(&mut noop_cx(), &mut op),
            Poll::Ready(Err(ArqError::Closed))
        ));
    }
}

#[test]
fn framed_receive_error_drains_same_and_previous_batch_before_error() {
    for previous_batch in [false, true] {
        let mut arq = new_arq(RecoveryLink::default());
        assert!(matches!(
            arq.poll_op(
                &mut noop_cx(),
                &mut Op::Write {
                    buf: b"outstanding"
                }
            ),
            Poll::Ready(Ok(OpOut::Write(11)))
        ));
        assert!(arq.poll_op(&mut noop_cx(), &mut Op::Flush).is_pending());
        assert!(arq.timer_running);
        let expected = queued_payload(&mut arq.channel);
        if previous_batch {
            assert!(arq.poll_op(&mut noop_cx(), &mut Op::Flush).is_pending());
            assert_eq!(arq.rn, 8);
        }
        arq.channel.rx.push_back(Err(Fault::Receive));
        let mut first = [0; 17];
        assert!(matches!(
            arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut first }),
            Poll::Ready(Ok(OpOut::Read(17)))
        ));
        assert_eq!(first, expected[..17]);
        assert_failed(&arq);
        assert!(matches!(
            arq.terminal_error,
            Some(ArqError::Io(Fault::Receive))
        ));
        let calls = arq.channel.calls;
        arq.channel
            .rx
            .push_back(Ok(wire_dat(10, b"must not be read")));
        drain(&mut arq, &expected[17..]);
        assert_eq!(arq.rn, 10);
        assert!(matches!(
            arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut first }),
            Poll::Ready(Err(ArqError::Io(Fault::Receive)))
        ));
        assert_closed(&mut arq);
        assert_eq!(arq.channel.calls, calls);
    }
}

#[test]
fn send_and_flush_errors_preserve_buffered_data_even_if_nonread_takes_error() {
    for fault in [Fault::Send, Fault::Flush] {
        for nonread in [false, true] {
            let mut arq = new_arq(RecoveryLink::default());
            assert!(matches!(
                arq.poll_op(
                    &mut noop_cx(),
                    &mut Op::Write {
                        buf: b"outstanding"
                    }
                ),
                Poll::Ready(Ok(OpOut::Write(11)))
            ));
            assert!(arq.poll_op(&mut noop_cx(), &mut Op::Flush).is_pending());
            let expected = queued_payload(&mut arq.channel);
            arq.channel.fail_send = fault == Fault::Send;
            arq.channel.fail_flush = fault == Fault::Flush;
            let mut first = [0; 19];
            let mut op = if nonread {
                Op::Flush
            } else {
                Op::Read { buf: &mut first }
            };
            let result = arq.poll_op(&mut noop_cx(), &mut op);
            if nonread {
                assert!(matches!(result, Poll::Ready(Err(ArqError::Io(e))) if e == fault));
            } else {
                assert!(matches!(result, Poll::Ready(Ok(OpOut::Read(19)))));
                assert_eq!(first, expected[..19]);
            }
            assert_failed(&arq);
            let calls = arq.channel.calls;
            drain(&mut arq, if nonread { &expected } else { &expected[19..] });
            if !nonread {
                assert!(
                    matches!(arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut first }), Poll::Ready(Err(ArqError::Io(e))) if e == fault)
                );
            }
            assert_closed(&mut arq);
            assert_eq!(arq.channel.calls, calls);
        }
    }
}

struct RejectAck;

impl AckCodec<16> for RejectAck {
    fn encode_ack(_: AckFrame) -> Result<[u8; 16], AckError> {
        Err(AckError::DecodeError)
    }

    fn decode_ack<C: Crc16>(crc: &C, bytes: &[u8; 16]) -> Result<AckFrame, AckError> {
        BchAckCodec::decode_ack(crc, bytes)
    }
}

#[test]
fn ack_encoding_error_is_terminal_without_discarding_accepted_bytes() {
    let mut link = RecoveryLink::default();
    let expected = queued_payload(&mut link);
    let mut arq = ArqLayer::<4, Crc16X25, BchAckCodec>::new()
        .with_ack_codec_type::<RejectAck>()
        .build_with_codec::<16, _, _>(link, Clock::default().timer());
    let mut first = [0; 1];
    assert!(matches!(
        arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut first }),
        Poll::Ready(Ok(OpOut::Read(1)))
    ));
    assert_failed(&arq);
    let calls = arq.channel.calls;
    drain(&mut arq, &expected[1..]);
    assert!(matches!(
        arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut first }),
        Poll::Ready(Err(ArqError::InvalidAck(AckError::DecodeError)))
    ));
    assert_closed(&mut arq);
    assert_eq!(arq.channel.calls, calls);
}

#[test]
fn slow_reads_deliver_full_retained_payload_out_of_order_fin_and_wrap() {
    for start in [0, MAX_SEQ - 2, MAX_SEQ - 9] {
        for chunk in [1, 37, MAX_PAYLOAD, crate::arq::ReadBuf::<4>::CAPACITY] {
            let mut arq = make_arq();
            arq.set_seq(start);
            let sn = |offset: u16| (start + offset) % MAX_SEQ;
            let mut expected = Vec::new();
            for offset in 0..11u16 {
                expected.extend_from_slice(&[offset as u8; MAX_PAYLOAD]);
            }
            for offset in [0, 1, 2, 3, 4, 5, 6, 7, 9, 8, 10] {
                let frame = if offset == 10 {
                    wire_fin(sn(offset), &[offset as u8; MAX_PAYLOAD])
                } else {
                    wire_dat(sn(offset), &[offset as u8; MAX_PAYLOAD])
                };
                arq.channel.rx.extend(frame);
            }
            assert!(matches!(
                arq.poll_op(&mut noop_cx(), &mut Op::Flush),
                Poll::Ready(Ok(OpOut::Done))
            ));
            assert_eq!(arq.rn, sn(8));
            assert_eq!(sent_acks(&arq).last(), Some(&sn(8)));
            for offset in 8..11 {
                assert!(arq.rbuf.get(sn(offset)).is_some());
            }
            let mut actual = Vec::new();
            let mut buf = vec![0; chunk];
            for _ in 0..expected.len() + 2 {
                match arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut buf }) {
                    Poll::Ready(Ok(OpOut::Read(0))) => break,
                    Poll::Ready(Ok(OpOut::Read(n))) => actual.extend_from_slice(&buf[..n]),
                    other => panic!("slow read stalled: {other:?}"),
                }
            }
            assert_eq!(actual, expected);
            assert!(arq.rx_finished);
            assert_eq!(arq.rn, sn(11));
            assert_eq!(sent_acks(&arq).last(), Some(&sn(11)));
            assert!(arq.channel.rx.is_empty());
            assert!(matches!(
                arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut buf }),
                Poll::Ready(Ok(OpOut::Read(0)))
            ));
        }
    }
}

#[test]
fn partial_read_only_advances_ack_when_retained_in_order_frame_fits() {
    let mut arq = make_arq();
    let mut expected = Vec::new();
    for sn in 0..9 {
        arq.channel
            .rx
            .extend(wire_dat(sn, &[sn as u8; MAX_PAYLOAD]));
        expected.extend_from_slice(&[sn as u8; MAX_PAYLOAD]);
    }
    assert!(matches!(
        arq.poll_op(&mut noop_cx(), &mut Op::Flush),
        Poll::Ready(Ok(OpOut::Done))
    ));
    assert_eq!(arq.read_buf.len(), crate::arq::ReadBuf::<4>::CAPACITY);
    assert_eq!(arq.rn, 8);
    assert!(arq.rbuf.get(8).is_some());
    assert_eq!(sent_acks(&arq), [8]);
    assert!(arq.channel.rx.is_empty());

    let mut partial = [0; MAX_PAYLOAD - 1];
    assert!(matches!(
        arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut partial }),
        Poll::Ready(Ok(OpOut::Read(n))) if n == partial.len()
    ));
    assert_eq!(partial, expected[..MAX_PAYLOAD - 1]);
    assert_eq!(arq.rn, 8);
    assert!(arq.rbuf.get(8).is_some());
    assert!(arq.ack_pending.is_none());

    let mut last = [0; 1];
    assert!(matches!(
        arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut last }),
        Poll::Ready(Ok(OpOut::Read(1)))
    ));
    assert_eq!(last[0], expected[MAX_PAYLOAD - 1]);
    assert_eq!(arq.rn, 9);
    assert!(arq.rbuf.get(8).is_none());
    assert!(arq.ack_pending.is_none());
    assert_eq!(sent_acks(&arq), [8, 9]);
    assert!(matches!(
        arq.poll_op(&mut noop_cx(), &mut Op::Flush),
        Poll::Ready(Ok(OpOut::Done))
    ));
    assert_eq!(sent_acks(&arq), [8, 9]);
    drain(&mut arq, &expected[MAX_PAYLOAD..]);
    assert!(
        arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut last })
            .is_pending()
    );
}

#[test]
fn dead_peer_flush_and_shutdown_exhaust_exact_retry_limit() {
    for limit in [0, 2, 16, 17] {
        for shutdown in [false, true] {
            let clock = Clock::default();
            let layer = ArqLayer::<4, Crc16X25, BchAckCodec>::new();
            let layer = if limit == 16 {
                layer
            } else {
                layer.with_retry_limit(limit)
            };
            let mut arq: TestArq = layer.build(MockLink::new(), clock.timer());
            assert_eq!(arq.retry_limit, limit);
            if !shutdown {
                assert!(matches!(
                    arq.poll_op(&mut noop_cx(), &mut Op::Write { buf: b"lost" }),
                    Poll::Ready(Ok(OpOut::Write(4)))
                ));
            }
            let mut op = if shutdown { Op::Shutdown } else { Op::Flush };
            assert!(arq.poll_op(&mut noop_cx(), &mut op).is_pending());
            let original = sent_data(&arq);
            assert_eq!(original.len(), 1);
            assert_eq!(matches!(original[0], Frame::Fin(_)), shutdown);
            for round in 1..=limit {
                assert!(clock.advance_to_next());
                assert!(arq.poll_op(&mut noop_cx(), &mut op).is_pending());
                assert_eq!(arq.retries, round);
                assert_eq!(sent_data(&arq).len(), round + 1);
            }
            assert!(clock.advance_to_next());
            assert!(matches!(
                arq.poll_op(&mut noop_cx(), &mut op),
                Poll::Ready(Err(ArqError::Timeout))
            ));
            assert_failed(&arq);
            assert_eq!(sent_data(&arq).len(), limit + 1);
            let tx = arq.channel.tx.clone();
            assert_closed(&mut arq);
            clock.advance(Duration::from_secs(60));
            assert_closed(&mut arq);
            assert_eq!(arq.channel.tx, tx);
        }
    }
}

#[test]
fn timeout_read_drains_retained_data_before_original_error() {
    let clock = Clock::default();
    let mut arq = ArqLayer::<4, Crc16X25, BchAckCodec>::new()
        .with_retry_limit(0)
        .build(RecoveryLink::default(), clock.timer());
    assert!(matches!(
        arq.poll_op(&mut noop_cx(), &mut Op::Write { buf: b"lost" }),
        Poll::Ready(Ok(OpOut::Write(4)))
    ));
    assert!(arq.poll_op(&mut noop_cx(), &mut Op::Flush).is_pending());
    let expected = queued_payload(&mut arq.channel);
    assert!(arq.poll_op(&mut noop_cx(), &mut Op::Flush).is_pending());
    assert!(clock.advance_to_next());
    let mut first = [0; 1];
    assert!(matches!(
        arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut first }),
        Poll::Ready(Ok(OpOut::Read(1)))
    ));
    assert_failed(&arq);
    let calls = arq.channel.calls;
    drain(&mut arq, &expected[1..]);
    assert!(matches!(
        arq.poll_op(&mut noop_cx(), &mut Op::Read { buf: &mut first }),
        Poll::Ready(Err(ArqError::Timeout))
    ));
    assert_closed(&mut arq);
    assert_eq!(arq.channel.calls, calls);
}

#[test]
fn only_ack_progress_resets_retry_budget() {
    let clock = Clock::default();
    let mut arq: TestArq = ArqLayer::<4, Crc16X25, BchAckCodec>::new()
        .with_retry_limit(1)
        .build(MockLink::new(), clock.timer());
    let payload = [7; MAX_PAYLOAD * 2];
    let mut written = 0;
    while written < payload.len() {
        match arq.poll_op(
            &mut noop_cx(),
            &mut Op::Write {
                buf: &payload[written..],
            },
        ) {
            Poll::Ready(Ok(OpOut::Write(n))) => {
                assert!(n > 0);
                written += n;
            }
            other => panic!("write stalled: {other:?}"),
        }
    }
    assert!(arq.poll_op(&mut noop_cx(), &mut Op::Flush).is_pending());
    assert_eq!(sent_sns(&arq), [0, 1]);
    assert!(clock.advance_to_next());
    assert!(arq.poll_op(&mut noop_cx(), &mut Op::Flush).is_pending());
    assert_eq!(arq.retries, 1);
    assert_eq!(sent_sns(&arq), [0, 1, 0, 1]);
    for an in [0, 3] {
        arq.channel.rx.extend(wire_ack(an));
        assert!(arq.poll_op(&mut noop_cx(), &mut Op::Flush).is_pending());
        assert_eq!(arq.retries, 1);
    }
    arq.channel.rx.extend(wire_ack(1));
    assert!(arq.poll_op(&mut noop_cx(), &mut Op::Flush).is_pending());
    assert_eq!((arq.sb, arq.w, arq.retries), (1, 1, 0));
    assert_eq!(arq.rto, arq.rto_initial);
    assert!(clock.advance_to_next());
    assert!(arq.poll_op(&mut noop_cx(), &mut Op::Flush).is_pending());
    assert_eq!(sent_sns(&arq), [0, 1, 0, 1, 1]);
    arq.channel.rx.extend(wire_ack(1));
    assert!(arq.poll_op(&mut noop_cx(), &mut Op::Flush).is_pending());
    assert_eq!(arq.retries, 1);
    assert!(clock.advance_to_next());
    assert!(matches!(
        arq.poll_op(&mut noop_cx(), &mut Op::Flush),
        Poll::Ready(Err(ArqError::Timeout))
    ));
    assert_failed(&arq);
}

fn outgoing_snapshot<L: Transport>(
    arq: &Arq<4, 16, L, Crc16X25, BchAckCodec, ManualTimer>,
) -> ([u8; crate::MAX_FRAME], usize, Phase, bool, bool) {
    let out = arq.outgoing.as_ref().unwrap();
    (out.buf, out.total, out.phase, out.started, out.arm_timer)
}

#[test]
fn flush_converts_unsent_final_data_in_place_without_duplicate() {
    let payload: Vec<u8> = (0..MAX_PAYLOAD).map(|i| i as u8).collect();
    for sn in [0, MAX_SEQ - 1] {
        for arm_timer in [false, true] {
            let mut arq = make_arq();
            arq.set_seq(sn);
            assert!(matches!(
                arq.service_op(&mut Op::Write { buf: &payload }),
                Some(OpOut::Write(MAX_PAYLOAD))
            ));
            assert!(arq.pick_next(TxPolicy::Full).unwrap());
            let original = arq.sbuf.get(sn).unwrap();
            assert!(!original.requests_ack());
            arq.arm_outgoing(&Frame::Dat(original), arm_timer).unwrap();
            assert!(arq.channel.tx.is_empty());
            let untouched = outgoing_snapshot(&arq);
            for policy in [TxPolicy::Full, TxPolicy::Acks] {
                assert!(!arq.pick_next(policy).unwrap());
                assert_eq!(outgoing_snapshot(&arq), untouched);
                assert!(!arq.sbuf.get(sn).unwrap().requests_ack());
            }
            assert!(!arq.pick_next(TxPolicy::Flush).unwrap());
            let out = arq.outgoing.as_ref().unwrap();
            let expected = wire_dat_ack_req(sn, &payload);
            assert_eq!(&out.buf[..out.total], expected);
            assert_eq!(
                (out.phase, out.started, out.arm_timer),
                (Phase::Write, false, arm_timer)
            );
            let stored = arq.sbuf.get(sn).unwrap();
            assert!(stored.requests_ack());
            assert_eq!(stored.payload(), payload);
            assert_eq!(stored.sn(), sn);
            assert_eq!((arq.sb, arq.w, arq.pending.len), (sn, 1, 0));
            let converted = outgoing_snapshot(&arq);
            assert!(!arq.pick_next(TxPolicy::Flush).unwrap());
            assert_eq!(outgoing_snapshot(&arq), converted);
            assert!(matches!(
                arq.service_tx(&mut noop_cx(), TxPolicy::Flush),
                Poll::Ready(Ok(true))
            ));
            assert_eq!(arq.channel.tx, expected);
            assert!(
                matches!(parse_stream(&arq.channel.tx).as_slice(), [Frame::DatAckReq(d)] if d.sn() == sn && d.payload() == payload)
            );
            assert_eq!(arq.timer_running, arm_timer);
            assert_eq!(arq.timer.clock.starts().len(), usize::from(arm_timer));
            for _ in 0..2 {
                assert!(matches!(
                    arq.service_tx(&mut noop_cx(), TxPolicy::Flush),
                    Poll::Ready(Ok(false))
                ));
            }
            assert_eq!(arq.channel.tx, expected);
            arq.r = sn;
            arq.retx = 1;
            assert!(matches!(
                arq.service_tx(&mut noop_cx(), TxPolicy::Full),
                Poll::Ready(Ok(true))
            ));
            assert_eq!(arq.channel.tx, expected.repeat(2));
            assert_eq!(parse_stream(&arq.channel.tx).len(), 2);
        }
    }
}

#[test]
fn flush_does_not_rewrite_blocked_data_and_requests_ack_after_completion() {
    let payload = b"payload with a checksum and a preserved partial prefix";
    let original = wire_dat(0, payload);
    let requested = wire_dat_ack_req(0, payload);
    for cap in [0, 1, original.len() - 1] {
        let mut arq = new_arq(CapLink::new(cap));
        assert!(matches!(
            arq.service_op(&mut Op::Write { buf: payload }),
            Some(OpOut::Write(n)) if n == payload.len()
        ));
        assert!(arq.service_tx(&mut noop_cx(), TxPolicy::Full).is_pending());
        let snapshot = outgoing_snapshot(&arq);
        assert_eq!((snapshot.2, snapshot.3), (Phase::Write, true));
        assert!(arq.channel.tx.is_empty());
        assert!(!arq.pick_next(TxPolicy::Flush).unwrap());
        assert_eq!(outgoing_snapshot(&arq), snapshot);
        assert!(!arq.sbuf.get(0).unwrap().requests_ack());
        assert!(arq.service_tx(&mut noop_cx(), TxPolicy::Flush).is_pending());
        assert_eq!(outgoing_snapshot(&arq), snapshot);
        assert!(arq.channel.tx.is_empty());
        arq.channel.cap = original.len();
        assert!(matches!(
            arq.service_tx(&mut noop_cx(), TxPolicy::Flush),
            Poll::Ready(Ok(true))
        ));
        assert_eq!(arq.channel.tx, original);
        assert!(
            matches!(parse_stream(&arq.channel.tx).as_slice(), [Frame::Dat(d)] if d.payload() == payload)
        );
        assert!(arq.timer_running);
        assert_eq!(arq.timer.clock.starts().len(), 1);
        assert!(!arq.sbuf.get(0).unwrap().requests_ack());
        assert!(arq.pick_next(TxPolicy::Flush).unwrap());
        let out = arq.outgoing.as_ref().unwrap();
        assert_eq!(&out.buf[..out.total], requested);
        assert!(!out.arm_timer);
        assert!(arq.sbuf.get(0).unwrap().requests_ack());
        arq.channel.cap = original.len() + requested.len();
        assert!(matches!(
            arq.service_tx(&mut noop_cx(), TxPolicy::Flush),
            Poll::Ready(Ok(true))
        ));
        let mut expected = original.clone();
        expected.extend_from_slice(&requested);
        assert_eq!(arq.channel.tx, expected);
        assert!(
            matches!(parse_stream(&arq.channel.tx).as_slice(), [Frame::Dat(d), Frame::DatAckReq(a)] if d.sn() == 0 && a.sn() == 0 && d.payload() == payload && a.payload() == payload)
        );
        assert_eq!(arq.timer.clock.starts().len(), 1);
        assert_eq!(arq.w, 1);
        assert!(matches!(
            arq.service_tx(&mut noop_cx(), TxPolicy::Flush),
            Poll::Ready(Ok(false))
        ));
        assert_eq!(arq.channel.tx, expected);
    }
}

#[test]
fn flush_leaves_unsent_data_unchanged_when_pending_data_or_fin_will_follow() {
    for fin in [false, true] {
        let mut arq = make_arq();
        assert!(matches!(
            arq.service_op(&mut Op::Write { buf: b"first" }),
            Some(OpOut::Write(5))
        ));
        assert!(arq.pick_next(TxPolicy::Full).unwrap());
        if fin {
            arq.fin_armed = true;
        } else {
            assert!(matches!(
                arq.service_op(&mut Op::Write { buf: b"last" }),
                Some(OpOut::Write(4))
            ));
        }
        let snapshot = outgoing_snapshot(&arq);
        assert!(!arq.pick_next(TxPolicy::Flush).unwrap());
        assert_eq!(outgoing_snapshot(&arq), snapshot);
        assert!(!arq.sbuf.get(0).unwrap().requests_ack());
        assert!(matches!(
            arq.service_tx(&mut noop_cx(), TxPolicy::Flush),
            Poll::Ready(Ok(true))
        ));
        assert!(matches!(
            arq.service_tx(&mut noop_cx(), TxPolicy::Flush),
            Poll::Ready(Ok(true))
        ));
        let final_wire = if fin {
            wire_fin(1, b"")
        } else {
            wire_dat_ack_req(1, b"last")
        };
        let mut expected = wire_dat(0, b"first");
        expected.extend_from_slice(&final_wire);
        assert_eq!(arq.channel.tx, expected);
        assert_eq!(parse_stream(&arq.channel.tx).len(), 2);
        assert_eq!(arq.w, 2);
        assert!(matches!(
            arq.service_tx(&mut noop_cx(), TxPolicy::Flush),
            Poll::Ready(Ok(false))
        ));
        assert_eq!(arq.channel.tx, expected);
    }
}

#[test]
fn flush_does_not_convert_outgoing_fin_or_ack() {
    let mut fin = make_arq();
    assert!(matches!(
        fin.service_op(&mut Op::Write {
            buf: b"fin payload"
        }),
        Some(OpOut::Write(11))
    ));
    fin.fin_armed = true;
    assert!(fin.pick_next(TxPolicy::Full).unwrap());
    let snapshot = outgoing_snapshot(&fin);
    assert!(!fin.pick_next(TxPolicy::Flush).unwrap());
    assert_eq!(outgoing_snapshot(&fin), snapshot);
    assert!(fin.sbuf.get(0).unwrap().is_fin());
    assert!(matches!(
        fin.service_tx(&mut noop_cx(), TxPolicy::Flush),
        Poll::Ready(Ok(true))
    ));
    assert_eq!(fin.channel.tx, wire_fin(0, b"fin payload"));
    assert!(
        matches!(parse_stream(&fin.channel.tx).as_slice(), [Frame::Fin(d)] if d.sn() == 0 && d.payload() == b"fin payload")
    );
    assert!(fin.timer_running);
    assert!(matches!(
        fin.service_tx(&mut noop_cx(), TxPolicy::Flush),
        Poll::Ready(Ok(false))
    ));
    assert_eq!(parse_stream(&fin.channel.tx).len(), 1);

    let mut ack = make_arq();
    assert!(matches!(
        ack.service_op(&mut Op::Write { buf: b"data" }),
        Some(OpOut::Write(4))
    ));
    assert!(matches!(
        ack.service_tx(&mut noop_cx(), TxPolicy::Full),
        Poll::Ready(Ok(true))
    ));
    ack.schedule_ack();
    assert!(ack.pick_next(TxPolicy::Full).unwrap());
    let snapshot = outgoing_snapshot(&ack);
    assert!(!ack.pick_next(TxPolicy::Flush).unwrap());
    assert_eq!(outgoing_snapshot(&ack), snapshot);
    assert!(!ack.sbuf.get(0).unwrap().requests_ack());
    assert!(matches!(
        ack.service_tx(&mut noop_cx(), TxPolicy::Flush),
        Poll::Ready(Ok(true))
    ));
    let mut expected = wire_dat(0, b"data");
    expected.extend(wire_ack(0));
    assert_eq!(ack.channel.tx, expected);
    assert!(
        matches!(parse_stream(&ack.channel.tx).as_slice(), [Frame::Dat(d), Frame::Ack(a)] if d.payload() == b"data" && a.an() == 0)
    );
    assert!(matches!(
        ack.service_tx(&mut noop_cx(), TxPolicy::Flush),
        Poll::Ready(Ok(true))
    ));
    expected.extend(wire_dat_ack_req(0, b"data"));
    assert_eq!(ack.channel.tx, expected);
    assert_eq!(parse_stream(&ack.channel.tx).len(), 3);
    assert!(matches!(
        ack.service_tx(&mut noop_cx(), TxPolicy::Flush),
        Poll::Ready(Ok(false))
    ));
    assert_eq!(ack.channel.tx, expected);
}

#[test]
fn flush_does_not_convert_nonfinal_outgoing_data() {
    let mut arq = make_arq();
    for payload in [b"first".as_slice(), b"last".as_slice()] {
        assert!(
            matches!(arq.service_op(&mut Op::Write { buf: payload }), Some(OpOut::Write(n)) if n == payload.len())
        );
        assert!(matches!(
            arq.service_tx(&mut noop_cx(), TxPolicy::Full),
            Poll::Ready(Ok(true))
        ));
    }
    let first = arq.sbuf.get(0).unwrap();
    arq.arm_outgoing(&Frame::Dat(first), false).unwrap();
    let snapshot = outgoing_snapshot(&arq);
    assert!(!arq.pick_next(TxPolicy::Flush).unwrap());
    assert_eq!(outgoing_snapshot(&arq), snapshot);
    assert!(!arq.sbuf.get(0).unwrap().requests_ack());
    assert!(!arq.sbuf.get(1).unwrap().requests_ack());
    assert!(matches!(
        arq.service_tx(&mut noop_cx(), TxPolicy::Flush),
        Poll::Ready(Ok(true))
    ));
    assert!(matches!(
        arq.service_tx(&mut noop_cx(), TxPolicy::Flush),
        Poll::Ready(Ok(true))
    ));
    let mut expected = wire_dat(0, b"first");
    expected.extend(wire_dat(1, b"last"));
    expected.extend(wire_dat(0, b"first"));
    expected.extend(wire_dat_ack_req(1, b"last"));
    assert_eq!(arq.channel.tx, expected);
    assert_eq!(parse_stream(&arq.channel.tx).len(), 4);
    assert_eq!(arq.w, 2);
    assert!(!arq.sbuf.get(0).unwrap().requests_ack());
    assert!(arq.sbuf.get(1).unwrap().requests_ack());
    assert!(matches!(
        arq.service_tx(&mut noop_cx(), TxPolicy::Flush),
        Poll::Ready(Ok(false))
    ));
    assert_eq!(arq.channel.tx, expected);
}
