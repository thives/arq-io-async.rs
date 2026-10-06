use std::collections::VecDeque;

use super::*;
use crate::MAX_FRAME;
use crate::frame::{TYPE_ACK, TYPE_DAT};

fn dat(sn: u16, payload: &[u8]) -> Vec<u8> {
    let mut bytes = [0; MAX_FRAME];
    let n = DatFrame::new_dat(&crc16(), sn, payload).to_bytes(&mut bytes);
    bytes[..n].to_vec()
}

fn ack(an: u16) -> Vec<u8> {
    wire_ack(an)
}

fn decode(bytes: &[u8]) -> Result<Frame, FrameError> {
    Frame::from_bytes::<BchAckCodec, 16, _>(&crc16(), bytes)
}

/// One fresh-budget receive attempt; a spent budget reads as pending.
fn recv<L: FrameIo<Error = Infallible>>(
    arq: &mut Arq<4, 16, { r::<4>() }, L, Crc16X25, BchAckCodec, ManualTimer>,
    cx: &mut Context<'_>,
) -> Poll<Result<Frame, ArqError<Infallible>>> {
    let mut budget = crate::RX_BUDGET;
    match arq.poll_recv_frame(cx, &mut budget) {
        Ok(crate::Recv::Frame(f)) => Poll::Ready(Ok(f)),
        Ok(crate::Recv::Pending | crate::Recv::Budget) => Poll::Pending,
        Ok(crate::Recv::Eof) => Poll::Ready(Err(ArqError::Closed)),
        Err(e) => Poll::Ready(Err(e)),
    }
}

#[test]
fn complete_data_checks_exact_length_type_and_crc() {
    let bytes = dat(0, &[0x55; 125]);
    assert!(matches!(decode(&bytes), Ok(Frame::Dat(_))));
    for len in [253, 61, 127] {
        let mut damaged = bytes.clone();
        damaged[2] = len;
        assert!(decode(&damaged).is_err());
    }
    let mut damaged = bytes.clone();
    damaged[42] ^= 0x80;
    assert!(matches!(decode(&damaged), Err(FrameError::CrcMismatch(..))));
    let mut damaged = bytes.clone();
    damaged[0] = (damaged[0] & !3) | 1;
    assert!(matches!(
        decode(&damaged),
        Err(FrameError::LengthMismatch(17, _))
    ));
    assert!(decode(&bytes[..3]).is_err());
    let mut damaged = bytes;
    damaged.push(0);
    assert!(decode(&damaged).is_err());
    // Maximum payload and a DAT whose wire length equals the ACK length.
    assert!(matches!(decode(&dat(1, &[0; 251])), Ok(Frame::Dat(_))));
    assert!(matches!(decode(&dat(1, &[0; 11])), Ok(Frame::Dat(_))));
}

#[test]
fn complete_ack_checks_exact_codeword_bch_type_and_crc() {
    let bytes = ack(7);
    assert!(matches!(decode(&bytes), Ok(Frame::Ack(a)) if a.an() == 7));
    let mut correctable = bytes.clone();
    correctable[1] ^= 1;
    assert!(matches!(decode(&correctable), Ok(Frame::Ack(a)) if a.an() == 7));
    assert!(decode(&[TYPE_ACK; 17]).is_err());
    assert!(decode(&bytes[..16]).is_err());
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(decode(&extra).is_err(), "a valid ACK prefix is not a frame");
    // A perfectly valid BCH encoding of the wrong CRC must still be rejected.
    let mut wrong_crc = bytes;
    let crc = AckFrame::new(&crc16(), 7).unwrap().crc() ^ 1;
    wrong_crc[9..].copy_from_slice(&crate::bch::encode(crc).to_le_bytes());
    assert!(decode(&wrong_crc).is_err());
}

struct Frames(VecDeque<Vec<u8>>);

impl FrameIo for Frames {
    type Error = Infallible;
    const FRAMED_RECV: bool = true;

    fn poll_recv(
        &mut self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        assert_eq!(buf.len(), MAX_FRAME);
        let Some(frame) = self.0.pop_front() else {
            return Poll::Pending;
        };
        let copied = frame.len().min(buf.len());
        buf[..copied].copy_from_slice(&frame[..copied]);
        Poll::Ready(Ok(frame.len()))
    }

    fn poll_send(
        &mut self,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<Result<usize, Self::Error>> {
        unreachable!()
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        unreachable!()
    }
}

#[test]
fn invalid_frames_are_not_retained_or_cross_consumed() {
    for len in [253, 61, 127] {
        let mut damaged = dat(0, &[0x55; 125]);
        damaged[2] = len;
        let mut extra_ack = ack(1);
        extra_ack.push(0);
        let frames = VecDeque::from([
            damaged,
            extra_ack,
            dat(1, b"next"),
            dat(0, b"retry"),
            ack(2),
        ]);
        let mut arq = new_arq(Frames(frames));
        let mut cx = noop_cx();
        assert!(
            matches!(recv(&mut arq, &mut cx), Poll::Ready(Ok(Frame::Dat(d)))
            if d.sn() == 1 && d.payload() == b"next")
        );
        assert_eq!(arq.rx_len, 0);
        assert_eq!(arq.channel.0.len(), 2);
        assert!(
            matches!(recv(&mut arq, &mut cx), Poll::Ready(Ok(Frame::Dat(d)))
            if d.sn() == 0 && d.payload() == b"retry")
        );
        assert!(
            matches!(recv(&mut arq, &mut cx), Poll::Ready(Ok(Frame::Ack(a)))
            if a.an() == 2)
        );
        assert!(recv(&mut arq, &mut cx).is_pending());
        assert_eq!(arq.rx_len, 0);
    }
}

#[test]
fn oversized_reported_length_is_discarded_without_accepting_a_prefix() {
    let mut oversized = ack(1);
    oversized.resize(MAX_FRAME + 1, 0);
    let mut arq = new_arq(Frames(VecDeque::from([oversized, dat(0, b"next")])));
    let mut cx = noop_cx();
    assert!(
        matches!(recv(&mut arq, &mut cx), Poll::Ready(Ok(Frame::Dat(d)))
        if d.sn() == 0 && d.payload() == b"next")
    );
    assert_eq!(arq.rx_len, 0);
    assert!(arq.channel.0.is_empty());
}

#[test]
fn corrupt_frame_flood_yields_and_resumes_without_buffering() {
    let mut frames = VecDeque::from(vec![vec![0xff]; 40]);
    frames.push_back(dat(0, b"valid"));
    let mut arq = new_arq(Frames(frames));
    let mut cx = noop_cx();
    assert!(recv(&mut arq, &mut cx).is_pending());
    assert_eq!(arq.channel.0.len(), 9);
    assert_eq!(arq.rx_len, 0);
    assert!(
        matches!(recv(&mut arq, &mut cx), Poll::Ready(Ok(Frame::Dat(d)))
        if d.payload() == b"valid")
    );
}

#[test]
fn corrupted_type_bits_are_discarded_not_reinterpreted() {
    // An ACK whose type bits became DAT, and a DAT whose type bits became ACK.
    let mut as_dat = ack(5);
    as_dat[0] = (as_dat[0] & !3) | TYPE_DAT;
    let mut as_ack = dat(0, b"payload");
    as_ack[0] = (as_ack[0] & !3) | TYPE_ACK;
    assert!(decode(&as_dat).is_err());
    assert!(decode(&as_ack).is_err());
    // The type byte's upper bits are not protected or interpreted.
    let mut upper = ack(5);
    upper[0] ^= 0xFC;
    assert!(matches!(decode(&upper), Ok(Frame::Ack(a)) if a.an() == 5));
}
