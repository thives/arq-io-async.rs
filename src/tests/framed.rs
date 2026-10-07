use std::collections::VecDeque;

use super::*;
use crate::MAX_FRAME;
use crate::frame::{TYPE_ACK, TYPE_DAT};

/// One fresh-budget receive batch.
fn recv<L: Transport<Error = Infallible>>(
    arq: &mut Arq<4, 16, L, Crc16X25, BchAckCodec, ManualTimer>,
) -> crate::arq::RecvBatch {
    arq.service_recv(&mut noop_cx()).unwrap()
}

#[test]
fn complete_data_checks_exact_length_type_and_crc() {
    let bytes = wire_dat(0, &[0x55; 125]);
    let mut direct = [0; MAX_FRAME];
    let n = DatFrame::new_dat(&crc16(), 0, &[0x55; 125]).to_bytes(&mut direct);
    assert_eq!(
        &direct[..n],
        &bytes[..],
        "DatFrame and Frame encodings agree"
    );
    assert!(matches!(decode_frame(&bytes), Ok(Frame::Dat(_))));
    for len in [253, 61, 127] {
        let mut damaged = bytes.clone();
        damaged[2] = len;
        assert!(decode_frame(&damaged).is_err());
    }
    let mut damaged = bytes.clone();
    damaged[42] ^= 0x80;
    assert!(matches!(
        decode_frame(&damaged),
        Err(FrameError::CrcMismatch(..))
    ));
    let mut damaged = bytes.clone();
    damaged[0] = (damaged[0] & !3) | 1;
    assert!(matches!(
        decode_frame(&damaged),
        Err(FrameError::LengthMismatch(17, _))
    ));
    assert!(decode_frame(&bytes[..3]).is_err());
    let mut damaged = bytes;
    damaged.push(0);
    assert!(decode_frame(&damaged).is_err());
    // Maximum payload and a DAT whose wire length equals the ACK length.
    assert!(matches!(
        decode_frame(&wire_dat(1, &[0; 251])),
        Ok(Frame::Dat(_))
    ));
    assert!(matches!(
        decode_frame(&wire_dat(1, &[0; 11])),
        Ok(Frame::Dat(_))
    ));
}

#[test]
fn length_mismatch_reports_total_wire_lengths() {
    let dat = wire_dat(0, &[1; 10]);
    assert_eq!(dat.len(), 15);
    let mut short = dat.clone();
    short.pop();
    assert!(matches!(
        decode_frame(&short),
        Err(FrameError::LengthMismatch(15, 14))
    ));
    let mut long = dat;
    long.push(0);
    assert!(matches!(
        decode_frame(&long),
        Err(FrameError::LengthMismatch(15, 16))
    ));
    let ack = wire_ack(7);
    assert_eq!(ack.len(), 17);
    assert!(matches!(
        decode_frame(&ack[..16]),
        Err(FrameError::LengthMismatch(17, 16))
    ));
    let mut long = ack;
    long.push(0);
    assert!(matches!(
        decode_frame(&long),
        Err(FrameError::LengthMismatch(17, 18))
    ));
    let mut oversized = vec![0; 256];
    oversized[0] = TYPE_DAT;
    oversized[2] = 255;
    assert!(matches!(
        decode_frame(&oversized),
        Err(FrameError::LengthMismatch(260, 256))
    ));
}

#[test]
fn complete_ack_checks_exact_codeword_bch_type_and_crc() {
    let bytes = wire_ack(7);
    assert!(matches!(decode_frame(&bytes), Ok(Frame::Ack(a)) if a.an() == 7));
    let mut correctable = bytes.clone();
    correctable[1] ^= 1;
    assert!(matches!(decode_frame(&correctable), Ok(Frame::Ack(a)) if a.an() == 7));
    assert!(decode_frame(&[TYPE_ACK; 17]).is_err());
    assert!(decode_frame(&bytes[..16]).is_err());
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(
        decode_frame(&extra).is_err(),
        "a valid ACK prefix is not a frame"
    );
    // A perfectly valid BCH encoding of the wrong CRC must still be rejected.
    let mut wrong_crc = bytes;
    let crc = AckFrame::new(&crc16(), 7).unwrap().crc() ^ 1;
    wrong_crc[9..].copy_from_slice(&crate::bch::encode(crc).to_le_bytes());
    assert!(decode_frame(&wrong_crc).is_err());
}

struct Frames(VecDeque<Vec<u8>>);

impl Transport for Frames {
    type Error = Infallible;

    fn poll_read(
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

    fn poll_write(
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
        let mut damaged = wire_dat(0, &[0x55; 125]);
        damaged[2] = len;
        let mut extra_ack = wire_ack(1);
        extra_ack.push(0);
        let frames = VecDeque::from([
            damaged,
            extra_ack,
            wire_dat(1, b"next"),
            wire_dat(0, b"retry"),
            wire_ack(2),
        ]);
        let mut arq = new_arq(Frames(frames));
        let batch = recv(&mut arq);
        assert_eq!(
            (batch.frames, batch.more),
            (3, false),
            "damaged length {len}"
        );
        assert!(arq.channel.0.is_empty(), "damaged length {len}");
        assert_eq!(arq.rn, 2, "damaged length {len}");
        let mut got = [0u8; 16];
        let mut op = Op::Read { buf: &mut got };
        assert!(
            matches!(arq.service_op(&mut op), Some(OpOut::Read(9))),
            "damaged length {len}"
        );
        assert_eq!(&got[..9], b"retrynext", "damaged length {len}");
    }
}

#[test]
fn oversized_reported_length_is_discarded_without_accepting_a_prefix() {
    let mut oversized = wire_ack(1);
    oversized.resize(MAX_FRAME + 1, 0);
    let mut arq = new_arq(Frames(VecDeque::from([oversized, wire_dat(0, b"next")])));
    let batch = recv(&mut arq);
    assert_eq!((batch.frames, batch.more), (1, false));
    assert!(arq.channel.0.is_empty());
    assert_eq!(arq.rn, 1);
    assert_eq!(arq.read_buf.len(), 4);
}

#[test]
fn corrupt_frame_flood_yields_and_resumes_without_buffering() {
    let mut frames = VecDeque::from(vec![vec![0xff]; 40]);
    frames.push_back(wire_dat(0, b"valid"));
    let mut arq = new_arq(Frames(frames));
    let batch = recv(&mut arq);
    assert_eq!((batch.frames, batch.more), (0, true));
    assert_eq!(arq.channel.0.len(), 9);
    assert!(arq.read_buf.is_empty());
    let batch = recv(&mut arq);
    assert_eq!((batch.frames, batch.more), (1, false));
    assert_eq!(arq.read_buf.len(), 5);
}

#[test]
fn corrupted_type_bits_are_discarded_not_reinterpreted() {
    // An ACK whose type bits became DAT, and a DAT whose type bits became ACK.
    let mut as_dat = wire_ack(5);
    as_dat[0] = (as_dat[0] & !3) | TYPE_DAT;
    let mut as_ack = wire_dat(0, b"payload");
    as_ack[0] = (as_ack[0] & !3) | TYPE_ACK;
    assert!(decode_frame(&as_dat).is_err());
    assert!(decode_frame(&as_ack).is_err());
    // The type byte's upper bits are not protected or interpreted.
    let mut upper = wire_ack(5);
    upper[0] ^= 0xFC;
    assert!(matches!(decode_frame(&upper), Ok(Frame::Ack(a)) if a.an() == 5));
}
