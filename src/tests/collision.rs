use super::*;
use crate::frame::{MAX_PAYLOAD, TYPE_DAT};
use crate::{AckCodec, Crc16, MAX_FRAME, MAX_SEQ};

/// A valid DAT frame of `len` payload bytes whose first 16 bytes BCH-decode
/// as a valid ACK codeword.
fn colliding_dat(len: usize) -> Vec<u8> {
    let crc = crc16();
    for an in 0..MAX_SEQ {
        let cw = BchAckCodec::encode_ack(AckFrame::new(&crc, an).unwrap()).unwrap();
        let mut bytes = vec![0u8; 5 + len];
        bytes[0] = (cw[0] & !0b11) | TYPE_DAT;
        bytes[1] = cw[1];
        bytes[2] = len as u8;
        let n = (16 - 5).min(len);
        bytes[5..5 + n].copy_from_slice(&cw[5..5 + n]);
        let v = crc.checksum_concat([&bytes[0..3], &bytes[5..]]);
        bytes[3..5].copy_from_slice(&v.to_le_bytes());
        let prefix: [u8; 16] = bytes[..16].try_into().unwrap();
        if BchAckCodec::decode_ack(&crc, &prefix).is_ok() {
            assert!(DatFrame::from_bytes(&crc, &bytes).is_ok());
            return bytes;
        }
    }
    panic!("no colliding frame of length {len}");
}

fn assert_dat(frame: Result<Frame, FrameError>, bytes: &[u8]) {
    match frame {
        Ok(Frame::Dat(d)) => assert_eq!(d.payload(), &bytes[5..]),
        other => panic!("expected the data frame, got {other:?}"),
    }
}

#[test]
fn colliding_data_frames_decode_as_data() {
    for len in [11, 40] {
        let bytes = colliding_dat(len);
        assert_eq!(wire_len(&bytes).unwrap(), bytes.len(), "stream boundary");
        assert_dat(decode_frame(&bytes), &bytes);
    }
}

#[test]
fn colliding_data_frames_survive_stream_parsing() {
    let a = colliding_dat(11);
    let b = colliding_dat(40);
    let mut stream = wire_ack(3);
    stream.extend(&a);
    stream.extend(wire_ack(MAX_SEQ - 1));
    stream.extend(&b);
    stream.extend(wire_fin(9, b"end"));
    let frames = parse_stream(&stream);
    assert_eq!(frames.len(), 5);
    assert!(matches!(frames[0], Frame::Ack(x) if x.an() == 3));
    assert!(matches!(frames[1], Frame::Dat(d) if d.payload() == &a[5..]));
    assert!(matches!(frames[2], Frame::Ack(x) if x.an() == MAX_SEQ - 1));
    assert!(matches!(frames[3], Frame::Dat(d) if d.payload() == &b[5..]));
    assert!(matches!(frames[4], Frame::Fin(d) if d.payload() == b"end"));
}

#[test]
fn colliding_data_frames_are_delivered() {
    let a = colliding_dat(11);
    let b = colliding_dat(40);
    for bytes in [a, b] {
        let sn = DatFrame::from_bytes(&crc16(), &bytes).unwrap().sn();
        let mut arq = make_arq();
        arq.set_seq(sn);
        arq.channel.rx.extend(&bytes);
        arq.channel.rx.extend(wire_fin((sn + 1) % MAX_SEQ, b""));
        let mut got = Vec::new();
        let mut cx = noop_cx();
        for _ in 0..20 {
            let mut buf = [0u8; 64];
            let mut op = Op::Read { buf: &mut buf };
            match arq.poll_op(&mut cx, &mut op) {
                Poll::Ready(Ok(OpOut::Read(0))) => break,
                Poll::Ready(Ok(OpOut::Read(n))) => got.extend_from_slice(&buf[..n]),
                Poll::Ready(other) => panic!("unexpected read: {other:?}"),
                Poll::Pending => {}
            }
        }
        assert_eq!(got, &bytes[5..]);
    }
}

#[test]
fn every_ack_sequence_roundtrips() {
    let crc = crc16();
    let mut buf = [0u8; MAX_FRAME];
    for an in 0..MAX_SEQ {
        let n = Frame::Ack(AckFrame::new(&crc, an).unwrap())
            .to_bytes::<BchAckCodec, 16>(&mut buf)
            .unwrap();
        assert_eq!(wire_len(&buf[..n]).unwrap(), n);
        match decode_frame(&buf[..n]) {
            Ok(Frame::Ack(a)) => assert_eq!(a.an(), an),
            other => panic!("ack {an}: {other:?}"),
        }
    }
}

#[test]
fn every_frame_type_roundtrips() {
    for (bytes, sn, payload) in [
        (wire_dat(MAX_SEQ - 1, b"a"), MAX_SEQ - 1, &b"a"[..]),
        (wire_dat_ack_req(0, b""), 0, &b""[..]),
        (wire_fin(5, &[7; MAX_PAYLOAD]), 5, &[7; MAX_PAYLOAD][..]),
    ] {
        assert_eq!(wire_len(&bytes).unwrap(), bytes.len());
        let frame = decode_frame(&bytes).unwrap();
        let (Frame::Dat(d) | Frame::DatAckReq(d) | Frame::Fin(d)) = frame else {
            panic!("unexpected ack");
        };
        assert_eq!(d.sn(), sn);
        assert_eq!(d.payload(), payload);
        assert_eq!(encode_frame(&frame), bytes);
    }
}

#[test]
fn stream_parser_handles_byte_fragments() {
    let a = colliding_dat(11);
    let sn = DatFrame::from_bytes(&crc16(), &a).unwrap().sn();
    let mut link = TrickleLink::new();
    link.push(wire_ack(0));
    link.push(a.clone());
    link.push(wire_ack(0));
    link.push(wire_fin((sn + 1) % MAX_SEQ, b"!"));
    let mut arq = new_arq(link);
    arq.set_seq(sn);
    let mut cx = noop_cx();
    let mut got = Vec::new();
    for _ in 0..1000 {
        let mut buf = [0u8; 64];
        let mut op = Op::Read { buf: &mut buf };
        match arq.poll_op(&mut cx, &mut op) {
            Poll::Ready(Ok(OpOut::Read(0))) => break,
            Poll::Ready(Ok(OpOut::Read(n))) => got.extend_from_slice(&buf[..n]),
            Poll::Ready(other) => panic!("unexpected read: {other:?}"),
            Poll::Pending => {}
        }
    }
    let mut expect = a[5..].to_vec();
    expect.push(b'!');
    assert_eq!(got, expect);
}
