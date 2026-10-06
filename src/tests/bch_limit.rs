use super::*;
use crate::ack_codec::AckCodec;
use crate::error::AckError;

const SPEC_SEQ: u16 = 10477;
const SPEC_HALF: u64 = 0x77b539c7b804fc85;
/// First half of the ACK for sequence 1 with twelve bit errors, which the
/// decoder still maps back to the original word.
const TWELVE_ERRORS_SEQ1: u64 = 0x24c5ab4589e3df5;

fn codeword(an: u16) -> [u8; 16] {
    let wire = wire_ack(an);
    wire[1..].try_into().unwrap()
}

fn flip(cw: &mut [u8; 16], half: usize, count: usize) {
    let mut bits = u64::from_le_bytes(cw[half * 8..half * 8 + 8].try_into().unwrap());
    // Bit 0 is not part of the protected word.
    for i in 0..count {
        bits ^= 1 << (1 + i * 5);
    }
    cw[half * 8..half * 8 + 8].copy_from_slice(&bits.to_le_bytes());
}

fn decode_cw(cw: &[u8; 16]) -> Result<crate::frame::AckFrame, AckError> {
    BchAckCodec::decode_ack(&crc16(), cw)
}

#[test]
fn unmodified_ack_roundtrips() {
    for an in [0, 1, 5, SPEC_SEQ, 16383] {
        assert_eq!(decode_cw(&codeword(an)).unwrap().an(), an);
    }
}

#[test]
fn up_to_eleven_errors_per_half_are_corrected() {
    for count in 1..=11 {
        for halves in [[true, false], [false, true], [true, true]] {
            let mut cw = codeword(SPEC_SEQ);
            for (half, hit) in halves.into_iter().enumerate() {
                if hit {
                    flip(&mut cw, half, count);
                }
            }
            assert_eq!(
                decode_cw(&cw).unwrap().an(),
                SPEC_SEQ,
                "{count} errors in {halves:?}"
            );
        }
    }
}

#[test]
fn twelve_corrections_in_first_half_are_rejected() {
    let mut cw = codeword(SPEC_SEQ);
    cw[..8].copy_from_slice(&SPEC_HALF.to_le_bytes());
    assert_eq!(decode_cw(&cw).unwrap_err(), AckError::DecodeError);
    let mut wire = wire_ack(SPEC_SEQ);
    wire[1..9].copy_from_slice(&SPEC_HALF.to_le_bytes());
    assert!(decode_frame(&wire).is_err());
}

#[test]
fn engine_fails_on_ack_beyond_the_correction_limit() {
    let mut arq = make_arq();
    let mut cx = noop_cx();
    assert!(matches!(
        arq.poll_op(&mut cx, &mut Op::Write { buf: b"abc" }),
        Poll::Ready(Ok(OpOut::Write(3)))
    ));
    assert!(arq.poll_op(&mut cx, &mut Op::Flush).is_pending());
    assert_eq!(arq.w, 1);

    let mut wire = wire_ack(1);
    wire[1..9].copy_from_slice(&TWELVE_ERRORS_SEQ1.to_le_bytes());
    arq.channel.rx = wire;
    assert!(matches!(
        arq.poll_op(&mut cx, &mut Op::Flush),
        Poll::Ready(Err(ArqError::Framing(_) | ArqError::InvalidAck(_)))
    ));
    assert_eq!((arq.sb, arq.w), (0, 1), "frame must stay outstanding");
}

#[test]
fn crc_mismatch_still_fails_validation() {
    let mut cw = codeword(5);
    cw[8..].copy_from_slice(&codeword(6)[8..]);
    assert!(matches!(decode_cw(&cw), Err(AckError::FrameError(_))));
}
