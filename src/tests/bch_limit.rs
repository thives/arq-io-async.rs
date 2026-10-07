use super::*;
use crate::ack_codec::AckCodec;
use crate::bch::{decode, encode};
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
    arq.channel.rx.push(wire);
    assert!(arq.poll_op(&mut cx, &mut Op::Flush).is_pending());
    assert!(!arq.failed);
    assert_eq!((arq.sb, arq.w), (0, 1), "frame must stay outstanding");
}

#[test]
fn crc_mismatch_still_fails_validation() {
    let mut cw = codeword(5);
    cw[8..].copy_from_slice(&codeword(6)[8..]);
    assert!(matches!(decode_cw(&cw), Err(AckError::FrameError(_))));
}

fn random_bits(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    *seed
}

#[test]
fn arbitrary_wire_input_does_not_panic() {
    let mut seed = 0x8f3a_26d1_5c79_b042;
    let mut rejected = 0;
    for _ in 0..16384 {
        let bits = random_bits(&mut seed);
        let result = decode(bits);
        assert_eq!(decode(bits ^ 1), result, "unprotected bit in {bits:#018x}");
        if let Some((word, count)) = result {
            assert_eq!(
                ((bits ^ encode(word)) >> 1).count_ones() as usize,
                count,
                "correction count for {bits:#018x}"
            );
        } else {
            rejected += 1;
        }
        let mut cw = [0; 16];
        cw[..8].copy_from_slice(&bits.to_le_bytes());
        cw[8..].copy_from_slice(&random_bits(&mut seed).to_le_bytes());
        let _ = decode_cw(&cw);
    }
    assert!(rejected > 0);
}

#[test]
fn every_single_bit_error_is_corrected() {
    for word in [0, 1, 0x1234, 0x8000, 0xa55a, u16::MAX] {
        for bit in 0..64 {
            assert_eq!(
                decode(encode(word) ^ (1 << bit)),
                Some((word, usize::from(bit != 0))),
                "word {word:#06x}, bit {bit}"
            );
        }
    }
}

#[test]
fn random_errors_through_correction_radius_are_corrected() {
    let mut seed = 0x293b_d605_a17e_4fc8;
    for count in 0..=11 {
        for _ in 0..64 {
            let word = random_bits(&mut seed) as u16;
            let mut mask = 0u64;
            while mask.count_ones() < count {
                mask |= 1 << (1 + random_bits(&mut seed) % 63);
            }
            for unprotected in [0, 1] {
                assert_eq!(
                    decode(encode(word) ^ mask ^ unprotected),
                    Some((word, count as usize)),
                    "word {word:#06x}, mask {mask:#018x}, bit 0 = {unprotected}"
                );
            }
        }
    }
}

#[test]
fn beyond_radius_correction_is_preserved_for_caller_validation() {
    for (word, bits) in [(41909, SPEC_HALF), (5, TWELVE_ERRORS_SEQ1)] {
        assert_eq!(decode(bits), Some((word, 12)));
    }
}
