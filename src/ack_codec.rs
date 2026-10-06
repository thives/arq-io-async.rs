use crate::bch::{decode, encode};
use crate::crc::Crc16;
use crate::error::AckError;
use crate::frame::AckFrame;

/// The most bit errors accepted in either half of a codeword: the number the
/// BCH code is guaranteed to correct.
const MAX_CORRECTIONS: usize = 11;

/// Encodes and decodes the error-correcting codeword that carries ACK frames.
///
/// The codeword is `ACK_CODEWORD_LEN` bytes long.
pub trait AckCodec<const ACK_CODEWORD_LEN: usize> {
    /// Encodes `frame` into the codeword.
    fn encode_ack(frame: AckFrame) -> Result<[u8; ACK_CODEWORD_LEN], AckError>;
    /// Decodes the codeword into an [`AckFrame`], validating the frame with
    /// `crc`.
    ///
    /// Implementations should reject a codeword that cannot be decoded
    /// reliably, rather than return a frame that needed more corrections than
    /// the code guarantees.
    fn decode_ack<C: Crc16>(
        crc: &C,
        codeword: &[u8; ACK_CODEWORD_LEN],
    ) -> Result<AckFrame, AckError>;
}

/// A BCH error-correcting [`AckCodec`] for 16-byte codewords.
///
/// The codeword carries two 8-byte BCH codewords, each protecting a 16-bit
/// value: the frame's packet identifier in the first half and the frame CRC
/// in the second half.
///
/// Up to 11 bit errors in each half are corrected. Decoding fails with
/// [`AckError::DecodeError`] when a half cannot be decoded or needed more
/// than 11 corrections, and with [`AckError::FrameError`] when the
/// reconstructed frame fails validation.
///
/// Beyond 11 bit errors a half can still decode to a different, valid
/// codeword. The CRC check makes accepting such an ACK unlikely, but it is
/// additional validation, not a guarantee.
#[derive(Debug, Default, Clone, Copy)]
pub struct BchAckCodec;

/// Implements [`AckCodec`] for 16-byte codewords.
impl AckCodec<16> for BchAckCodec {
    fn encode_ack(frame: AckFrame) -> Result<[u8; 16], AckError> {
        let mut result = [0u8; 16];
        result[..8].copy_from_slice(encode(frame.pkt_id()).to_le_bytes().as_slice());
        result[8..].copy_from_slice(encode(frame.crc()).to_le_bytes().as_slice());
        Ok(result)
    }

    fn decode_ack<C: Crc16>(crc: &C, codeword: &[u8; 16]) -> Result<AckFrame, AckError> {
        let half = |bytes: &[u8]| {
            decode(u64::from_le_bytes(bytes.try_into().unwrap()))
                .filter(|&(_, corrected)| corrected <= MAX_CORRECTIONS)
                .map(|(word, _)| word)
                .ok_or(AckError::DecodeError)
        };
        let pkt_id = half(&codeword[..8])?;
        let crc2 = half(&codeword[8..])?;
        Ok(AckFrame::from_parts(crc, pkt_id, crc2)?)
    }
}
