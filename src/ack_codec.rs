use crate::bch::{decode, encode};
use crate::crc::Crc16;
use crate::error::AckError;
use crate::frame::AckFrame;

pub trait AckCodec<const ACK_CODEWORD_LEN: usize> {
    fn encode_ack(frame: AckFrame) -> Result<[u8; ACK_CODEWORD_LEN], AckError>;
    fn decode_ack<C: Crc16>(
        crc: &C,
        codeword: &[u8; ACK_CODEWORD_LEN],
    ) -> Result<AckFrame, AckError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct CodeRsAckCodec;

impl AckCodec<16> for CodeRsAckCodec {
    fn encode_ack(frame: AckFrame) -> Result<[u8; 16], AckError> {
        let mut result = [0u8; 16];
        result[..8].copy_from_slice(encode(frame.pkt_id()).to_le_bytes().as_slice());
        result[8..].copy_from_slice(encode(frame.crc()).to_le_bytes().as_slice());
        Ok(result)
    }

    fn decode_ack<C: Crc16>(crc: &C, codeword: &[u8; 16]) -> Result<AckFrame, AckError> {
        let (pkt_id, _) = decode(u64::from_le_bytes(codeword[..8].try_into().unwrap()))
            .ok_or(AckError::DecodeError)?;
        let (crc2, _) = decode(u64::from_le_bytes(codeword[8..].try_into().unwrap()))
            .ok_or(AckError::DecodeError)?;
        Ok(AckFrame::from_parts(crc, pkt_id, crc2)?)
    }
}
