use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ArqError<E> {
    Io(E),
    Framing(FrameError),
    InvalidAck(AckError),
    Timeout,
    Closed,
}

impl<E: fmt::Display> fmt::Display for ArqError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArqError::Io(e) => write!(f, "io error: {}", e),
            ArqError::Framing(error) => write!(f, "arq framing error: {}", error),
            ArqError::InvalidAck(error) => write!(f, "ack error: {}", error),
            ArqError::Timeout => write!(f, "arq ack timeout"),
            ArqError::Closed => write!(f, "arq link closed"),
        }
    }
}

#[cfg(feature = "defmt")]
impl<E: defmt::Format> defmt::Format for ArqError<E> {
    fn format(&self, f: defmt::Formatter<'_>) {
        match self {
            ArqError::Io(e) => defmt::write!(f, "io error: {}", e),
            ArqError::Framing(error) => defmt::write!(f, "arq framing error: {}", error),
            ArqError::InvalidAck(error) => defmt::write!(f, "ack error: {}", error),
            ArqError::Timeout => defmt::write!(f, "arq ack timeout"),
            ArqError::Closed => defmt::write!(f, "arq link closed"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> core::error::Error for ArqError<E> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum FrameError {
    CrcMismatch(u16, u16),
    Invalid,
    InvalidSeq(u16, u16),
    TooShort(usize),
    TooLong(usize),
    LengthMismatch(usize, usize),
    InvalidType(u8),
    TypeMismatch,
}

impl core::error::Error for FrameError {}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::CrcMismatch(recv, calc) => {
                write!(f, "CRC mismatch: recv {} vs. calc {}", recv, calc)
            }
            FrameError::Invalid => write!(f, "invalid frame"),
            FrameError::InvalidSeq(seq, max) => write!(
                f,
                "invalid frame sequence number: {} must be < {}",
                seq, max
            ),
            FrameError::TooShort(usize) => write!(f, "frame too short: {}", usize),
            FrameError::TooLong(usize) => write!(f, "frame length too long: {}", usize),
            FrameError::LengthMismatch(expected, actual) => write!(
                f,
                "frame length mismatch: expected {} vs. actual {}",
                expected, actual
            ),
            FrameError::InvalidType(t) => write!(f, "invalid frame type: {}", t),
            FrameError::TypeMismatch => write!(f, "frame type mismatch"),
        }
    }
}

#[cfg(feature = "defmt")]
impl defmt::Format for FrameError {
    fn format(&self, f: defmt::Formatter<'_>) {
        match self {
            FrameError::CrcMismatch(recv, calc) => {
                defmt::write!(f, "CRC mismatch: recv {} vs. calc {}", recv, calc)
            }
            FrameError::Invalid => defmt::write!(f, "invalid frame"),
            FrameError::InvalidSeq(seq, max) => defmt::write!(
                f,
                "invalid frame sequence number: {} must be < {}",
                seq,
                max
            ),
            FrameError::TooShort(usize) => defmt::write!(f, "frame too short: {}", usize),
            FrameError::TooLong(usize) => defmt::write!(f, "frame length too long: {}", usize),
            FrameError::LengthMismatch(expected, actual) => defmt::write!(
                f,
                "frame length mismatch: expected {} vs. actual {}",
                expected,
                actual
            ),
            FrameError::InvalidType(t) => defmt::write!(f, "invalid frame type: {}", t),
            FrameError::TypeMismatch => defmt::write!(f, "frame type mismatch"),
        }
    }
}

impl<E> From<FrameError> for ArqError<E> {
    fn from(e: FrameError) -> Self {
        ArqError::Framing(e)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AckError {
    DecodeError,
    FrameError(FrameError),
}
impl core::error::Error for AckError {}

impl fmt::Display for AckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AckError::DecodeError => {
                write!(f, "Failed to decode ACK")
            }
            AckError::FrameError(e) => {
                write!(f, "ACK frame error: {}", e)
            }
        }
    }
}

#[cfg(feature = "defmt")]
impl defmt::Format for AckError {
    fn format(&self, f: defmt::Formatter<'_>) {
        match self {
            AckError::DecodeError => {
                defmt::write!(f, "Failed to decode ACK")
            }
            AckError::FrameError(e) => {
                defmt::write!(f, "ACK frame error: {}", e)
            }
        }
    }
}

impl<E> From<AckError> for ArqError<E> {
    fn from(e: AckError) -> Self {
        match e {
            AckError::FrameError(error) => ArqError::Framing(error),
            AckError::DecodeError => ArqError::InvalidAck(e),
        }
    }
}

impl From<FrameError> for AckError {
    fn from(e: FrameError) -> Self {
        AckError::FrameError(e)
    }
}

impl From<AckError> for FrameError {
    fn from(e: AckError) -> Self {
        match e {
            AckError::FrameError(error) => error,
            AckError::DecodeError => FrameError::Invalid,
        }
    }
}
