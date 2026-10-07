/// A CRC-16 algorithm used to protect frames.
pub trait Crc16 {
    /// Computes the CRC-16 over the concatenation of `chunks`.
    fn checksum_concat<const N: usize>(&self, chunks: [&[u8]; N]) -> u16;
}

/// Any closure that computes a CRC-16 over the concatenation of slices can be
/// used as a [`Crc16`].
impl<F: Fn(&[&[u8]]) -> u16> Crc16 for F {
    fn checksum_concat<const N: usize>(&self, chunks: [&[u8]; N]) -> u16 {
        self(&chunks)
    }
}

macro_rules! impl_crc16 {
    ($($ty:ty),*) => {$(
        impl Crc16 for $ty {
            fn checksum_concat<const N: usize>(&self, chunks: [&[u8]; N]) -> u16 {
                let mut d = self.digest();
                for chunk in chunks {
                    d.update(chunk);
                }
                d.finalize()
            }
        }
    )*};
}

impl_crc16!(crc::Crc<u16>, crc::Crc<u16, crc::NoTable>, crc::Crc<u16, crc::Table<16>>);

#[cfg(test)]
mod tests {
    use super::Crc16;

    fn check<C: Crc16>(crc: &C) {
        // CRC-16/X-25 check value over the concatenated chunks.
        assert_eq!(crc.checksum_concat([b"123456789"]), 0x906E);
        assert_eq!(crc.checksum_concat([b"1234", b"", b"56789"]), 0x906E);
        assert_eq!(crc.checksum_concat([b"", b"123456789", b""]), 0x906E);
        assert_eq!(crc.checksum_concat([b"1", b"2", b"3", b"456789"]), 0x906E);
    }

    #[test]
    fn checksum_concat_spans_chunks_for_every_table_size() {
        check(&crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC));
        check(&crc::Crc::<u16, crc::NoTable>::new(&crc::CRC_16_IBM_SDLC));
        check(&crc::Crc::<u16, crc::Table<16>>::new(&crc::CRC_16_IBM_SDLC));
    }
}
