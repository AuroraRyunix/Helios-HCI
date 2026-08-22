//! CRC32C (Castagnoli).
//!
//! Software table implementation. Detection is the job here, not authentication, and the
//! footer carries an algorithm byte precisely so that "something stronger" later is a
//! value change rather than a format migration. If this ever shows up in a profile, the
//! replacement is the SSE4.2 `crc32` instruction, which computes the same values.

const POLY: u32 = 0x82F6_3B78;

static TABLE: [[u32; 256]; 8] = build_tables();

const fn build_tables() -> [[u32; 256]; 8] {
    let mut t = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut k = 0;
        while k < 8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ POLY } else { crc >> 1 };
            k += 1;
        }
        t[0][i] = crc;
        i += 1;
    }
    // Slicing-by-8: each further table folds one more byte of lookahead.
    let mut s = 1;
    while s < 8 {
        let mut j = 0;
        while j < 256 {
            let prev = t[s - 1][j];
            t[s][j] = (prev >> 8) ^ t[0][(prev & 0xFF) as usize];
            j += 1;
        }
        s += 1;
    }
    t
}

/// CRC32C of `data`, seeded with `seed` so callers can checksum a header and its payload
/// as one value without concatenating them into a temporary buffer.
pub fn crc32c(seed: u32, data: &[u8]) -> u32 {
    let mut crc = !seed;
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks {
        crc ^= u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        crc = TABLE[7][(crc & 0xFF) as usize]
            ^ TABLE[6][((crc >> 8) & 0xFF) as usize]
            ^ TABLE[5][((crc >> 16) & 0xFF) as usize]
            ^ TABLE[4][(crc >> 24) as usize]
            ^ TABLE[3][c[4] as usize]
            ^ TABLE[2][c[5] as usize]
            ^ TABLE[1][c[6] as usize]
            ^ TABLE[0][c[7] as usize];
    }
    for b in chunks.remainder() {
        crc = (crc >> 8) ^ TABLE[0][((crc ^ *b as u32) & 0xFF) as usize];
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::crc32c;

    #[test]
    fn known_vectors() {
        // The canonical CRC32C check value: "123456789" -> 0xE3069283.
        assert_eq!(crc32c(0, b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(0, b""), 0);
        // Seeding must equal concatenation, or header+payload checksums are wrong.
        let split = crc32c(crc32c(0, b"12345"), b"6789");
        assert_eq!(split, 0xE306_9283);
    }

    #[test]
    fn detects_single_bit_flips() {
        let data = vec![0xA5u8; 4096];
        let good = crc32c(0, &data);
        for bit in [0usize, 1, 17, 4095 * 8 + 7] {
            let mut bad = data.clone();
            bad[bit / 8] ^= 1 << (bit % 8);
            assert_ne!(good, crc32c(0, &bad), "bit {} went undetected", bit);
        }
    }
}
