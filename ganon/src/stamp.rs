//! Self-describing blocks.
//!
//! Every block Ganon writes says where it believes it lives and which generation it
//! belongs to. That is what turns three different silent-corruption classes into loud
//! ones: a misdirected write lands at an offset that disagrees with the stamp, a stale
//! read carries a generation older than the newest acknowledged one, and a torn or
//! rotted block fails its own checksum.
//!
//! ```text
//! magic u32 | vdisk_tag u64 | offset u64 | generation u64 | wall_ns u64 |
//! payload_crc u32 | stamp_crc u32 | filler...
//! ```
//!
//! The CRC32C here is a second, independent implementation of the same algorithm Sidon
//! uses. Duplication is the point -- see the note in Cargo.toml.

pub const BLOCK: usize = 4096;
pub const MAGIC: u32 = 0x474E_4F4E; // "GNON"
const HEAD: usize = 44;

/// CRC32C, bitwise. Slower than a table and irrelevant at this scale, but it is
/// transparently the algorithm rather than a table someone has to trust.
pub fn crc32c(seed: u32, data: &[u8]) -> u32 {
    let mut crc = !seed;
    for b in data {
        crc ^= *b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ 0x82F6_3B78 } else { crc >> 1 };
        }
    }
    !crc
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamp {
    pub vdisk_tag: u64,
    pub offset: u64,
    pub generation: u64,
    pub wall_ns: u64,
}

/// What a block turned out to be when read back.
#[derive(Debug)]
pub enum Read {
    /// Never written by anyone: all zeroes, which is what a sparse disk owes us.
    Zeroed,
    /// A well-formed stamp.
    Valid(Stamp),
    /// Something that is neither. The string is evidence, not a category.
    Damaged(String),
}

pub fn build(vdisk_tag: u64, offset: u64, generation: u64, wall_ns: u64) -> Vec<u8> {
    let mut b = vec![0u8; BLOCK];
    b[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    b[4..12].copy_from_slice(&vdisk_tag.to_le_bytes());
    b[12..20].copy_from_slice(&offset.to_le_bytes());
    b[20..28].copy_from_slice(&generation.to_le_bytes());
    b[28..36].copy_from_slice(&wall_ns.to_le_bytes());

    // Fill the remainder deterministically from the stamp, so a block that is byte-copied
    // from a *different* generation cannot pass by carrying the right header alone.
    let mut x = generation
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ offset.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    for chunk in b[HEAD..].chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let bytes = x.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }

    let payload_crc = crc32c(0, &b[HEAD..]);
    b[36..40].copy_from_slice(&payload_crc.to_le_bytes());
    let stamp_crc = crc32c(0, &b[0..40]);
    b[40..44].copy_from_slice(&stamp_crc.to_le_bytes());
    b
}

pub fn parse(block: &[u8], expect_offset: u64) -> Read {
    if block.len() < BLOCK {
        return Read::Damaged(format!("short block: {} bytes", block.len()));
    }
    if block.iter().all(|b| *b == 0) {
        return Read::Zeroed;
    }
    let magic = u32::from_le_bytes(block[0..4].try_into().unwrap());
    if magic != MAGIC {
        return Read::Damaged(format!(
            "magic {magic:#010x} is not a Ganon block; the range holds something else"
        ));
    }
    let want_stamp = u32::from_le_bytes(block[40..44].try_into().unwrap());
    if crc32c(0, &block[0..40]) != want_stamp {
        return Read::Damaged("stamp header failed its own checksum".to_string());
    }
    let want_payload = u32::from_le_bytes(block[36..40].try_into().unwrap());
    if crc32c(0, &block[HEAD..]) != want_payload {
        return Read::Damaged("block payload failed its checksum (torn or rotted)".to_string());
    }
    let s = Stamp {
        vdisk_tag: u64::from_le_bytes(block[4..12].try_into().unwrap()),
        offset: u64::from_le_bytes(block[12..20].try_into().unwrap()),
        generation: u64::from_le_bytes(block[20..28].try_into().unwrap()),
        wall_ns: u64::from_le_bytes(block[28..36].try_into().unwrap()),
    };
    if s.offset != expect_offset {
        return Read::Damaged(format!(
            "misdirected: block read at {expect_offset} says it lives at {}",
            s.offset
        ));
    }
    // Regenerating the filler catches a whole-block copy of a different generation whose
    // header was then rewritten -- the checksums would agree, the contents would not.
    let expected = build(s.vdisk_tag, s.offset, s.generation, s.wall_ns);
    if expected[HEAD..] != block[HEAD..] {
        return Read::Damaged(format!(
            "block at {expect_offset} generation {} has contents from some other block",
            s.generation
        ));
    }
    Read::Valid(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_matches_the_canonical_check_value() {
        assert_eq!(crc32c(0, b"123456789"), 0xE306_9283);
    }

    #[test]
    fn a_built_block_parses_back() {
        let b = build(7, 8192, 3, 123);
        match parse(&b, 8192) {
            Read::Valid(s) => {
                assert_eq!(s.offset, 8192);
                assert_eq!(s.generation, 3);
                assert_eq!(s.vdisk_tag, 7);
            }
            other => panic!("expected Valid, got {other:?}"),
        }
    }

    #[test]
    fn zeroes_are_unwritten_not_damaged() {
        assert!(matches!(parse(&vec![0u8; BLOCK], 0), Read::Zeroed));
    }

    #[test]
    fn reading_a_block_at_the_wrong_offset_is_a_misdirect() {
        let b = build(7, 8192, 3, 123);
        match parse(&b, 4096) {
            Read::Damaged(m) => assert!(m.contains("misdirected"), "{m}"),
            other => panic!("expected Damaged, got {other:?}"),
        }
    }

    #[test]
    fn a_flipped_payload_bit_is_damage() {
        let mut b = build(7, 0, 1, 0);
        b[3000] ^= 0x01;
        match parse(&b, 0) {
            Read::Damaged(m) => assert!(m.contains("payload"), "{m}"),
            other => panic!("expected Damaged, got {other:?}"),
        }
    }

    #[test]
    fn a_rewritten_header_over_old_contents_is_caught() {
        // The attack a checksum alone misses: take generation 1's block, restamp it as
        // generation 2 with correct checksums. Only the regenerated filler catches it.
        let old = build(7, 0, 1, 0);
        let mut forged = old.clone();
        forged[20..28].copy_from_slice(&2u64.to_le_bytes());
        let payload_crc = crc32c(0, &forged[HEAD..]);
        forged[36..40].copy_from_slice(&payload_crc.to_le_bytes());
        let stamp_crc = crc32c(0, &forged[0..40]);
        forged[40..44].copy_from_slice(&stamp_crc.to_le_bytes());
        match parse(&forged, 0) {
            Read::Damaged(m) => assert!(m.contains("some other block"), "{m}"),
            other => panic!("expected Damaged, got {other:?}"),
        }
    }

    #[test]
    fn generations_produce_different_contents() {
        assert_ne!(build(1, 0, 1, 0), build(1, 0, 2, 0));
        assert_ne!(build(1, 0, 1, 0), build(1, 4096, 1, 0));
    }
}
