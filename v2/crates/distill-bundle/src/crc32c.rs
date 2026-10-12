//! CRC-32C (Castagnoli). Pinned by §6 for both container chunk checksums;
//! §12's artifact format reuses it. Test vector:
//! `crc32c(b"123456789") == 0xE3069283`.
//!
//! Every bundle read checks the CRCs of its whole file, so this runs over
//! every byte a pass parses: the CPU's CRC-32C instruction (SSE4.2 on
//! x86-64, the CRC extension on AArch64) where present, eight table
//! lookups per 8 bytes (slicing-by-8) elsewhere.

/// Reflected Castagnoli polynomial (0x1EDC6F41 bit-reversed).
const POLY: u32 = 0x82F6_3B78;

/// `TABLES[0]` is the byte-at-a-time table; `TABLES[k][i]` is the CRC of
/// byte `i` followed by `k` zero bytes, for slicing-by-8.
const fn build_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        tables[0][i] = crc;
        i += 1;
    }
    let mut k = 1;
    while k < 8 {
        let mut i = 0;
        while i < 256 {
            let previous = tables[k - 1][i];
            tables[k][i] = (previous >> 8) ^ tables[0][(previous & 0xFF) as usize];
            i += 1;
        }
        k += 1;
    }
    tables
}

static TABLES: [[u32; 256]; 8] = build_tables();

/// CRC-32C of `bytes` (init 0xFFFFFFFF, reflected, final xor 0xFFFFFFFF).
pub fn crc32c(bytes: &[u8]) -> u32 {
    !update(!0u32, bytes)
}

fn update(crc: u32, bytes: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("sse4.2") {
            // SAFETY: the CPU supports SSE4.2 (checked above).
            return unsafe { update_sse42(crc, bytes) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") {
            // SAFETY: the CPU supports the CRC extension (checked above).
            return unsafe { update_arm_crc(crc, bytes) };
        }
    }
    update_table(crc, bytes)
}

fn update_table(mut crc: u32, bytes: &[u8]) -> u32 {
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let low = crc ^ u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let high = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        crc = TABLES[7][(low & 0xFF) as usize]
            ^ TABLES[6][((low >> 8) & 0xFF) as usize]
            ^ TABLES[5][((low >> 16) & 0xFF) as usize]
            ^ TABLES[4][(low >> 24) as usize]
            ^ TABLES[3][(high & 0xFF) as usize]
            ^ TABLES[2][((high >> 8) & 0xFF) as usize]
            ^ TABLES[1][((high >> 16) & 0xFF) as usize]
            ^ TABLES[0][(high >> 24) as usize];
    }
    for &b in chunks.remainder() {
        crc = TABLES[0][((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn update_sse42(crc: u32, bytes: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};
    let mut crc = crc as u64;
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let word = u64::from_le_bytes(chunk.try_into().expect("8-byte chunk"));
        crc = _mm_crc32_u64(crc, word);
    }
    let mut crc = crc as u32;
    for &b in chunks.remainder() {
        crc = _mm_crc32_u8(crc, b);
    }
    crc
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn update_arm_crc(crc: u32, bytes: &[u8]) -> u32 {
    use std::arch::aarch64::{__crc32cb, __crc32cd};
    let mut crc = crc;
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let word = u64::from_le_bytes(chunk.try_into().expect("8-byte chunk"));
        crc = __crc32cd(crc, word);
    }
    for &b in chunks.remainder() {
        crc = __crc32cb(crc, b);
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::{crc32c, update_table, TABLES};

    #[test]
    fn check_vector() {
        // The canonical CRC-32C check value.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn empty_input() {
        assert_eq!(crc32c(b""), 0);
    }

    /// The byte-at-a-time definition, which every path must equal.
    fn reference(bytes: &[u8]) -> u32 {
        let mut crc = !0u32;
        for &b in bytes {
            crc = TABLES[0][((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
        }
        !crc
    }

    #[test]
    fn every_path_matches_the_reference_at_every_length() {
        let bytes = (0..1031u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect::<Vec<_>>();
        for len in 0..bytes.len() {
            for start in 0..3.min(len + 1) {
                let slice = &bytes[start..len];
                let expected = reference(slice);
                assert_eq!(crc32c(slice), expected, "len {len} start {start}");
                assert_eq!(!update_table(!0, slice), expected, "table len {len}");
            }
        }
    }
}
