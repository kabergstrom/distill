//! Streaming copy (doc 22 §4.4): copy into a destination the CPU will not
//! read soon — a receive sink, an upload staging buffer — with
//! non-temporal stores, which write whole cache lines without first
//! reading them (no read-for-ownership) and without evicting the cache.
//!
//! - x86_64: AVX2 `vmovntdq` when the CPU has it (runtime detection,
//!   cached by std), SSE2 `movntdq` otherwise (always present on x86_64).
//!   The destination head up to 32-byte alignment and the tail are copied
//!   with ordinary stores, as is a copy shorter than [`STREAM_MIN`].
//! - Other architectures: an ordinary copy.
//!
//! Non-temporal stores are weakly ordered. Call [`publish`] once after the
//! last streaming copy into a buffer and before handing it off: to another
//! thread, or to the GPU through a queue submit.

/// Below this many bytes [`copy_streaming`] copies with ordinary stores:
/// the head/tail handling and the fence outweigh the saved line reads.
pub const STREAM_MIN: usize = 4096;

/// Copy `src` into `dst` (equal lengths) with non-temporal stores where
/// the architecture has them. Byte-identical to `dst.copy_from_slice(src)`.
/// Follow the last copy into a buffer with [`publish`] before handing the
/// buffer off.
///
/// # Panics
/// When the lengths differ.
#[inline]
pub fn copy_streaming(dst: &mut [u8], src: &[u8]) {
    assert_eq!(dst.len(), src.len(), "copy_streaming: length mismatch");
    if dst.len() < STREAM_MIN {
        dst.copy_from_slice(src);
        return;
    }
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: equal lengths checked above; the AVX2 path runs only when
        // the CPU reports AVX2; SSE2 is part of x86_64.
        unsafe {
            if std::arch::is_x86_feature_detected!("avx2") {
                x86::copy_avx2(dst, src);
            } else {
                x86::copy_sse2(dst, src);
            }
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    dst.copy_from_slice(src);
}

/// Copy `src` to `dst`, `len` bytes, with streaming stores (see
/// [`copy_streaming`]).
///
/// # Safety
/// `src` valid for `len` reads, `dst` valid for `len` writes, the two not
/// overlapping (as for `core::ptr::copy_nonoverlapping`).
#[inline]
pub unsafe fn copy_streaming_raw(src: *const u8, dst: *mut u8, len: usize) {
    // SAFETY: the caller's contract.
    unsafe {
        copy_streaming(
            std::slice::from_raw_parts_mut(dst, len),
            std::slice::from_raw_parts(src, len),
        )
    }
}

/// Order every earlier streaming store before later stores: `sfence` on
/// x86_64, nothing elsewhere. Once per buffer, before its hand-off.
#[inline]
pub fn publish() {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: SSE is part of x86_64.
    unsafe {
        std::arch::x86_64::_mm_sfence()
    };
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    /// Ordinary-store head up to `align` bytes of destination alignment;
    /// returns its length.
    #[inline(always)]
    fn head_len(dst: &[u8], align: usize) -> usize {
        dst.as_ptr().align_offset(align).min(dst.len())
    }

    /// # Safety
    /// AVX2 present; equal lengths.
    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn copy_avx2(dst: &mut [u8], src: &[u8]) {
        let head = head_len(dst, 32);
        dst[..head].copy_from_slice(&src[..head]);
        let body = (dst.len() - head) & !127;
        // SAFETY: [head, head + body) is in bounds of both slices; the
        // destination is 32-byte aligned there; loads are unaligned.
        unsafe {
            let s = src.as_ptr().add(head);
            let d = dst.as_mut_ptr().add(head);
            let mut at = 0;
            while at < body {
                let a = _mm256_loadu_si256(s.add(at) as *const __m256i);
                let b = _mm256_loadu_si256(s.add(at + 32) as *const __m256i);
                let c = _mm256_loadu_si256(s.add(at + 64) as *const __m256i);
                let e = _mm256_loadu_si256(s.add(at + 96) as *const __m256i);
                _mm256_stream_si256(d.add(at) as *mut __m256i, a);
                _mm256_stream_si256(d.add(at + 32) as *mut __m256i, b);
                _mm256_stream_si256(d.add(at + 64) as *mut __m256i, c);
                _mm256_stream_si256(d.add(at + 96) as *mut __m256i, e);
                at += 128;
            }
        }
        let tail = head + body;
        dst[tail..].copy_from_slice(&src[tail..]);
    }

    /// # Safety
    /// Equal lengths.
    pub(super) unsafe fn copy_sse2(dst: &mut [u8], src: &[u8]) {
        let head = head_len(dst, 16);
        dst[..head].copy_from_slice(&src[..head]);
        let body = (dst.len() - head) & !63;
        // SAFETY: as in `copy_avx2`, with 16-byte alignment.
        unsafe {
            let s = src.as_ptr().add(head);
            let d = dst.as_mut_ptr().add(head);
            let mut at = 0;
            while at < body {
                let a = _mm_loadu_si128(s.add(at) as *const __m128i);
                let b = _mm_loadu_si128(s.add(at + 16) as *const __m128i);
                let c = _mm_loadu_si128(s.add(at + 32) as *const __m128i);
                let e = _mm_loadu_si128(s.add(at + 48) as *const __m128i);
                _mm_stream_si128(d.add(at) as *mut __m128i, a);
                _mm_stream_si128(d.add(at + 16) as *mut __m128i, b);
                _mm_stream_si128(d.add(at + 32) as *mut __m128i, c);
                _mm_stream_si128(d.add(at + 48) as *mut __m128i, e);
                at += 64;
            }
        }
        let tail = head + body;
        dst[tail..].copy_from_slice(&src[tail..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[1] ^ seed)
            .collect()
    }

    const SIZES: &[usize] = &[
        0, 1, 7, 31, 63, 64, 127, 128, 129, 4095, 4096, 4097, 4127, 4224, 8191, 65_536, 65_537,
        1 << 20, (1 << 20) + 77,
    ];

    /// Every size around the thresholds and unroll widths, every source and
    /// destination offset modulo 64: identical to `copy_from_slice`, and
    /// nothing outside the destination range is touched.
    fn check(copy: impl Fn(&mut [u8], &[u8])) {
        for &len in SIZES {
            let source = pattern(len + 64, 0x5a);
            for src_off in [0, 1, 3, 15, 16, 31, 32, 33, 63] {
                for dst_off in [0, 1, 7, 16, 17, 31, 32, 48, 63] {
                    let src = &source[src_off..src_off + len];
                    let mut expected = vec![0xeeu8; len + 128];
                    expected[dst_off..dst_off + len].copy_from_slice(src);
                    let mut actual = vec![0xeeu8; len + 128];
                    copy(&mut actual[dst_off..dst_off + len], src);
                    publish();
                    assert!(actual == expected, "len {len} src+{src_off} dst+{dst_off}");
                }
            }
        }
    }

    #[test]
    fn streaming_copy_matches_copy_from_slice() {
        check(copy_streaming);
    }

    #[test]
    fn raw_streaming_copy_matches_copy_from_slice() {
        check(|dst, src| unsafe { copy_streaming_raw(src.as_ptr(), dst.as_mut_ptr(), src.len()) });
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn each_x86_path_matches_copy_from_slice() {
        check(|dst, src| unsafe { x86::copy_sse2(dst, src) });
        if std::arch::is_x86_feature_detected!("avx2") {
            check(|dst, src| unsafe { x86::copy_avx2(dst, src) });
        }
    }

    /// The vector paths directly (no small-size cutoff): every length up to
    /// 1 KiB, every destination offset within a 32-byte line, two source
    /// offsets.
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn x86_paths_exhaustive_small() {
        let source = pattern(1024 + 64, 0xa5);
        let avx2 = std::arch::is_x86_feature_detected!("avx2");
        for len in 0..=1024 {
            for src_off in [0, 5] {
                let src = &source[src_off..src_off + len];
                for dst_off in 0..32 {
                    let mut expected = vec![0x11u8; len + 64];
                    expected[dst_off..dst_off + len].copy_from_slice(src);
                    let mut actual = vec![0x11u8; len + 64];
                    unsafe { x86::copy_sse2(&mut actual[dst_off..dst_off + len], src) };
                    assert!(actual == expected, "sse2 len {len} src+{src_off} dst+{dst_off}");
                    if avx2 {
                        actual.fill(0x11);
                        unsafe { x86::copy_avx2(&mut actual[dst_off..dst_off + len], src) };
                        assert!(actual == expected, "avx2 len {len} src+{src_off} dst+{dst_off}");
                    }
                }
            }
        }
        publish();
    }

    #[test]
    #[should_panic(expected = "length mismatch")]
    fn lengths_must_match() {
        copy_streaming(&mut [0; 8], &[0; 9]);
    }
}
