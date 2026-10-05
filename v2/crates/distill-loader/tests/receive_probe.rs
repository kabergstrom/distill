//! Doc 22 phase 0: the engine's receive cost per 1 MiB chunk, split into
//! its parts, and the copy shapes that explain the warm-sink copy rate.
//!
//! Ignored; run with
//! `cargo test --release -p distill-loader --test receive_probe -- --ignored --nocapture --test-threads 1`.
//!
//! `probe_receive_parts`: a sender thread streams 1 MiB capnp messages over
//! loopback TCP, copying each from a resident buffer into one reused
//! message (the doc 22 §4.2 daemon shape), with at most 4 unanswered (the
//! receiver returns a credit byte per message, as a `next` call would be
//! answered). The receiver runs the engine's stack: a current-thread tokio
//! runtime, `TcpStream::compat`, `futures::io::BufReader` (8 KiB), and
//! capnp-futures' reader, then copies the message's `Data` into a warm,
//! prefaulted 512 MiB sink. Three receivers:
//! - `stock`: `capnp_futures::serialize::try_read_message` as the RPC
//!   system calls it;
//! - `split`: the same steps written out (`serialize.rs` `read_segment_table`
//!   then `read_segments`), timed apart;
//! - `reused`: `split` with one segment buffer reused across messages.
//!
//! `probe_copy_shapes`: memcpy shapes for one 256 MiB pass.

// The probe-local streaming copy is x86_64 only.
#![cfg(target_arch = "x86_64")]

use std::cell::Cell;
use std::io::{BufWriter, Read, Write};
use std::net::TcpListener;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Instant;

use capnp::message::{HeapAllocator, ReaderOptions};
use capnp::serialize::SegmentLengthsBuilder;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio_util::compat::TokioAsyncReadCompatExt;

const CHUNK: usize = 1 << 20;
const MESSAGES: usize = 256;
const IN_FLIGHT: usize = 4;
const ITERATIONS: usize = 5;
const SINK: usize = 512 << 20;
const FRAME_BYTES: f64 = 33e6;

fn minflt() -> u64 {
    // SAFETY: getrusage writes the struct it is given.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_THREAD, &mut usage);
        usage.ru_minflt as u64
    }
}

fn ns(from: Instant, to: Instant) -> u64 {
    to.duration_since(from).as_nanos() as u64
}

fn rate(bytes: u64, ns: u64) -> String {
    if ns == 0 {
        return "      -    ".into();
    }
    let gib_s = bytes as f64 / (1u64 << 30) as f64 / (ns as f64 * 1e-9);
    let ms_per_frame = ns as f64 * 1e-6 * FRAME_BYTES / bytes as f64;
    format!("{gib_s:6.1} GiB/s {ms_per_frame:6.2} ms/33MB")
}

/// Time spent inside the socket's `poll_read`, split by its answer.
#[derive(Default)]
struct ReadCounters {
    ready_ns: Cell<u64>,
    ready_calls: Cell<u64>,
    pending_ns: Cell<u64>,
    pending_calls: Cell<u64>,
}

struct Timed<R> {
    inner: R,
    counters: Rc<ReadCounters>,
}

impl<R: AsyncRead + Unpin> AsyncRead for Timed<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let began = Instant::now();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        let spent = began.elapsed().as_nanos() as u64;
        let counters = &self.counters;
        if result.is_ready() {
            counters.ready_ns.set(counters.ready_ns.get() + spent);
            counters.ready_calls.set(counters.ready_calls.get() + 1);
        } else {
            counters.pending_ns.set(counters.pending_ns.get() + spent);
            counters.pending_calls.set(counters.pending_calls.get() + 1);
        }
        result
    }
}

fn spawn_sender(listener: TcpListener, messages: usize) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream.set_nodelay(true).unwrap();
        let mut credits = stream.try_clone().unwrap();
        // Resident source, like a mapped segment already in the page cache.
        let source = vec![7u8; MESSAGES * CHUNK];
        let mut message = capnp::message::Builder::new(
            HeapAllocator::new().first_segment_words((CHUNK / 8 + 64) as u32),
        );
        message
            .init_root::<capnp::any_pointer::Builder>()
            .initn_as::<capnp::data::Builder>(CHUNK as u32);
        let mut writer = BufWriter::with_capacity(64 * 1024, stream);
        let mut outstanding = 0usize;
        let mut credit = [0u8; 64];
        for index in 0..messages {
            while outstanding >= IN_FLIGHT {
                let read = credits.read(&mut credit).unwrap();
                assert!(read > 0, "receiver closed");
                outstanding -= read;
            }
            let at = (index % MESSAGES) * CHUNK;
            message
                .get_root::<capnp::data::Builder>()
                .unwrap()
                .copy_from_slice(&source[at..at + CHUNK]);
            capnp::serialize::write_message(&mut writer, &message).unwrap();
            writer.flush().unwrap();
            outstanding += 1;
        }
        // Drain the last credits so the receiver's writes succeed.
        while outstanding > 0 {
            match credits.read(&mut credit) {
                Ok(0) | Err(_) => break,
                Ok(read) => outstanding = outstanding.saturating_sub(read),
            }
        }
    })
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Receiver {
    Stock,
    /// `Stock` copying into the sink with the probe-local streaming
    /// stores, fenced per message (nt-copy).
    StockStreaming,
    Split,
    Reused,
}

#[derive(Default, Debug, Clone, Copy)]
struct Parts {
    wall_ns: u64,
    /// `try_read_message` as a whole (stock only).
    read_message_ns: u64,
    /// Wall time inside the reads (header and body).
    read_wall_ns: u64,
    /// Inside `poll_read` answering Ready: the kernel copy into the buffer.
    socket_ready_ns: u64,
    socket_ready_calls: u64,
    /// Inside `poll_read` answering Pending (EAGAIN plus registration).
    socket_pending_ns: u64,
    socket_pending_calls: u64,
    alloc_ns: u64,
    alloc_faults: u64,
    read_faults: u64,
    parse_ns: u64,
    copy_ns: u64,
    copy_faults: u64,
    free_ns: u64,
    credit_ns: u64,
}

fn receive(receiver: Receiver, sink: &mut [u8], sink_cursor: &mut usize) -> Parts {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let sender = spawn_sender(listener, MESSAGES);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let parts = runtime.block_on(async {
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (reader, mut writer) = stream.compat().split();
        let counters = Rc::new(ReadCounters::default());
        let mut reader = BufReader::new(Timed {
            inner: reader,
            counters: counters.clone(),
        });
        let options = ReaderOptions::new();
        let mut parts = Parts::default();
        let mut reused: Option<capnp::serialize::OwnedSegments> = None;
        let began = Instant::now();
        for _ in 0..MESSAGES {
            let ready_before = (counters.ready_ns.get(), counters.ready_calls.get());
            let pending_before = (counters.pending_ns.get(), counters.pending_calls.get());
            let at = *sink_cursor;
            *sink_cursor = (*sink_cursor + CHUNK) % (SINK - CHUNK);
            match receiver {
                Receiver::Stock | Receiver::StockStreaming => {
                    let t0 = Instant::now();
                    let f0 = minflt();
                    let message = capnp_futures::serialize::try_read_message(&mut reader, options)
                        .await
                        .unwrap()
                        .expect("a message");
                    let t1 = Instant::now();
                    let f1 = minflt();
                    let bytes = message.get_root::<capnp::data::Reader>().unwrap();
                    let t2 = Instant::now();
                    if receiver == Receiver::StockStreaming {
                        copy_streaming(&mut sink[at..at + bytes.len()], bytes);
                        sfence();
                    } else {
                        sink[at..at + bytes.len()].copy_from_slice(bytes);
                    }
                    let t3 = Instant::now();
                    let f3 = minflt();
                    drop(message);
                    let t4 = Instant::now();
                    parts.read_message_ns += ns(t0, t1);
                    parts.read_wall_ns += ns(t0, t1);
                    parts.read_faults += f1 - f0;
                    parts.parse_ns += ns(t1, t2);
                    parts.copy_ns += ns(t2, t3);
                    parts.copy_faults += f3 - f1;
                    parts.free_ns += ns(t3, t4);
                }
                Receiver::Split | Receiver::Reused => {
                    let t0 = Instant::now();
                    let mut header = [0u8; 8];
                    reader.read_exact(&mut header).await.unwrap();
                    let segments = u32::from_le_bytes(header[0..4].try_into().unwrap()) + 1;
                    assert_eq!(segments, 1, "one segment per chunk message");
                    let words = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
                    let t1 = Instant::now();
                    let f1 = minflt();
                    let mut owned = match (receiver, reused.take()) {
                        (Receiver::Reused, Some(owned)) if owned.len() == words * 8 => owned,
                        _ => {
                            let mut lengths = SegmentLengthsBuilder::with_capacity(1);
                            lengths.try_push_segment(words).unwrap();
                            lengths.into_owned_segments()
                        }
                    };
                    let t2 = Instant::now();
                    let f2 = minflt();
                    reader.read_exact(&mut owned[..]).await.unwrap();
                    let t3 = Instant::now();
                    let f3 = minflt();
                    let message = capnp::message::Reader::new(owned, options);
                    let bytes = message.get_root::<capnp::data::Reader>().unwrap();
                    let t4 = Instant::now();
                    sink[at..at + bytes.len()].copy_from_slice(bytes);
                    let t5 = Instant::now();
                    let f5 = minflt();
                    if receiver == Receiver::Reused {
                        reused = Some(message.into_segments());
                    } else {
                        drop(message);
                    }
                    let t6 = Instant::now();
                    parts.read_wall_ns += ns(t0, t1) + ns(t2, t3);
                    parts.alloc_ns += ns(t1, t2);
                    parts.alloc_faults += f2 - f1;
                    parts.read_faults += f3 - f2;
                    parts.parse_ns += ns(t3, t4);
                    parts.copy_ns += ns(t4, t5);
                    parts.copy_faults += f5 - f3;
                    parts.free_ns += ns(t5, t6);
                }
            }
            parts.socket_ready_ns += counters.ready_ns.get() - ready_before.0;
            parts.socket_ready_calls += counters.ready_calls.get() - ready_before.1;
            parts.socket_pending_ns += counters.pending_ns.get() - pending_before.0;
            parts.socket_pending_calls += counters.pending_calls.get() - pending_before.1;
            let c0 = Instant::now();
            writer.write_all(&[1]).await.unwrap();
            writer.flush().await.unwrap();
            parts.credit_ns += ns(c0, Instant::now());
        }
        parts.wall_ns = ns(began, Instant::now());
        drop(writer);
        parts
    });
    sender.join().unwrap();
    parts
}

fn report(receiver: Receiver, iteration: usize, parts: &Parts) {
    let bytes = (MESSAGES * CHUNK) as u64;
    let socket = parts.socket_ready_ns;
    let polling = parts.socket_pending_ns;
    // In a read, outside poll_read: parked in epoll or in the runtime.
    let waiting = parts.read_wall_ns.saturating_sub(socket + polling);
    let busy = parts.wall_ns - waiting;
    println!(
        "PROBE receive {receiver:?} iter={iteration}: wall {:.1} ms ({}), busy {:.1} ms ({})",
        parts.wall_ns as f64 * 1e-6,
        rate(bytes, parts.wall_ns),
        busy as f64 * 1e-6,
        rate(bytes, busy),
    );
    if matches!(receiver, Receiver::Stock | Receiver::StockStreaming) {
        println!(
            "  read_message {:7.1} ms {}  faults {}",
            parts.read_message_ns as f64 * 1e-6,
            rate(bytes, parts.read_message_ns),
            parts.read_faults,
        );
    } else {
        println!(
            "  alloc        {:7.1} ms {}  faults {} ({:.1}/msg)",
            parts.alloc_ns as f64 * 1e-6,
            rate(bytes, parts.alloc_ns),
            parts.alloc_faults,
            parts.alloc_faults as f64 / MESSAGES as f64,
        );
    }
    println!(
        "  socket read  {:7.1} ms {}  calls {} ({:.1}/msg), read faults {}",
        socket as f64 * 1e-6,
        rate(bytes, socket),
        parts.socket_ready_calls,
        parts.socket_ready_calls as f64 / MESSAGES as f64,
        parts.read_faults,
    );
    println!(
        "  pending poll {:7.1} ms  calls {}",
        polling as f64 * 1e-6,
        parts.socket_pending_calls,
    );
    println!("  waiting      {:7.1} ms", waiting as f64 * 1e-6);
    println!(
        "  parse        {:7.3} ms {}",
        parts.parse_ns as f64 * 1e-6,
        rate(bytes, parts.parse_ns),
    );
    println!(
        "  copy to sink {:7.1} ms {}  faults {}",
        parts.copy_ns as f64 * 1e-6,
        rate(bytes, parts.copy_ns),
        parts.copy_faults,
    );
    println!(
        "  free         {:7.1} ms   credit write {:.1} ms",
        parts.free_ns as f64 * 1e-6,
        parts.credit_ns as f64 * 1e-6,
    );
}

#[test]
#[ignore = "measurement probe (doc 22 phase 0)"]
fn probe_receive_parts() {
    // Warm sink: prefaulted once, reused (a staging ring stand-in).
    let mut sink = vec![1u8; SINK];
    let mut cursor = 0usize;
    for receiver in [
        Receiver::Stock,
        Receiver::StockStreaming,
        Receiver::Split,
        Receiver::Reused,
    ] {
        // The first pass warms the allocator (glibc's dynamic mmap
        // threshold) and the sender; it is reported but not representative.
        for iteration in 0..=ITERATIONS {
            let parts = receive(receiver, &mut sink, &mut cursor);
            report(receiver, iteration, &parts);
        }
    }
}

fn aligned(buffer: &mut [u8], len: usize) -> &mut [u8] {
    let skew = buffer.as_ptr().align_offset(64);
    &mut buffer[skew..skew + len]
}

#[test]
#[ignore = "measurement probe (doc 22 phase 0)"]
fn probe_copy_shapes() {
    const TOTAL: usize = 256 << 20;
    const FONT: usize = 36 << 20;
    let bytes = TOTAL as u64;
    let mut dst_buffer = vec![1u8; TOTAL + 64];
    let dst = aligned(&mut dst_buffer, TOTAL);
    let src_big = vec![2u8; TOTAL];
    let src_hot = vec![3u8; CHUNK];
    let mut hot_dst = vec![4u8; CHUNK];
    let mut shapes: Vec<(&str, Vec<u64>)> = Vec::new();
    let mut run = |name: &'static str, f: &mut dyn FnMut()| {
        f();
        let mut times = Vec::new();
        for _ in 0..ITERATIONS {
            let began = Instant::now();
            f();
            times.push(began.elapsed().as_nanos() as u64);
        }
        shapes.push((name, times));
    };
    run("one 256 MiB memcpy, warm dst (glibc: non-temporal above 24 MiB)", &mut || {
        dst.copy_from_slice(&src_big);
    });
    run("256 x 1 MiB memcpy, hot 1 MiB src, warm dst (regular stores)", &mut || {
        for chunk in dst.chunks_exact_mut(CHUNK) {
            chunk.copy_from_slice(&src_hot);
        }
    });
    run("256 x 1 MiB memcpy, cold src, warm dst", &mut || {
        for (chunk, src) in dst.chunks_exact_mut(CHUNK).zip(src_big.chunks_exact(CHUNK)) {
            chunk.copy_from_slice(src);
        }
    });
    run("256 x 1 MiB streaming copy, hot src, warm dst", &mut || {
        for chunk in dst.chunks_exact_mut(CHUNK) {
            copy_streaming(chunk, &src_hot);
        }
        sfence();
    });
    run("256 x 1 MiB streaming copy, cold src, warm dst", &mut || {
        for (chunk, src) in dst.chunks_exact_mut(CHUNK).zip(src_big.chunks_exact(CHUNK)) {
            copy_streaming(chunk, src);
        }
        sfence();
    });
    // The loader's fetch buffer (nt-copy): sized once, never touched before
    // 64 KiB chunks arrive, so every page faults in during the copy.
    run("fresh 256 MiB Vec, 64 KiB memcpy chunks (page faults)", &mut || {
        fresh_fill(&src_hot, false);
    });
    run("fresh 256 MiB Vec, 64 KiB streaming chunks (page faults)", &mut || {
        fresh_fill(&src_hot, true);
    });
    // The font's `to_vec` (36 MiB, fresh pages): 7 per pass, 252 MiB.
    let font = &src_big[..FONT];
    run("7 x 36 MiB to_vec into fresh pages (glibc: non-temporal above 24 MiB)", &mut || {
        for _ in 0..7 {
            std::hint::black_box(font.to_vec());
        }
    });
    run("7 x 36 MiB streaming copy into fresh pages", &mut || {
        for _ in 0..7 {
            let mut out = Vec::<u8>::with_capacity(FONT);
            // SAFETY: `out` has room for `FONT` bytes, all written before
            // `set_len`.
            unsafe {
                copy_streaming_raw(font.as_ptr(), out.as_mut_ptr(), FONT);
                out.set_len(FONT);
            }
            sfence();
            std::hint::black_box(out);
        }
    });
    run("256 x 1 MiB memcpy into one cache-resident 1 MiB dst", &mut || {
        for _ in 0..TOTAL / CHUNK {
            hot_dst.copy_from_slice(&src_hot);
            std::hint::black_box(&mut hot_dst);
        }
    });
    run("256 x 1 MiB fresh Vec (capnp allocate_zeroed_vec shape)", &mut || {
        for _ in 0..TOTAL / CHUNK {
            let words = capnp::Word::allocate_zeroed_vec(CHUNK / 8);
            std::hint::black_box(&words);
        }
    });
    run("256 x 1 MiB write-only fill, warm dst (memset)", &mut || {
        for chunk in dst.chunks_exact_mut(CHUNK) {
            chunk.fill(5);
            std::hint::black_box(&mut *chunk);
        }
    });
    for (name, times) in &shapes {
        let low = *times.iter().min().unwrap();
        let high = *times.iter().max().unwrap();
        println!(
            "PROBE copy {name}: {:.1}-{:.1} ms, {} .. {}",
            low as f64 * 1e-6,
            high as f64 * 1e-6,
            rate(bytes, high),
            rate(bytes, low),
        );
    }
}

/// 256 MiB into a fresh `Vec` in 64 KiB chunks from a hot source, as the
/// loader's fetch buffer fills; dropped after.
fn fresh_fill(src_hot: &[u8], streaming: bool) {
    const TOTAL: usize = 256 << 20;
    const PIECE: usize = 64 << 10;
    let mut out = Vec::<u8>::with_capacity(TOTAL);
    for at in (0..TOTAL).step_by(PIECE) {
        // SAFETY: `out` has `TOTAL` bytes of capacity; each piece is written
        // in full before `set_len` covers it.
        unsafe {
            let dst = out.as_mut_ptr().add(at);
            if streaming {
                copy_streaming_raw(src_hot.as_ptr(), dst, PIECE);
            } else {
                std::ptr::copy_nonoverlapping(src_hot.as_ptr(), dst, PIECE);
            }
            out.set_len(at + PIECE);
        }
    }
    sfence();
    std::hint::black_box(&out);
}

/// Probe-local streaming copy (doc 22 §1.6, nt-copy measurements): AVX2
/// non-temporal stores with the destination head to 32-byte alignment and
/// the tail copied ordinarily. Measurement only; nothing in Distill or the
/// engine uses streaming stores.
fn copy_streaming(dst: &mut [u8], src: &[u8]) {
    assert_eq!(dst.len(), src.len());
    assert!(std::arch::is_x86_feature_detected!("avx2"), "the probe needs AVX2");
    // SAFETY: AVX2 checked above; equal lengths.
    unsafe { copy_avx2(dst, src) }
}

/// # Safety
/// `src` valid for `len` reads, `dst` for `len` writes, not overlapping.
unsafe fn copy_streaming_raw(src: *const u8, dst: *mut u8, len: usize) {
    // SAFETY: the caller's contract.
    unsafe {
        copy_streaming(
            std::slice::from_raw_parts_mut(dst, len),
            std::slice::from_raw_parts(src, len),
        )
    }
}

#[target_feature(enable = "avx2")]
unsafe fn copy_avx2(dst: &mut [u8], src: &[u8]) {
    use std::arch::x86_64::*;
    let head = dst.as_ptr().align_offset(32).min(dst.len());
    dst[..head].copy_from_slice(&src[..head]);
    let body = (dst.len() - head) & !127;
    // SAFETY: [head, head + body) is in bounds of both; destination aligned.
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

/// Orders the streaming stores before the buffer is handed on.
fn sfence() {
    // SAFETY: SSE is part of x86_64.
    unsafe { std::arch::x86_64::_mm_sfence() }
}

#[test]
fn probe_streaming_copy_matches_copy_from_slice() {
    if !std::arch::is_x86_feature_detected!("avx2") {
        return;
    }
    let source: Vec<u8> = (0..70_000u32).map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8).collect();
    for len in [0, 1, 31, 127, 128, 129, 4095, 65_537] {
        for (src_off, dst_off) in [(0, 0), (1, 7), (33, 17), (5, 31)] {
            let src = &source[src_off..src_off + len];
            let mut expected = vec![0xeeu8; len + 64];
            expected[dst_off..dst_off + len].copy_from_slice(src);
            let mut actual = vec![0xeeu8; len + 64];
            copy_streaming(&mut actual[dst_off..dst_off + len], src);
            sfence();
            assert!(actual == expected, "len {len} src+{src_off} dst+{dst_off}");
        }
    }
}
