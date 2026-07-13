//! The deterministic `BuildHasher` (§5's serializability rule):
//! serializable hash-based maps and sets require the asset-types
//! support crate's fixed-seed state — a std `RandomState` map is
//! unserializable. Decoding constructs through the map's real hasher
//! (§12), so a random seed would make two decodes of one verified
//! artifact iterate differently.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};

/// Fixed-seed `BuildHasher`: FNV-1a over the written bytes, seeded by a
/// pinned constant. Deterministic across processes, builds, and
/// platforms — framework-injected entropy is exactly what §5 bars.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeterministicState;

impl BuildHasher for DeterministicState {
    type Hasher = DeterministicHasher;
    fn build_hasher(&self) -> DeterministicHasher {
        DeterministicHasher(0xcbf2_9ce4_8422_2325)
    }
}

/// 64-bit FNV-1a.
#[derive(Debug, Clone)]
pub struct DeterministicHasher(u64);

impl Hasher for DeterministicHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= u64::from(b);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    // The standard defaults use native-endian integer bytes. Override
    // every integer entry so iteration order is also stable across LE/BE
    // targets (the same cross-target contract as the wire grammars).
    fn write_u8(&mut self, i: u8) {
        self.write(&i.to_le_bytes());
    }
    fn write_u16(&mut self, i: u16) {
        self.write(&i.to_le_bytes());
    }
    fn write_u32(&mut self, i: u32) {
        self.write(&i.to_le_bytes());
    }
    fn write_u64(&mut self, i: u64) {
        self.write(&i.to_le_bytes());
    }
    fn write_u128(&mut self, i: u128) {
        self.write(&i.to_le_bytes());
    }
    fn write_usize(&mut self, i: usize) {
        self.write(&(i as u64).to_le_bytes());
    }
    fn write_i8(&mut self, i: i8) {
        self.write(&i.to_le_bytes());
    }
    fn write_i16(&mut self, i: i16) {
        self.write(&i.to_le_bytes());
    }
    fn write_i32(&mut self, i: i32) {
        self.write(&i.to_le_bytes());
    }
    fn write_i64(&mut self, i: i64) {
        self.write(&i.to_le_bytes());
    }
    fn write_i128(&mut self, i: i128) {
        self.write(&i.to_le_bytes());
    }
    fn write_isize(&mut self, i: isize) {
        self.write(&(i as i64).to_le_bytes());
    }
}

/// The serializable hash map: `HashMap` pinned to the fixed-seed state.
pub type AssetHashMap<K, V> = HashMap<K, V, DeterministicState>;
/// The serializable hash set: `HashSet` pinned to the fixed-seed state.
pub type AssetHashSet<T> = HashSet<T, DeterministicState>;
