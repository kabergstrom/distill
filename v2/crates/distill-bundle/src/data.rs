//! Entry data (§6): each entry's `data` is read and written against its
//! `LogicalSchema` by the typed text codec (`ngp_schema::text`), which
//! checks the shape, canonical order and float spelling, and locates every
//! blob leaf by canonical structural path. This module adapts the codec to
//! the bundle: blob placement (container chunk in, layout out) and the
//! mapping of codec errors to [`BundleError`]s rooted at `data`.

use std::collections::BTreeMap;

use distill_json::Lexer;
use ngp_schema::{
    display_path, encode_path, read_value, BlobSink, BlobSource, Options, PathComponent,
    SchemaNode, TextError, TextErrorKind, TypeTable, Value,
};

use crate::error::BundleError;

/// One blob located by the container parse.
pub(crate) struct BlobSite {
    pub local_id: String,
    pub path_bytes: Vec<u8>,
    pub path_display: String,
    pub offset: u64,
    pub len: u64,
}

/// Writer-side cap on `data` text nesting. Pinned below
/// `distill_json::MAX_DEPTH` (512) with room for the envelope's own
/// nesting (top → assets → entry → data) and the `{"len","offset"}` blob
/// placement: the writer never emits a file the parser's depth cap would
/// reject.
pub(crate) const MAX_DATA_DEPTH: usize = 500;

pub(crate) fn display(path: &[PathComponent]) -> String {
    display_path("data", path)
}

/// Where a parse takes blob bytes from.
pub(crate) enum Blobs<'c> {
    /// Plain encoding: a blob node is an error (§6: blobs require the
    /// container).
    Plain,
    /// Container: placements are bounds-checked against the chunk and
    /// recorded for the global layout checks.
    Container {
        chunk: &'c [u8],
        sites: &'c mut Vec<BlobSite>,
    },
}

struct ChunkSource<'c, 'l> {
    local_id: &'l str,
    chunk: &'c [u8],
    sites: &'c mut Vec<BlobSite>,
    /// The bundle error behind a refused placement (the codec carries only
    /// a message).
    failed: Option<BundleError>,
}

impl BlobSource for ChunkSource<'_, '_> {
    fn blob(&mut self, path: &[PathComponent], len: u64, offset: u64) -> Result<Vec<u8>, String> {
        let blob_len = self.chunk.len() as u64;
        let bytes = offset
            .checked_add(len)
            .filter(|end| *end <= blob_len)
            .and_then(|end| self.chunk.get(offset as usize..end as usize));
        let Some(bytes) = bytes else {
            let error = BundleError::BlobOutOfBounds {
                local_id: self.local_id.to_string(),
                path: display(path),
                offset,
                len,
                blob_len,
            };
            let message = error.to_string();
            self.failed = Some(error);
            return Err(message);
        };
        self.sites.push(BlobSite {
            local_id: self.local_id.to_string(),
            path_bytes: encode_path(path),
            path_display: display(path),
            offset,
            len,
        });
        Ok(bytes.to_vec())
    }
}

/// Parse one entry's data span (canonical text) against its schema root.
pub(crate) fn parse_entry(
    local_id: &str,
    root: &SchemaNode,
    lex: &mut Lexer<'_>,
    blobs: &mut Blobs<'_>,
    mut types: &TypeTable,
) -> Result<Value, BundleError> {
    match blobs {
        Blobs::Plain => read_value(lex, root, Options::canonical().types(&mut types))
            .map_err(|e| map_error(local_id, e, None)),
        Blobs::Container { chunk, sites } => {
            let mut source = ChunkSource {
                local_id,
                chunk,
                sites,
                failed: None,
            };
            let out = read_value(
                lex,
                root,
                Options::canonical().blobs(&mut source).types(&mut types),
            );
            let failed = source.failed.take();
            out.map_err(|e| map_error(local_id, e, failed))
        }
    }
}

/// Places blobs at a precomputed layout, copying their bytes into the
/// chunk.
pub(crate) struct LayoutSink<'a> {
    pub layout: &'a BTreeMap<Vec<u8>, u64>,
    pub chunk: &'a mut [u8],
}

impl BlobSink for LayoutSink<'_> {
    fn place(&mut self, path: &[PathComponent], blob: &[u8]) -> Result<(u64, u64), String> {
        let offset = *self
            .layout
            .get(&encode_path(path))
            .ok_or_else(|| format!("no layout offset for blob at {}", display(path)))?;
        let start = offset as usize;
        let dest = self
            .chunk
            .get_mut(start..start + blob.len())
            .ok_or_else(|| format!("layout overruns the chunk at {}", display(path)))?;
        dest.copy_from_slice(blob);
        Ok((blob.len() as u64, offset))
    }
}

/// Container nesting of written `data` text (outside strings).
pub(crate) fn text_depth(text: &str) -> usize {
    let (mut depth, mut max, mut in_string, mut escaped) = (0usize, 0usize, false, false);
    for b in text.bytes() {
        if in_string {
            match (escaped, b) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'[' | b'{' => {
                depth += 1;
                max = max.max(depth);
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    max
}

/// A codec error as the bundle error naming the same defect. `failed` is
/// the bundle error behind a refused blob placement.
pub(crate) fn map_error(local_id: &str, e: TextError, failed: Option<BundleError>) -> BundleError {
    let local_id = local_id.to_string();
    let path = display(&e.path);
    match e.kind {
        TextErrorKind::Json(e) => BundleError::Json(e),
        TextErrorKind::Shape { expected, found } => BundleError::Shape {
            local_id,
            path,
            expected,
            found,
        },
        TextErrorKind::MissingField(field) => BundleError::MissingField {
            local_id,
            path,
            field,
        },
        TextErrorKind::UnknownField(field) => BundleError::ExtraField {
            local_id,
            path,
            field,
        },
        TextErrorKind::UnknownVariant(variant) => BundleError::UnknownVariant {
            local_id,
            path,
            variant,
        },
        TextErrorKind::EnumShape { keys } => BundleError::EnumShape {
            local_id,
            path,
            keys,
        },
        TextErrorKind::ArrayLen { expected, actual } => BundleError::ArrayLen {
            local_id,
            path,
            expected,
            actual,
        },
        TextErrorKind::NotSorted { what, index } | TextErrorKind::Duplicate { what, index } => {
            BundleError::NotSorted {
                local_id,
                path,
                what,
                index,
            }
        }
        TextErrorKind::NonFiniteFloat => BundleError::NonFiniteFloat { local_id, path },
        TextErrorKind::F32OutOfRange => BundleError::F32OutOfRange { local_id, path },
        TextErrorKind::BlobInPlain => BundleError::BlobInPlainBundle { local_id, path },
        TextErrorKind::BlobBarred => BundleError::BlobBarred { local_id, path },
        TextErrorKind::Blob(detail) => failed.unwrap_or(BundleError::BadBlobObject {
            local_id,
            path,
            detail,
        }),
        TextErrorKind::KeyNotEncodable { what } => BundleError::KeyNotEncodable {
            local_id,
            path,
            what,
        },
        TextErrorKind::BadBackRef { distance, frames } => BundleError::BadBackRef {
            local_id,
            path,
            distance,
            frames,
        },
        TextErrorKind::Depth => BundleError::WalkDepth { local_id, path },
        kind @ (TextErrorKind::Dynamic(_)
        | TextErrorKind::Reference(_)
        | TextErrorKind::MapRemoval) => BundleError::Value {
            local_id,
            path,
            detail: kind.to_string(),
        },
    }
}
