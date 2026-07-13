//! §6 happy paths: byte-identical roundtrips for both encodings,
//! deterministic blob layout by canonical structural path, and the pinned
//! path/CRC encodings.

mod common;

use common::*;
use distill_bundle::{
    crc32c, encode_path, parse_bundle, write_bundle, PathComponent as P, CONTAINER_MAGIC,
};
use distill_json::AuthoredValue as V;
use ngp_schema::{PrimitiveKind as PK, SchemaNode as N};

#[test]
fn plain_roundtrip_two_assets_byte_identical() {
    let sc = simple_schema();
    let b = bundle(
        &[&sc],
        vec![
            (
                "a",
                entry(UUID_A, &sc, obj(&[("count", u(3)), ("name", s("first"))])),
            ),
            (
                "b",
                entry(UUID_B, &sc, obj(&[("count", u(0)), ("name", s("second"))])),
            ),
        ],
        Some("a"),
    );
    let bytes = write_bundle(&b).unwrap();
    assert_eq!(bytes[0], b'{', "no blobs => plain JSON");
    assert!(bytes.ends_with(b"\n"), "exactly one trailing newline");
    assert!(!bytes.ends_with(b"\n\n"), "exactly one trailing newline");

    let p = parse_bundle(&bytes).unwrap();
    assert_eq!(p, b, "parse(write(b)) == b structurally");
    assert_eq!(write_bundle(&p).unwrap(), bytes, "byte-identical rewrite");
}

#[test]
fn plain_parse_write_parse_stable() {
    let sc = simple_schema();
    let b = bundle(
        &[&sc],
        vec![(
            "only",
            entry(UUID_A, &sc, obj(&[("count", u(7)), ("name", s("x"))])),
        )],
        None,
    );
    let bytes = write_bundle(&b).unwrap();
    let p1 = parse_bundle(&bytes).unwrap();
    let bytes2 = write_bundle(&p1).unwrap();
    let p2 = parse_bundle(&bytes2).unwrap();
    assert_eq!(bytes, bytes2);
    assert_eq!(p1, p2);
}

#[test]
fn plain_accepts_noncanonical_formatting_and_canonicalizes() {
    let sc = simple_schema();
    let b = bundle(
        &[&sc],
        vec![(
            "a",
            entry(UUID_A, &sc, obj(&[("count", u(1)), ("name", s("n"))])),
        )],
        None,
    );
    let bytes = write_bundle(&b).unwrap();
    // Hand-editing tolerance: surrounding whitespace parses fine…
    let mut spaced = b"  ".to_vec();
    spaced.extend_from_slice(&bytes);
    spaced.extend_from_slice(b"  \n");
    let p = parse_bundle(&spaced).unwrap();
    assert_eq!(p, b);
    // …and the rewrite is canonical again.
    assert_eq!(write_bundle(&p).unwrap(), bytes);
}

#[test]
fn empty_assets_bundle_roundtrips() {
    let b = bundle(&[], vec![], None);
    let bytes = write_bundle(&b).unwrap();
    let p = parse_bundle(&bytes).unwrap();
    assert_eq!(p, b);
    assert_eq!(write_bundle(&p).unwrap(), bytes);
}

#[test]
fn container_roundtrip_with_blobs_byte_identical() {
    let sc = blobby_schema();
    let data = obj(&[
        (
            "choice",
            obj(&[("A", obj(&[("data", blob(b"choiceblob"))]))]),
        ),
        ("data", blob(b"rootblob")),
        ("inner", obj(&[("data", blob(b""))])), // zero-length blob
        (
            "items",
            arr(vec![
                obj(&[("data", blob(b"item0"))]),
                obj(&[("data", blob(b"item1"))]),
            ]),
        ),
        (
            "table",
            arr(vec![arr(vec![u(5), obj(&[("data", blob(b"tableblob"))])])]),
        ),
    ]);
    let b = bundle(&[&sc], vec![("x", entry(UUID_A, &sc, data))], None);

    let bytes = write_bundle(&b).unwrap();
    assert_eq!(&bytes[..8], &CONTAINER_MAGIC, "blobs => container");

    let p = parse_bundle(&bytes).unwrap();
    assert_eq!(p, b, "blob bytes survive the roundtrip");
    assert_eq!(write_bundle(&p).unwrap(), bytes, "byte-identical rewrite");
}

#[test]
fn container_blob_chunk_layout_is_canonical() {
    // Same fixture as the roundtrip; the chunk layout is pinned: blobs at
    // successive 16-aligned offsets ordered by encoded structural path
    // bytes. All leaves are named "data" — ordering is by full path:
    //   .data (Field "data", len 4)          @ 0   (8 bytes)
    //   .inner.data (len-5 "inner")          @ 16  (0 bytes, zero-length —
    //                                          equals the next offset)
    //   .items[0].data                       @ 16  (5 bytes)
    //   .items[1].data                       @ 32  (5 bytes)
    //   .table[5].data                       @ 48  (9 bytes)
    //   .choice.<A>.data (len-6 "choice")    @ 64  (10 bytes)
    let sc = blobby_schema();
    let data = obj(&[
        (
            "choice",
            obj(&[("A", obj(&[("data", blob(b"choiceblob"))]))]),
        ),
        ("data", blob(b"rootblob")),
        ("inner", obj(&[("data", blob(b""))])),
        (
            "items",
            arr(vec![
                obj(&[("data", blob(b"item0"))]),
                obj(&[("data", blob(b"item1"))]),
            ]),
        ),
        (
            "table",
            arr(vec![arr(vec![u(5), obj(&[("data", blob(b"tableblob"))])])]),
        ),
    ]);
    let b = bundle(&[&sc], vec![("x", entry(UUID_A, &sc, data))], None);
    let bytes = write_bundle(&b).unwrap();
    let (_, pad, chunk) = container_parts(&bytes);
    assert!(pad.iter().all(|&b| b == 0), "pad bytes are zero");

    let mut expect = Vec::new();
    expect.extend_from_slice(b"rootblob");
    expect.resize(16, 0);
    expect.extend_from_slice(b"item0");
    expect.resize(32, 0);
    expect.extend_from_slice(b"item1");
    expect.resize(48, 0);
    expect.extend_from_slice(b"tableblob");
    expect.resize(64, 0);
    expect.extend_from_slice(b"choiceblob");
    assert_eq!(
        chunk, expect,
        "canonical blob layout, blob_len = end of last blob"
    );
}

#[test]
fn leaf_name_aliasing_resolved_by_full_paths_and_local_id_order() {
    // Two blobs whose leaf field name is identical ("blob") but nested
    // under different parents: distinct structural paths, deterministic
    // order by encoded path bytes (Field "a" < Field "bb" because the
    // length frame is compared first). Two assets prove (local_id, path)
    // ordering: all of p's blobs precede q's.
    let sc = schema(st(&[
        ("a", st(&[("blob", N::Blob)])),
        ("bb", st(&[("blob", N::Blob)])),
    ]));
    let data_p = obj(&[
        ("a", obj(&[("blob", blob(b"P-FIRST"))])),
        ("bb", obj(&[("blob", blob(b"P-SECOND"))])),
    ]);
    let data_q = obj(&[
        ("a", obj(&[("blob", blob(b"Q-FIRST"))])),
        ("bb", obj(&[("blob", blob(b"Q-SECOND"))])),
    ]);
    let b = bundle(
        &[&sc],
        vec![
            ("p", entry(UUID_A, &sc, data_p)),
            ("q", entry(UUID_B, &sc, data_q)),
        ],
        None,
    );
    let bytes = write_bundle(&b).unwrap();
    let (_, _, chunk) = container_parts(&bytes);

    let mut expect = Vec::new();
    expect.extend_from_slice(b"P-FIRST");
    expect.resize(16, 0);
    expect.extend_from_slice(b"P-SECOND");
    expect.resize(32, 0);
    expect.extend_from_slice(b"Q-FIRST");
    expect.resize(48, 0);
    expect.extend_from_slice(b"Q-SECOND");
    assert_eq!(chunk, expect);
    assert_eq!(parse_bundle(&bytes).unwrap(), b);
}

#[test]
fn backref_recursive_tree_blobs_at_two_depths() {
    // Tree node: { children: Vec<BackRef(0)>, payload: Blob }. The child's
    // payload is reached through BackRef expansion — its structural path
    // (.children[0].payload) differs from the root's (.payload), and
    // "payload" (len 7) sorts before "children" (len 8).
    let sc = tree_schema();
    let data = obj(&[
        (
            "children",
            arr(vec![obj(&[
                ("children", arr(vec![])),
                ("payload", blob(b"leafblob")),
            ])]),
        ),
        ("payload", blob(b"rootblob")),
    ]);
    let b = bundle(&[&sc], vec![("t", entry(UUID_A, &sc, data))], None);
    let bytes = write_bundle(&b).unwrap();
    let (_, _, chunk) = container_parts(&bytes);

    let mut expect = Vec::new();
    expect.extend_from_slice(b"rootblob"); // .payload
    expect.resize(16, 0);
    expect.extend_from_slice(b"leafblob"); // .children[0].payload
    assert_eq!(chunk, expect);
    assert_eq!(parse_bundle(&bytes).unwrap(), b);
    assert_eq!(write_bundle(&parse_bundle(&bytes).unwrap()).unwrap(), bytes);
}

#[test]
fn string_key_map_blobs_ordered_by_encoded_key() {
    let sc = schema(st(&[(
        "sm",
        N::Map {
            key: Box::new(N::String),
            value: Box::new(st(&[("d", N::Blob)])),
        },
    )]));
    let data = obj(&[(
        "sm",
        obj(&[
            ("alpha", obj(&[("d", blob(b"AAA"))])),
            ("beta", obj(&[("d", blob(b"BBB"))])),
            ("bets", obj(&[("d", blob(b"CCC"))])),
        ]),
    )]);
    let b = bundle(&[&sc], vec![("m", entry(UUID_A, &sc, data))], None);
    let bytes = write_bundle(&b).unwrap();
    let (_, _, chunk) = container_parts(&bytes);

    // MapKey components are length-framed (u64 BE length before payload),
    // so encoded-path order compares key length first, then key bytes:
    // "beta" and "bets" (len 4) precede "alpha" (len 5), and tie-break
    // lexicographically. Deterministic total order is the requirement; this
    // pins it.
    let mut expect = Vec::new();
    expect.extend_from_slice(b"BBB"); // ."beta".d
    expect.resize(16, 0);
    expect.extend_from_slice(b"CCC"); // ."bets".d
    expect.resize(32, 0);
    expect.extend_from_slice(b"AAA"); // ."alpha".d
    assert_eq!(chunk, expect);
    assert_eq!(parse_bundle(&bytes).unwrap(), b);
    assert_eq!(write_bundle(&parse_bundle(&bytes).unwrap()).unwrap(), bytes);
}

#[test]
fn option_none_walks_nothing_option_some_walks_inner() {
    let sc = schema(st(&[
        ("blobby", N::Option(Box::new(st(&[("data", N::Blob)])))),
        ("name", N::Option(Box::new(N::String))),
    ]));
    // None at the blob-carrying option: the Blob node is never reached, so
    // the writer picks plain JSON.
    let b_none = bundle(
        &[&sc],
        vec![(
            "a",
            entry(UUID_A, &sc, obj(&[("blobby", V::Null), ("name", s("hi"))])),
        )],
        None,
    );
    let bytes = write_bundle(&b_none).unwrap();
    assert_eq!(bytes[0], b'{');
    assert_eq!(parse_bundle(&bytes).unwrap(), b_none);

    // Some: the inner struct is walked, the blob lands in the container.
    let b_some = bundle(
        &[&sc],
        vec![(
            "a",
            entry(
                UUID_A,
                &sc,
                obj(&[("blobby", obj(&[("data", blob(b"D"))])), ("name", V::Null)]),
            ),
        )],
        None,
    );
    let bytes = write_bundle(&b_some).unwrap();
    assert_eq!(&bytes[..8], &CONTAINER_MAGIC);
    assert_eq!(parse_bundle(&bytes).unwrap(), b_some);
    assert_eq!(write_bundle(&parse_bundle(&bytes).unwrap()).unwrap(), bytes);
}

#[test]
fn nonstring_map_and_set_roundtrip() {
    let sc = schema(st(&[
        (
            "m",
            N::Map {
                key: Box::new(N::Primitive(PK::I64)),
                value: Box::new(N::String),
            },
        ),
        ("s", N::Set(Box::new(N::String))),
        (
            "sm",
            N::Map {
                key: Box::new(N::String),
                value: Box::new(N::Primitive(PK::U8)),
            },
        ),
    ]));
    let data = obj(&[
        // Non-string-key map: [k, v] pairs sorted by encoded key bytes
        // ("-1" < "2" byte-wise).
        (
            "m",
            arr(vec![
                arr(vec![V::Int(-1), s("neg")]),
                arr(vec![u(2), s("pos")]),
            ]),
        ),
        ("s", arr(vec![s("a"), s("b")])),
        ("sm", obj(&[("k1", u(1)), ("k2", u(2))])),
    ]);
    let b = bundle(&[&sc], vec![("a", entry(UUID_A, &sc, data))], None);
    let bytes = write_bundle(&b).unwrap();
    let p = parse_bundle(&bytes).unwrap();
    assert_eq!(p, b);
    assert_eq!(write_bundle(&p).unwrap(), bytes);
}

#[test]
fn reserved_settings_and_record_local_ids_accepted() {
    let sc = simple_schema();
    let mk = |uuid: &str| entry(uuid, &sc, obj(&[("count", u(1)), ("name", s("v"))]));
    let b = bundle(
        &[&sc],
        vec![
            ("$record", mk("00000000-0000-0000-0000-000000000001")),
            ("$settings", mk("00000000-0000-0000-0000-000000000002")),
            ("content", mk(UUID_A)),
        ],
        Some("content"),
    );
    let bytes = write_bundle(&b).unwrap();
    let p = parse_bundle(&bytes).unwrap();
    assert_eq!(p, b);
    assert_eq!(write_bundle(&p).unwrap(), bytes);
}

#[test]
fn container_with_zero_blobs_parses_but_writer_picks_plain() {
    let sc = simple_schema();
    let b = bundle(
        &[&sc],
        vec![(
            "a",
            entry(UUID_A, &sc, obj(&[("count", u(2)), ("name", s("z"))])),
        )],
        None,
    );
    let plain = write_bundle(&b).unwrap();
    let container = build_container(&plain, &[]);
    let p = parse_bundle(&container).unwrap();
    assert_eq!(p, b, "zero-blob container is legal to parse");
    assert_eq!(
        write_bundle(&p).unwrap(),
        plain,
        "the writer never produces a zero-blob container"
    );
}

#[test]
fn crc32c_check_vector() {
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
}

#[test]
fn path_component_encoding_is_pinned() {
    // tag byte + u64 BIG-endian length + payload; Index payload is an
    // 8-byte big-endian u64.
    assert_eq!(
        encode_path(&[P::Field("ab".into())]),
        vec![0x01, 0, 0, 0, 0, 0, 0, 0, 2, b'a', b'b']
    );
    assert_eq!(
        encode_path(&[P::Variant("V".into())]),
        vec![0x02, 0, 0, 0, 0, 0, 0, 0, 1, b'V']
    );
    assert_eq!(
        encode_path(&[P::Index(3)]),
        vec![0x03, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3]
    );
    assert_eq!(
        encode_path(&[P::MapKey(b"5".to_vec())]),
        vec![0x04, 0, 0, 0, 0, 0, 0, 0, 1, b'5']
    );
    // Components concatenate in order.
    assert_eq!(
        encode_path(&[P::Field("a".into()), P::Index(0)]),
        vec![
            0x01, 0, 0, 0, 0, 0, 0, 0, 1, b'a', 0x03, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0,
            0
        ]
    );
}
