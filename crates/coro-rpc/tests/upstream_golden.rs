//! Golden vectors generated from alibaba/yalantinglibs commit
//! c1cef74057b139944c982d840c09c9940f26e08e with
//! `interop/upstream_golden.cpp`.

use std::collections::{BTreeMap, BTreeSet};

use coro_rpc::impl_struct_pack;
use coro_rpc::struct_pack::{deserialize, serialize, type_hash, type_literal};

#[derive(Debug, PartialEq, Eq)]
struct Person {
    id: i32,
    name: String,
}
impl_struct_pack!(Person {
    id: i32,
    name: String
});

fn hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(text, 16).unwrap()
        })
        .collect()
}

#[test]
fn upstream_type_literals_and_hashes_match() {
    assert_eq!(type_literal::<i32>(), hex("01"));
    assert_eq!(type_hash::<i32>(), 0x55a5_4008);
    assert_eq!(type_literal::<String>(), hex("800c"));
    assert_eq!(type_hash::<String>(), 0x9dcf_fa76);
    assert_eq!(type_literal::<(i32, String)>(), hex("fd01800cff"));
    assert_eq!(type_hash::<(i32, String)>(), 0x85a8_fde6);
    assert_eq!(type_literal::<Person>(), hex("fd01800cff"));
    assert_eq!(type_hash::<Person>(), 0x85a8_fde6);
}

#[test]
fn rust_encoding_matches_upstream_release_build() {
    assert_eq!(serialize(&-42_i32).unwrap(), hex("0840a555d6ffffff"));
    assert_eq!(
        serialize(&"hello".to_owned()).unwrap(),
        hex("76facf9d0568656c6c6f")
    );
    assert_eq!(
        serialize(&(7_i32, "cake".to_owned())).unwrap(),
        hex("e6fda885070000000463616b65")
    );
    assert_eq!(
        serialize(&Person {
            id: 42,
            name: "Betty".to_owned(),
        })
        .unwrap(),
        hex("e6fda8852a000000054265747479")
    );
    assert_eq!(
        serialize(&Some("yes".to_owned())).unwrap(),
        hex("42055a020103796573")
    );
    assert_eq!(
        serialize(&Option::<String>::None).unwrap(),
        hex("42055a0200")
    );
    assert_eq!(serialize(&()).unwrap(), hex("eacf0189"));
    assert_eq!(
        serialize(&vec![1_i32, -2, 3]).unwrap(),
        hex("108d7c270301000000feffffff03000000")
    );
    assert_eq!(serialize(&true).unwrap(), hex("d8ffc81301"));
    assert_eq!(
        serialize(&0x0102_0304_0506_0708_u64).unwrap(),
        hex("7a7e7fec0807060504030201")
    );
    assert_eq!(
        serialize(&3.5_f64).unwrap(),
        hex("185644a80000000000000c40")
    );
    assert_eq!(serialize(&'🍰').unwrap(), hex("24b2ed4d70f30100"));
    assert_eq!(
        serialize(&[1_i16, -2, 3]).unwrap(),
        hex("eaef5a7a0100feff0300")
    );

    let map = BTreeMap::from([("a".to_owned(), 1_i32), ("b".to_owned(), 2_i32)]);
    assert_eq!(
        serialize(&map).unwrap(),
        hex("66e64e4d02016101000000016202000000")
    );
    let set = BTreeSet::from([-2_i32, 1, 3]);
    assert_eq!(
        serialize(&set).unwrap(),
        hex("1a5d71e203feffffff0100000003000000")
    );
    assert_eq!(
        serialize(&(1001_u16, "expected interop error".to_owned())).unwrap(),
        hex("4ce62b37e90316657870656374656420696e7465726f70206572726f72")
    );
}

#[test]
fn accepts_upstream_debug_type_metadata() {
    assert_eq!(
        deserialize::<i32>(&hex("0940a555040100d6ffffff")).unwrap(),
        -42
    );
    assert_eq!(
        deserialize::<String>(&hex("77facf9d04800c000568656c6c6f")).unwrap(),
        "hello"
    );
    assert_eq!(
        deserialize::<(i32, String)>(&hex("e7fda88504fd01800cff00070000000463616b65")).unwrap(),
        (7, "cake".to_owned())
    );
    assert_eq!(
        deserialize::<Person>(&hex("e7fda88504fd01800cff002a000000054265747479")).unwrap(),
        Person {
            id: 42,
            name: "Betty".to_owned(),
        }
    );
}

#[test]
fn large_container_metadata_matches_upstream() {
    let value = "x".repeat(300);
    let encoded = serialize(&value).unwrap();
    assert_eq!(&encoded[..7], &hex("77facf9d082c01"));
    assert_eq!(encoded.len(), 307);
    assert_eq!(deserialize::<String>(&encoded).unwrap(), value);
}
