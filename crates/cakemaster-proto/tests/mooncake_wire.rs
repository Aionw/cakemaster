#![allow(dead_code)]

use cakemaster_proto::mooncake::{
    ExpectedBool, ExpectedGetReplicaListResponse, ExpectedReplicaDescriptors, ExpectedVoid,
    ObjectDataType, ObjectMeta, ReplicaType, ReplicateConfig, Uuid,
};
use coro_rpc::function_id;
use coro_rpc::struct_pack::{deserialize, serialize, type_hash, type_literal};

type BatchKeyRequest = (Vec<String>, String);
type BatchExistsResponse = Vec<ExpectedBool>;
type BatchGetResponse = Vec<ExpectedGetReplicaListResponse>;
type BatchPutStartRequest = (Uuid, Vec<String>, Vec<u64>, ReplicateConfig, String);
type BatchPutStartResponse = Vec<ExpectedReplicaDescriptors>;
type BatchPutEndRequest = (Uuid, Vec<ObjectMeta>, ReplicaType, String);
type BatchPutRevokeRequest = (Uuid, Vec<String>, ReplicaType, String);
type BatchVoidResponse = Vec<ExpectedVoid>;

#[test]
fn mooncake_rpc_schema_matches_yalantinglibs_metadata() {
    assert_type::<BatchKeyRequest>("fd84800c800cff", 16_223_586);
    assert_type::<BatchExistsResponse>("84870b01", 710_904_924);
    assert_type::<BatchGetResponse>(
        "8487fd84fd0486fdfd0404800c800cfffffdfd0404800c800cfffffd800c04fffdfd04048989ff04800cffff01ff048504ff01",
        3_125_797_772,
    );
    assert_type::<BatchPutStartRequest>(
        "fdfd04048989ff84800c8404fd04040b0b84800c800c84800c0b06800c8584800cff800cff",
        2_152_225_908,
    );
    assert_type::<BatchPutStartResponse>(
        "848784fd0486fdfd0404800c800cfffffdfd0404800c800cfffffd800c04fffdfd04048989ff04800cffff01ff01",
        2_887_615_138,
    );
    assert_type::<BatchPutEndRequest>("fdfd04048989ff84fd800c8504ff01800cff", 4_118_718_408);
    assert_type::<BatchPutRevokeRequest>("fdfd04048989ff84800c01800cff", 2_498_649_412);
    assert_type::<BatchVoidResponse>("8487fa01", 972_837_582);
}

#[test]
fn mooncake_rpc_routes_match_wrapped_master_service() {
    assert_eq!(
        function_id("mooncake::WrappedMasterService::BatchExistKey"),
        3_097_470_640
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::BatchGetReplicaList"),
        453_845_247
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::BatchPutStart"),
        1_455_298_594
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::BatchPutEnd"),
        2_162_172_075
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::BatchPutRevoke"),
        3_089_459_114
    );
}

#[test]
fn mooncake_put_end_bytes_match_cpp_struct_pack() {
    let request = (
        Uuid { high: 1, low: 2 },
        vec![ObjectMeta {
            key: "benchmark-key-00000000".to_owned(),
            object_checksum: None,
        }],
        ReplicaType::All,
        "default".to_owned(),
    );
    let cpp_encoded = hex(
        "c9a77ef504fdfd04048989ff84fd800c8504ff01800cff0001000000000000000200000000000000011662656e63686d61726b2d6b65792d303030303030303000040000000764656661756c74",
    );
    assert_eq!(
        deserialize::<BatchPutEndRequest>(&cpp_encoded).unwrap(),
        request
    );

    let rust_encoded = serialize(&request).unwrap();
    assert_eq!(
        rust_encoded,
        hex(
            "c8a77ef501000000000000000200000000000000011662656e63686d61726b2d6b65792d303030303030303000040000000764656661756c74"
        )
    );
    assert_eq!(
        deserialize::<BatchPutEndRequest>(&rust_encoded).unwrap(),
        request
    );

    assert_eq!(
        serialize(&ObjectDataType::Kvcache).unwrap(),
        serialize(&1_u8).unwrap()
    );
}

fn assert_type<T: coro_rpc::StructPack>(literal: &str, hash: u32) {
    assert_eq!(type_literal::<T>(), hex(literal));
    assert_eq!(type_hash::<T>(), hash);
}

fn hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(pair, 16).unwrap()
        })
        .collect()
}
