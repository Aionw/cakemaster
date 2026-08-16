#![allow(dead_code)]

use cakemaster::MOONCAKE_STORE_VERSION;
use cakemaster::mooncake::{
    ExpectedBool, ExpectedGetReplicaListResponse, ExpectedGetStorageConfigResponse, ExpectedI64,
    ExpectedPingResponse, ExpectedReplicaDescriptors, ExpectedString, ExpectedVoid,
    GetStorageConfigResponse, ObjectDataType, ObjectMeta, ReplicaType, ReplicateConfig, Segment,
    SoftPinAction, Uuid,
};
use coro_rpc::function_id;
use coro_rpc::struct_pack::{deserialize, serialize, type_hash, type_literal};

type SingleKeyRequest = (String, String);
type SingleExistsResponse = ExpectedBool;
type SingleGetResponse = ExpectedGetReplicaListResponse;
type BatchKeyRequest = (Vec<String>, String);
type BatchExistsResponse = Vec<ExpectedBool>;
type BatchGetResponse = Vec<ExpectedGetReplicaListResponse>;
type BatchPutStartRequest = (Uuid, Vec<String>, Vec<u64>, ReplicateConfig, String);
type BatchPutStartResponse = Vec<ExpectedReplicaDescriptors>;
type BatchPutEndRequest = (Uuid, Vec<ObjectMeta>, ReplicaType, String);
type BatchPutRevokeRequest = (Uuid, Vec<String>, ReplicaType, String);
type BatchVoidResponse = Vec<ExpectedVoid>;
type PingRequest = Uuid;
type RemountRequest = (Vec<Segment>, Uuid);
type MountSegmentRequest = (Segment, Uuid);
type UnmountSegmentRequest = (Uuid, Uuid);
type GracefulUnmountSegmentRequest = (Uuid, Uuid, u64);
type UpsertStartRequest = (Uuid, String, u64, ReplicateConfig, String);
type UpsertEndRequest = (Uuid, ObjectMeta, ReplicaType, String);
type UpsertRevokeRequest = (Uuid, String, ReplicaType, String);
type BatchUpsertEndRequest = (Uuid, Vec<ObjectMeta>, String);
type BatchUpsertRevokeRequest = (Uuid, Vec<String>, String);
type RemoveRequest = (String, bool, String);
type RemoveAllRequest = (bool, String);
type BatchRemoveRequest = (Vec<String>, bool, String);

#[test]
fn mooncake_rpc_schema_matches_yalantinglibs_metadata() {
    assert_type::<PingRequest>("fd04048989ff", 1_013_810_144);
    assert_eq!(type_hash::<ExpectedPingResponse>(), 1_948_946_258);
    assert_eq!(type_hash::<RemountRequest>(), 3_334_892_424);
    assert_type::<MountSegmentRequest>(
        "fdfdfd04048989ff800c0404800c800c800cfffd04048989ffff",
        2_666_991_022,
    );
    assert_type::<UnmountSegmentRequest>("fdfd04048989fffd04048989ffff", 3_569_685_216);
    assert_type::<GracefulUnmountSegmentRequest>("fdfd04048989fffd04048989ff04ff", 2_382_966_428);
    assert_eq!(type_hash::<ExpectedVoid>(), 2_938_661_068);
    assert_type::<GetStorageConfigResponse>("fd800c0b04ff", 4_262_655_338);
    assert_type::<ExpectedGetStorageConfigResponse>("87fd800c0b04ff01", 2_629_134_570);
    assert_type::<ExpectedString>("87800c01", 481_545_268);
    assert_type::<SingleKeyRequest>("fd800c800cff", 2_096_701_144);
    assert_type::<SingleExistsResponse>("870b01", 2_431_123_666);
    assert_type::<SingleGetResponse>(
        "87fd84fd0486fdfd0404800c800cfffffdfd0404800c800cfffffd800c04fffdfd04048989ff04800cffff01ff048504ff01",
        745_261_896,
    );
    assert_type::<BatchKeyRequest>("fd84800c800cff", 16_223_586);
    assert_type::<BatchExistsResponse>("84870b01", 710_904_924);
    assert_type::<BatchGetResponse>(
        "8487fd84fd0486fdfd0404800c800cfffffdfd0404800c800cfffffd800c04fffdfd04048989ff04800cffff01ff048504ff01",
        3_125_797_772,
    );
    assert_type::<BatchPutStartRequest>(
        "fdfd04048989ff84800c8404fd04040685040b84800c800c84800c0b06800c8584800cff800cff",
        1_937_958_152,
    );
    assert_type::<BatchPutStartResponse>(
        "848784fd0486fdfd0404800c800cfffffdfd0404800c800cfffffd800c04fffdfd04048989ff04800cffff01ff01",
        2_887_615_138,
    );
    assert_type::<BatchPutEndRequest>("fdfd04048989ff84fd800c8504ff01800cff", 4_118_718_408);
    assert_type::<BatchPutRevokeRequest>("fdfd04048989ff84800c01800cff", 2_498_649_412);
    assert_type::<BatchVoidResponse>("8487fa01", 972_837_582);
    assert_type::<UpsertStartRequest>(
        "fdfd04048989ff800c04fd04040685040b84800c800c84800c0b06800c8584800cff800cff",
        2_413_733_834,
    );
    assert_type::<UpsertEndRequest>("fdfd04048989fffd800c8504ff01800cff", 3_514_929_698);
    assert_type::<UpsertRevokeRequest>("fdfd04048989ff800c01800cff", 460_989_066);
    assert_eq!(type_hash::<BatchUpsertEndRequest>(), 3_706_271_504);
    assert_eq!(type_hash::<BatchUpsertRevokeRequest>(), 3_590_919_318);
    assert_type::<RemoveRequest>("fd800c0b800cff", 417_898_478);
    assert_type::<ExpectedI64>("870301", 3_090_383_358);
    assert_type::<RemoveAllRequest>("fd0b800cff", 1_218_888_308);
    assert_type::<BatchRemoveRequest>("fd84800c0b800cff", 427_465_888);
}

#[test]
fn mooncake_rpc_routes_match_wrapped_master_service() {
    assert_eq!(
        function_id("mooncake::WrappedMasterService::Ping"),
        3_603_094_245
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::ReMountSegment"),
        184_892_274
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::MountSegment"),
        1_048_495_291
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::UnmountSegment"),
        1_863_365_733
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::GracefulUnmountSegment"),
        2_766_942_581
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::ExistKey"),
        2_302_172_937
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::GetReplicaList"),
        528_413_044
    );
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
    assert_eq!(
        function_id("mooncake::WrappedMasterService::GetStorageConfig"),
        2_376_708_262
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::ServiceReady"),
        2_072_099_670
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::UpsertStart"),
        2_704_498_241
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::UpsertEnd"),
        773_907_032
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::UpsertRevoke"),
        1_628_111_918
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::BatchUpsertStart"),
        3_580_146_723
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::BatchUpsertEnd"),
        2_981_943_087
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::BatchUpsertRevoke"),
        2_425_597_561
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::Remove"),
        4_054_882_164
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::RemoveByRegex"),
        4_241_235_923
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::RemoveAll"),
        3_648_573_104
    );
    assert_eq!(
        function_id("mooncake::WrappedMasterService::BatchRemove"),
        1_493_319_805
    );
}

#[test]
fn mooncake_bootstrap_response_bytes_match_cpp_struct_pack() {
    let storage_config: ExpectedGetStorageConfigResponse = Ok(GetStorageConfigResponse {
        fsdir: String::new(),
        enable_disk_eviction: false,
        quota_bytes: 0,
    });
    let cpp_storage_config = hex("eb68b59c0487fd800c0b04ff01000100000000000000000000");
    assert_eq!(
        deserialize::<ExpectedGetStorageConfigResponse>(&cpp_storage_config).unwrap(),
        storage_config
    );
    assert_eq!(
        serialize(&storage_config).unwrap(),
        hex("ea68b59c0100000000000000000000")
    );

    let service_ready: ExpectedString = Ok(MOONCAKE_STORE_VERSION.to_owned());
    let cpp_service_ready = hex("35ccb31c0487800c01000105322e302e30");
    assert_eq!(
        deserialize::<ExpectedString>(&cpp_service_ready).unwrap(),
        service_ready
    );
    assert_eq!(
        serialize(&service_ready).unwrap(),
        hex("34ccb31c0105322e302e30")
    );
}

#[test]
fn latest_mooncake_put_start_bytes_decode_with_soft_pin_action() {
    let request = (
        Uuid { high: 1, low: 2 },
        vec!["benchmark-key-00000000".to_owned()],
        vec![4096],
        ReplicateConfig {
            replica_num: 1,
            nof_replica_num: 0,
            soft_pin_action: SoftPinAction::Enable,
            soft_pin_ttl_ms: Some(1234),
            with_hard_pin: false,
            preferred_segments: Vec::new(),
            preferred_segment: String::new(),
            preferred_nof_segments: Vec::new(),
            prefer_alloc_in_same_node: false,
            data_type: ObjectDataType::Kvcache,
            host_id: String::new(),
            group_ids: None,
        },
        "default".to_owned(),
    );
    let cpp_encoded = hex(
        "09e5827304fdfd04048989ff84800c8404fd04040685040b84800c800c84800c0b06800c8584800cff800cff0001000000000000000200000000000000011662656e63686d61726b2d6b65792d3030303030303030010010000000000000010000000000000000000000000000000101d20400000000000000000000000100000764656661756c74",
    );
    assert_eq!(
        deserialize::<BatchPutStartRequest>(&cpp_encoded).unwrap(),
        request
    );
    let rust_encoded = serialize(&request).unwrap();
    assert_eq!(
        rust_encoded,
        hex(
            "08e5827301000000000000000200000000000000011662656e63686d61726b2d6b65792d3030303030303030010010000000000000010000000000000000000000000000000101d20400000000000000000000000100000764656661756c74"
        )
    );
    assert_eq!(
        deserialize::<BatchPutStartRequest>(&rust_encoded).unwrap(),
        request
    );
    assert_eq!(
        serialize(&SoftPinAction::Preserve).unwrap(),
        serialize(&0_u8).unwrap()
    );
    assert_eq!(
        serialize(&SoftPinAction::Enable).unwrap(),
        serialize(&1_u8).unwrap()
    );
    assert_eq!(
        serialize(&SoftPinAction::Disable).unwrap(),
        serialize(&2_u8).unwrap()
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
