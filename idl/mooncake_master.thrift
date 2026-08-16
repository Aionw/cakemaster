namespace rs mooncake

enum ErrorCode {
  OK = 0
  INTERNAL_ERROR = -1
  BUFFER_OVERFLOW = -10
  SHARD_INDEX_OUT_OF_RANGE = -100
  SEGMENT_NOT_FOUND = -101
  SEGMENT_ALREADY_EXISTS = -102
  CLIENT_NOT_FOUND = -103
  NO_AVAILABLE_HANDLE = -200
  INVALID_VERSION = -300
  INVALID_KEY = -400
  WRITE_FAIL = -500
  INVALID_PARAMS = -600
  ILLEGAL_CLIENT = -601
  INVALID_WRITE = -700
  INVALID_READ = -701
  INVALID_REPLICA = -702
  REPLICA_IS_NOT_READY = -703
  OBJECT_NOT_FOUND = -704
  OBJECT_ALREADY_EXISTS = -705
  OBJECT_HAS_LEASE = -706
  LEASE_EXPIRED = -707
  OBJECT_HAS_REPLICATION_TASK = -708
  OBJECT_NO_REPLICATION_TASK = -709
  REPLICA_NOT_FOUND = -710
  REPLICA_ALREADY_EXISTS = -711
  REPLICA_IS_GONE = -712
  REPLICA_NOT_IN_LOCAL_MEMORY = -713
  OBJECT_REPLICA_BUSY = -714
  TRANSFER_FAIL = -800
  CHECKSUM_MISMATCH = -801
  RPC_FAIL = -900
  RPC_TIMEOUT = -901
  ETCD_OPERATION_ERROR = -1000
  ETCD_KEY_NOT_EXIST = -1001
  ETCD_TRANSACTION_FAIL = -1002
  ETCD_CTX_CANCELLED = -1003
  OPLOG_ENTRY_NOT_FOUND = -1004
  K8S_LEASE_OPERATION_ERROR = -1005
  K8S_LEASE_NOT_FOUND = -1006
  INCOMPLETE_OPLOG_CATCH_UP = -1007
  UNAVAILABLE_IN_CURRENT_STATUS = -1010
  UNAVAILABLE_IN_CURRENT_MODE = -1011
  FILE_NOT_FOUND = -1100
  FILE_OPEN_FAIL = -1101
  FILE_READ_FAIL = -1102
  FILE_WRITE_FAIL = -1103
  FILE_INVALID_BUFFER = -1104
  FILE_LOCK_FAIL = -1105
  FILE_INVALID_HANDLE = -1106
  BUCKET_NOT_FOUND = -1200
  BUCKET_ALREADY_EXISTS = -1201
  KEYS_EXCEED_BUCKET_LIMIT = -1202
  KEYS_ULTRA_LIMIT = -1203
  UNABLE_OFFLOAD = -1300
  UNABLE_OFFLOADING = -1301
  TASK_NOT_FOUND = -1400
  TASK_PENDING_LIMIT_EXCEEDED = -1401
  JOB_NOT_FOUND = -1402
  SERIALIZE_UNSUPPORTED = -1500
  SERIALIZE_FAIL = -1501
  DESERIALIZE_FAIL = -1502
  PERSISTENT_FAIL = -1503
  DFS_NETWORK_TIMEOUT = -1600
  DFS_SERVICE_UNAVAILABLE = -1601
  DFS_QUOTA_EXCEEDED = -1602
  DFS_PERMISSION_DENIED = -1603
  DFS_STALE_HANDLE = -1604
  DFS_PARTIAL_WRITE = -1605
  TENANT_QUOTA_EXCEEDED = -1700
  TENANT_NOT_REGISTERED = -1701
  TENANT_NOT_EMPTY = -1702
}

enum ObjectDataType {
  UNKNOWN = 0
  KVCACHE = 1
  TENSOR = 2
  WEIGHT = 3
  SAMPLE = 4
  ACTIVATION = 5
  GRADIENT = 6
  OPTIMIZER_STATE = 7
  METADATA = 8
  GENERAL = 9
} (coro_rpc.repr = "u8")

enum ReplicaType {
  MEMORY = 0
  DISK = 1
  LOCAL_DISK = 2
  NOF_SSD = 3
  ALL = 4
}

enum ReplicaStatus {
  UNDEFINED = 0
  INITIALIZED = 1
  PROCESSING = 2
  COMPLETE = 3
  REMOVED = 4
  FAILED = 5
}

enum SoftPinAction {
  PRESERVE = 0
  ENABLE = 1
  DISABLE = 2
} (coro_rpc.repr = "u8")

struct UUID {
  1: required u64 high
  2: required u64 low
} (coro_rpc.cpp_u64_pair)

enum ClientStatus {
  UNDEFINED = 0
  OK = 1
  NEED_REMOUNT = 2
}

struct Segment {
  1: required UUID id
  2: required string name
  3: required u64 base
  4: required u64 size
  5: required string te_endpoint
  6: required string protocol
  7: required string host_id
}

struct PingResponse {
  1: required i64 view_version_id
  2: required ClientStatus client_status
}

struct GetStorageConfigResponse {
  1: required string fsdir
  2: required bool enable_disk_eviction
  3: required u64 quota_bytes
}

struct BufferDescriptor {
  1: required u64 size
  2: required u64 buffer_address
  3: required string protocol
  4: required string transport_endpoint
}

struct MemoryDescriptor {
  1: required BufferDescriptor buffer_descriptor
}

struct NoFDescriptor {
  1: required BufferDescriptor buffer_descriptor
}

struct DiskDescriptor {
  1: required string file_path
  2: required u64 object_size
}

struct LocalDiskDescriptor {
  1: required UUID client_id
  2: required u64 object_size
  3: required string transport_endpoint
}

union DescriptorVariant {
  1: MemoryDescriptor memory
  2: NoFDescriptor nof_ssd
  3: DiskDescriptor disk
  4: LocalDiskDescriptor local_disk
}

struct ReplicaDescriptor {
  1: required u64 id
  2: required DescriptorVariant descriptor_variant
  3: required ReplicaStatus status
}

struct GetReplicaListResponse {
  1: required list<ReplicaDescriptor> replicas
  2: required u64 lease_ttl_ms
  3: optional u64 object_checksum
}

struct ObjectMeta {
  1: required string key
  2: optional u64 object_checksum
}

struct ReplicateConfig {
  1: required u64 replica_num
  2: required u64 nof_replica_num
  3: required SoftPinAction soft_pin_action
  4: optional u64 soft_pin_ttl_ms
  5: required bool with_hard_pin
  6: required list<string> preferred_segments
  7: required string preferred_segment
  8: required list<string> preferred_nof_segments
  9: required bool prefer_alloc_in_same_node
  10: required ObjectDataType data_type
  11: required string host_id
  12: optional list<string> group_ids
}

union ExpectedBool {
  1: bool value
  2: ErrorCode error
} (coro_rpc.expected)

union ExpectedGetReplicaListResponse {
  1: GetReplicaListResponse value
  2: ErrorCode error
} (coro_rpc.expected)

union ExpectedReplicaDescriptors {
  1: list<ReplicaDescriptor> value
  2: ErrorCode error
} (coro_rpc.expected)

union ExpectedPingResponse {
  1: PingResponse value
  2: ErrorCode error
} (coro_rpc.expected)

union ExpectedGetStorageConfigResponse {
  1: GetStorageConfigResponse value
  2: ErrorCode error
} (coro_rpc.expected)

union ExpectedString {
  1: string value
  2: ErrorCode error
} (coro_rpc.expected)

union ExpectedI64 {
  1: i64 value
  2: ErrorCode error
} (coro_rpc.expected)

union ExpectedVoid {
  1: ErrorCode error
} (coro_rpc.expected)

service WrappedMasterService {
  ExpectedPingResponse Ping(
    1: required UUID client_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::Ping")

  ExpectedGetStorageConfigResponse GetStorageConfig()
    (coro_rpc.name = "mooncake::WrappedMasterService::GetStorageConfig")

  ExpectedString ServiceReady()
    (coro_rpc.name = "mooncake::WrappedMasterService::ServiceReady")

  ExpectedVoid MountSegment(
    1: required Segment segment,
    2: required UUID client_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::MountSegment")

  ExpectedVoid ReMountSegment(
    1: required list<Segment> segments,
    2: required UUID client_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::ReMountSegment")

  ExpectedVoid UnmountSegment(
    1: required UUID segment_id,
    2: required UUID client_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::UnmountSegment")

  ExpectedVoid GracefulUnmountSegment(
    1: required UUID segment_id,
    2: required UUID client_id,
    3: required u64 grace_period_ms
  ) (coro_rpc.name = "mooncake::WrappedMasterService::GracefulUnmountSegment")

  ExpectedBool ExistKey(
    1: required string key,
    2: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::ExistKey")

  ExpectedGetReplicaListResponse GetReplicaList(
    1: required string key,
    2: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::GetReplicaList")

  list<ExpectedBool> BatchExistKey(
    1: required list<string> keys,
    2: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::BatchExistKey")

  list<ExpectedGetReplicaListResponse> BatchGetReplicaList(
    1: required list<string> keys,
    2: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::BatchGetReplicaList")

  list<ExpectedReplicaDescriptors> BatchPutStart(
    1: required UUID client_id,
    2: required list<string> keys,
    3: required list<u64> slice_lengths,
    4: required ReplicateConfig config,
    5: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::BatchPutStart")

  list<ExpectedVoid> BatchPutEnd(
    1: required UUID client_id,
    2: required list<ObjectMeta> object_metas,
    3: required ReplicaType replica_type,
    4: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::BatchPutEnd")

  list<ExpectedVoid> BatchPutRevoke(
    1: required UUID client_id,
    2: required list<string> keys,
    3: required ReplicaType replica_type,
    4: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::BatchPutRevoke")

  ExpectedReplicaDescriptors UpsertStart(
    1: required UUID client_id,
    2: required string key,
    3: required u64 slice_length,
    4: required ReplicateConfig config,
    5: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::UpsertStart")

  ExpectedVoid UpsertEnd(
    1: required UUID client_id,
    2: required ObjectMeta object_meta,
    3: required ReplicaType replica_type,
    4: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::UpsertEnd")

  ExpectedVoid UpsertRevoke(
    1: required UUID client_id,
    2: required string key,
    3: required ReplicaType replica_type,
    4: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::UpsertRevoke")

  list<ExpectedReplicaDescriptors> BatchUpsertStart(
    1: required UUID client_id,
    2: required list<string> keys,
    3: required list<u64> slice_lengths,
    4: required ReplicateConfig config,
    5: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::BatchUpsertStart")

  list<ExpectedVoid> BatchUpsertEnd(
    1: required UUID client_id,
    2: required list<ObjectMeta> object_metas,
    3: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::BatchUpsertEnd")

  list<ExpectedVoid> BatchUpsertRevoke(
    1: required UUID client_id,
    2: required list<string> keys,
    3: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::BatchUpsertRevoke")

  ExpectedVoid Remove(
    1: required string key,
    2: required bool force,
    3: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::Remove")

  ExpectedI64 RemoveByRegex(
    1: required string regex,
    2: required bool force,
    3: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::RemoveByRegex")

  i64 RemoveAll(
    1: required bool force,
    2: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::RemoveAll")

  list<ExpectedVoid> BatchRemove(
    1: required list<string> keys,
    2: required bool force,
    3: required string tenant_id
  ) (coro_rpc.name = "mooncake::WrappedMasterService::BatchRemove")
}
