namespace rs api

typedef i64 UserId

enum ErrorCode {
  OK = 0
  INTERNAL_ERROR = -1
  OBJECT_NOT_FOUND = -704
}

struct User {
  1: required UserId id
  2: required string name
  3: optional list<string> tags
  4: optional map<string, i32> counters
  5: optional set<i16> levels
}

struct MemoryDescriptor {
  1: required i64 address
}

struct NoFDescriptor {
  1: required i64 address
}

struct DiskDescriptor {
  1: required string path
  2: required i64 object_size
}

struct LocalDiskDescriptor {
  1: required i64 client_id_high
  2: required i64 client_id_low
  3: required string transport_endpoint
}

union DescriptorVariant {
  1: MemoryDescriptor memory
  2: NoFDescriptor nof_ssd
  3: DiskDescriptor disk
  4: LocalDiskDescriptor local_disk
}

struct ReplicaDescriptor {
  1: required i64 id
  2: required DescriptorVariant descriptor_variant
  3: required i32 status
}

service DemoService {
  string echo(1: required string value)
  i32 add(1: required i32 left, 2: required i32 right)
  ErrorCode echo_error(1: required ErrorCode error)
  ReplicaDescriptor echo_descriptor(1: required ReplicaDescriptor descriptor)
  string ping()
  void fail()
  void attachment_echo() (coro_rpc.attachment = "true")
}
