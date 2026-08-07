use std::fs;

use coro_rpc_codegen::{Builder, CodegenError};

#[test]
fn generates_typed_client_server_and_struct_pack_models() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("demo.thrift");
    let output = directory.path().join("demo.rs");
    fs::write(
        &input,
        r#"
            namespace rs generated.api
            namespace cpp demo

            typedef i64 UserId

            struct User {
              2: optional list<string> tags
              1: required UserId id
            }

            service DemoService {
              User lookup(1: required UserId id)
                (coro_rpc.name = "existing::lookup")
              void upload(1: required binary body)
                (coro_rpc.attachment = "true")
            }
        "#,
    )
    .unwrap();

    Builder::new().compile(&input, &output).unwrap();
    let generated = fs::read_to_string(output).unwrap();
    assert!(generated.contains("pub mod generated"));
    assert!(generated.contains("pub mod api"));
    assert!(generated.contains("pub struct User"));
    assert!(generated.contains("pub struct DemoServiceClient"));
    assert!(generated.contains("pub trait DemoService"));
    assert!(generated.contains("pub struct DemoServiceServer"));
    assert!(generated.contains("existing::lookup"));
    assert!(generated.contains("attachment: impl"));
    assert!(generated.contains("context: ::coro_rpc::RequestContext"));
    let id_position = generated.find("pub id:").unwrap();
    let tags_position = generated.find("pub tags:").unwrap();
    assert!(id_position < tags_position, "field IDs define wire order");
}

#[test]
fn generates_i32_backed_struct_pack_enums() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("enum.thrift");
    let output = directory.path().join("enum.rs");
    fs::write(
        &input,
        r#"
            enum ErrorCode {
              OK,
              INTERNAL_ERROR = -1,
              OBJECT_NOT_FOUND = -0x2c0,
              NEXT_ERROR,
            }

            struct Status {
              1: required ErrorCode code
              2: required set<ErrorCode> history
            }

            service ErrorService {
              ErrorCode echo(1: required ErrorCode code)
            }
        "#,
    )
    .unwrap();

    Builder::new().compile(&input, &output).unwrap();
    let generated = fs::read_to_string(output).unwrap();
    assert!(generated.contains("pub enum ErrorCode"));
    assert!(generated.contains("Ok = 0"));
    assert!(generated.contains("InternalError = -1"));
    assert!(generated.contains("ObjectNotFound = -704"));
    assert!(generated.contains("NextError = -703"));
    assert!(generated.contains("impl ::coro_rpc::StructPack for ErrorCode"));
    assert!(generated.contains("BTreeSet<ErrorCode>"));
}

#[test]
fn generates_unsigned_u8_enums_and_expected_business_types() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("mooncake-types.thrift");
    let output = directory.path().join("mooncake-types.rs");
    fs::write(
        &input,
        r#"
            enum ErrorCode {
              OK = 0
              INVALID_PARAMS = -600
            }

            enum ObjectDataType {
              UNKNOWN = 0
              KVCACHE = 1
            } (coro_rpc.repr = "u8")

            union ExpectedBool {
              1: bool value
              2: ErrorCode error
            } (coro_rpc.expected)

            union ExpectedVoid {
              1: ErrorCode error
            } (coro_rpc.expected = "true")

            struct UUID {
              1: required u64 high
              2: required u64 low
            } (coro_rpc.cpp_u64_pair)

            struct WireValues {
              1: required u8 tag
              2: required u16 small
              3: required u32 medium
              4: required u64 large
              5: required ObjectDataType data_type
              6: required UUID client_id
            }

            service ExpectedService {
              list<ExpectedBool> check(1: required WireValues values)
              list<ExpectedVoid> finish(1: required list<u64> ids)
            }
        "#,
    )
    .unwrap();

    Builder::new().compile(&input, &output).unwrap();
    let generated = fs::read_to_string(output).unwrap();
    assert!(generated.contains("#[repr(u8)]"));
    assert!(generated.contains("pub tag: u8"));
    assert!(generated.contains("pub small: u16"));
    assert!(generated.contains("pub medium: u32"));
    assert!(generated.contains("pub large: u64"));
    assert!(generated.contains("pub type ExpectedBool = ::core::result::Result<bool, ErrorCode>"));
    assert!(generated.contains("pub type ExpectedVoid = ::core::result::Result<(), ErrorCode>"));
    assert!(generated.contains("output.extend_from_slice(&[137, 137])"));
}

#[test]
fn generates_struct_pack_variants_from_thrift_unions() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("union.thrift");
    let output = directory.path().join("union.rs");
    fs::write(
        &input,
        r#"
            struct MemoryDescriptor {
              1: required i64 address
            }

            struct DiskDescriptor {
              1: required string path
            }

            union DescriptorVariant {
              2: DiskDescriptor disk
              1: MemoryDescriptor memory
            }

            struct ReplicaDescriptor {
              1: required DescriptorVariant descriptor
            }

            service DescriptorService {
              ReplicaDescriptor echo(1: required ReplicaDescriptor descriptor)
            }
        "#,
    )
    .unwrap();

    Builder::new().compile(&input, &output).unwrap();
    let generated = fs::read_to_string(output).unwrap();
    assert!(generated.contains("pub enum DescriptorVariant"));
    assert!(generated.contains("Memory(MemoryDescriptor)"));
    assert!(generated.contains("Disk(DiskDescriptor)"));
    assert!(generated.contains("TYPE_VARIANT"));
    let memory_position = generated.find("Memory(MemoryDescriptor)").unwrap();
    let disk_position = generated.find("Disk(DiskDescriptor)").unwrap();
    assert!(
        memory_position < disk_position,
        "field IDs define std::variant index order"
    );
}

#[test]
fn rejects_contracts_that_struct_pack_cannot_represent() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("bad.thrift");
    let output = directory.path().join("bad.rs");
    fs::write(
        &input,
        r#"
            struct Broken {
              1: required i32 first
              1: required i32 second
            }
            service BrokenService {
              Broken get()
            }
        "#,
    )
    .unwrap();

    let error = Builder::new().compile(&input, output).unwrap_err();
    assert!(
        matches!(error, CodegenError::InvalidContract { .. }),
        "unexpected error: {error}"
    );
    assert!(error.to_string().contains("duplicate field ID 1"));
}

#[test]
fn rejects_duplicate_enum_values() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("duplicate-enum.thrift");
    let output = directory.path().join("duplicate-enum.rs");
    fs::write(
        &input,
        r#"
            enum ErrorCode {
              FIRST = -1,
              SECOND = -1,
            }
            service ErrorService {
              ErrorCode get()
            }
        "#,
    )
    .unwrap();

    let error = Builder::new().compile(&input, output).unwrap_err();
    assert!(matches!(error, CodegenError::InvalidContract { .. }));
    assert!(error.to_string().contains("both use value -1"));
}

#[test]
fn rejects_non_contiguous_union_field_ids() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("gapped-union.thrift");
    let output = directory.path().join("gapped-union.rs");
    fs::write(
        &input,
        r#"
            union BrokenVariant {
              1: i32 first
              3: string third
            }
            service BrokenService {
              BrokenVariant get()
            }
        "#,
    )
    .unwrap();

    let error = Builder::new().compile(&input, output).unwrap_err();
    assert!(matches!(error, CodegenError::InvalidContract { .. }));
    assert!(error.to_string().contains("must be contiguous from 1"));
}

#[test]
fn rejects_union_field_modifiers() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("modified-union.thrift");
    let output = directory.path().join("modified-union.rs");
    fs::write(
        &input,
        r#"
            union BrokenVariant {
              1: optional i32 value
            }
            service BrokenService {
              BrokenVariant get()
            }
        "#,
    )
    .unwrap();

    let error = Builder::new().compile(&input, output).unwrap_err();
    assert!(matches!(error, CodegenError::InvalidContract { .. }));
    assert!(
        error
            .to_string()
            .contains("must not use required or optional modifiers")
    );
}

#[test]
fn rejects_malformed_expected_unions() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("malformed-expected.thrift");
    let output = directory.path().join("malformed-expected.rs");
    fs::write(
        &input,
        r#"
            enum ErrorCode { OK = 0 }
            union BrokenExpected {
              1: bool success
              2: ErrorCode failure
            } (coro_rpc.expected)
            service BrokenService {
              BrokenExpected get()
            }
        "#,
    )
    .unwrap();

    let error = Builder::new().compile(&input, output).unwrap_err();
    assert!(matches!(error, CodegenError::InvalidContract { .. }));
    assert!(error.to_string().contains("expected union BrokenExpected"));
}

#[test]
fn reports_thrift_syntax_errors_before_rust_generation() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("invalid.thrift");
    let output = directory.path().join("invalid.rs");
    fs::write(
        &input,
        r#"
            service BrokenService {
              string unfinished(1: required string value
            }
        "#,
    )
    .unwrap();

    let error = Builder::new().compile(&input, output).unwrap_err();
    let (line, column) = match &error {
        CodegenError::Parse { line, column, .. } => (*line, *column),
        _ => panic!("unexpected error: {error}"),
    };
    assert!(line > 0 && column > 0);
    assert!(error.to_string().contains("invalid.thrift:"));
}

#[test]
fn rejects_container_types_that_cannot_use_rust_btree_collections() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("unordered.thrift");
    let output = directory.path().join("unordered.rs");
    fs::write(
        &input,
        r#"
            struct Measurements {
              1: required set<double> values
            }
            service MeasurementService {
              Measurements get()
            }
        "#,
    )
    .unwrap();

    let error = Builder::new().compile(&input, output).unwrap_err();
    assert!(matches!(error, CodegenError::InvalidContract { .. }));
    assert!(error.to_string().contains("must implement Rust Ord"));
}
