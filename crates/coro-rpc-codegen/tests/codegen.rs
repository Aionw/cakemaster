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
