fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is not set")?);
    let builder = coro_rpc_codegen::Builder::new();
    builder.compile("idl/cakemaster.thrift", output.join("cakemaster_rpc.rs"))?;
    builder.compile(
        "idl/mooncake_master.thrift",
        output.join("mooncake_master_rpc.rs"),
    )?;
    Ok(())
}
