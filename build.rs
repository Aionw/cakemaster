fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is not set")?);
    coro_rpc_codegen::Builder::new()
        .compile("idl/cakemaster.thrift", output.join("cakemaster_rpc.rs"))?;
    Ok(())
}
