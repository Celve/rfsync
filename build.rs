fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/rfsync.proto");
    println!("cargo:rerun-if-changed=build.rs");
    tonic_build::compile_protos("proto/rfsync.proto")?;
    Ok(())
}
