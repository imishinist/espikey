fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("grpc/espikey.proto")?;

    println!("cargo:rerun-if-changed=grpc");
    Ok(())
}
