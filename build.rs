use std::{env, path::PathBuf};

use anyhow::Result;
fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("tunnel_descriptor.bin"))
        .compile_protos(&["proto/tunnel.proto"], &["proto"])?;
    Ok(())
}
