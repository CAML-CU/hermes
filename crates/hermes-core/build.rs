use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let files: Vec<_> = ["common", "bot", "wrangler", "services", "orders"]
        .iter()
        .map(|name| proto_root.join(format!("trading/v1/{name}.proto")))
        .collect();
    for file in &files {
        println!("cargo:rerun-if-changed={}", file.display());
    }
    prost_build::compile_protos(&files, &[proto_root])?;
    Ok(())
}
