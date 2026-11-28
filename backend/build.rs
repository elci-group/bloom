fn main() {
    prost_build::compile_protos(&["bloom_backend.proto"], &["OUT_DIR"])
        .expect("Failed to compile proto files");
}