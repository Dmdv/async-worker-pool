use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let root_dir = PathBuf::from(manifest_dir).join("../..");

    println!("cargo:rustc-link-search=native={}", root_dir.display());
    println!("cargo:rustc-link-lib=static=awp");
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rerun-if-changed={}/libawp.a", root_dir.display());
    println!("cargo:rerun-if-changed={}/include/awp/awp.h", root_dir.display());
}
