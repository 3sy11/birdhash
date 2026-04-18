use std::process::Command;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=gpu-kernel/src/lib.rs");
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let src = format!("{}/gpu-kernel/src/lib.rs", manifest);
    let ptx_dst = PathBuf::from(&out_dir).join("kernel.ptx");

    let status = Command::new("rustc")
        .args([
            "+nightly",
            "--edition=2021",
            "--target=nvptx64-nvidia-cuda",
            "-C", "opt-level=3",
            "-C", "panic=abort",
            "--emit=asm",
            "--crate-type=cdylib",
            &src,
            "--out-dir", &out_dir,
        ])
        .status();

    match status {
        Ok(s) if s.success() => {
            // rustc --emit=asm 在 out_dir 生成 lib.s
            let asm = PathBuf::from(&out_dir).join("lib.s");
            if asm.exists() {
                std::fs::rename(&asm, &ptx_dst).expect("rename lib.s -> kernel.ptx");
                println!("cargo:rustc-env=GPU_KERNEL_AVAILABLE=1");
            }
        }
        _ => {
            // 编译失败时写一个空标记文件，运行时会优雅降级
            std::fs::write(&ptx_dst, "").ok();
            println!("cargo:warning=GPU kernel 编译失败，--gpu 将报错");
        }
    }
}
