//! GPU 碰撞器：CPU 计算 BIP39 seed（PBKDF2），GPU 批量执行 BIP32+secp256k1+Keccak
//! 通过 CUDA Driver API（nvcuda.dll）加载 PTX kernel，无需 nvcc/CUDA Toolkit

use anyhow::{Context, Result};
use libloading::Library;
use rayon::prelude::*;
use std::ffi::c_void;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::collider::{
    append_hit, contains_bf, ensure_hits_csv, load_all_bf, load_checkpoint,
    load_derivation_candidates, load_or_create_seed, path_index_to_account_index,
    paths_per_id, save_checkpoint,
};
use crate::config::AppConfig;

// ── CUDA Driver API 类型 ───────────────────────────────────────────────────

type CUresult = u32;
type CUdevice = i32;
type CUdeviceptr = u64;
type CUmodule = usize;
type CUfunction = usize;
const CUDA_SUCCESS: u32 = 0;

const PTX: &str = include_str!(concat!(env!("OUT_DIR"), "/kernel.ptx"));
const BLOCK_SIZE: u32 = 256;
const BATCH_SEEDS: usize = 512; // 每轮 GPU launch 处理的 seed 数

// ── CUDA Driver API 加载 ──────────────────────────────────────────────────

macro_rules! load_sym {
    ($lib:expr, $name:literal, $T:ty) => {{
        let f: libloading::Symbol<$T> = unsafe { $lib.get($name) }
            .with_context(|| concat!("未找到 CUDA 函数: ", stringify!($name)))?;
        *f
    }};
}

type FnInit = unsafe extern "C" fn(u32) -> CUresult;
type FnDevGet = unsafe extern "C" fn(*mut CUdevice, i32) -> CUresult;
type FnCtxCreate = unsafe extern "C" fn(*mut usize, u32, CUdevice) -> CUresult;
type FnModLoadData = unsafe extern "C" fn(*mut CUmodule, *const c_void) -> CUresult;
type FnModGetFunc = unsafe extern "C" fn(*mut CUfunction, CUmodule, *const i8) -> CUresult;
type FnMemAlloc = unsafe extern "C" fn(*mut CUdeviceptr, usize) -> CUresult;
type FnMemcpyHD = unsafe extern "C" fn(CUdeviceptr, *const c_void, usize) -> CUresult;
type FnMemcpyDH = unsafe extern "C" fn(*mut c_void, CUdeviceptr, usize) -> CUresult;
type FnLaunch = unsafe extern "C" fn(CUfunction, u32, u32, u32, u32, u32, u32, u32, usize, *mut *mut c_void, *mut *mut c_void) -> CUresult;
type FnSync = unsafe extern "C" fn() -> CUresult;
type FnMemFree = unsafe extern "C" fn(CUdeviceptr) -> CUresult;
type FnGetErrStr = unsafe extern "C" fn(CUresult, *mut *const i8) -> CUresult;

struct CudaApi {
    _lib: Library,
    cu_init: FnInit,
    cu_device_get: FnDevGet,
    cu_ctx_create: FnCtxCreate,
    cu_module_load_data: FnModLoadData,
    cu_module_get_function: FnModGetFunc,
    cu_mem_alloc: FnMemAlloc,
    cu_memcpy_hd: FnMemcpyHD,
    cu_memcpy_dh: FnMemcpyDH,
    cu_launch_kernel: FnLaunch,
    cu_ctx_synchronize: FnSync,
    cu_mem_free: FnMemFree,
    cu_get_error_string: FnGetErrStr,
}

impl CudaApi {
    fn load() -> Result<Self> {
        let lib = unsafe { Library::new("nvcuda.dll") }
            .context("无法加载 nvcuda.dll，请确认 NVIDIA 驱动已安装")?;
        Ok(Self {
            cu_init: load_sym!(lib, b"cuInit\0", FnInit),
            cu_device_get: load_sym!(lib, b"cuDeviceGet\0", FnDevGet),
            cu_ctx_create: load_sym!(lib, b"cuCtxCreate\0", FnCtxCreate),
            cu_module_load_data: load_sym!(lib, b"cuModuleLoadData\0", FnModLoadData),
            cu_module_get_function: load_sym!(lib, b"cuModuleGetFunction\0", FnModGetFunc),
            cu_mem_alloc: load_sym!(lib, b"cuMemAlloc\0", FnMemAlloc),
            cu_memcpy_hd: load_sym!(lib, b"cuMemcpyHtoD\0", FnMemcpyHD),
            cu_memcpy_dh: load_sym!(lib, b"cuMemcpyDtoH\0", FnMemcpyDH),
            cu_launch_kernel: load_sym!(lib, b"cuLaunchKernel\0", FnLaunch),
            cu_ctx_synchronize: load_sym!(lib, b"cuCtxSynchronize\0", FnSync),
            cu_mem_free: load_sym!(lib, b"cuMemFree\0", FnMemFree),
            cu_get_error_string: load_sym!(lib, b"cuGetErrorString\0", FnGetErrStr),
            _lib: lib,
        })
    }
    fn err_str(&self, code: CUresult) -> String {
        let mut p: *const i8 = core::ptr::null();
        unsafe { (self.cu_get_error_string)(code, &mut p) };
        if p.is_null() { return format!("CUDA 错误 {}", code); }
        let cstr = unsafe { std::ffi::CStr::from_ptr(p) };
        cstr.to_string_lossy().into_owned()
    }
}

macro_rules! cu {
    ($api:expr, $call:expr) => {{
        let r = unsafe { $call };
        if r != CUDA_SUCCESS { anyhow::bail!("CUDA 错误: {}", $api.err_str(r)); }
    }};
}

// ── DeviceBuffer RAII ─────────────────────────────────────────────────────

struct DevBuf<'a> {
    api: &'a CudaApi,
    ptr: CUdeviceptr,
}
impl<'a> DevBuf<'a> {
    fn alloc(api: &'a CudaApi, bytes: usize) -> Result<Self> {
        let mut ptr = 0u64;
        cu!(api, (api.cu_mem_alloc)(&mut ptr, bytes));
        Ok(Self { api, ptr })
    }
    fn copy_from(&self, src: &[u8]) -> Result<()> {
        cu!(self.api, (self.api.cu_memcpy_hd)(self.ptr, src.as_ptr() as *const c_void, src.len()));
        Ok(())
    }
    fn copy_to(&self, dst: &mut [u8]) -> Result<()> {
        cu!(self.api, (self.api.cu_memcpy_dh)(dst.as_mut_ptr() as *mut c_void, self.ptr, dst.len()));
        Ok(())
    }
}
impl Drop for DevBuf<'_> {
    fn drop(&mut self) { unsafe { (self.api.cu_mem_free)(self.ptr) }; }
}

// ── 主入口 ────────────────────────────────────────────────────────────────

pub fn run_gpu_collider(cfg: &AppConfig, num_cpu_threads: usize) -> Result<()> {
    anyhow::ensure!(!PTX.is_empty(), "GPU kernel PTX 为空，请检查编译");
    cfg.ensure_dirs()?;

    let seed_key = load_or_create_seed(&cfg.generator_seed_path())?;
    let candidates = Arc::new(load_derivation_candidates(&cfg.derivation_candidates_path())?);
    let paths_per = paths_per_id(&candidates);
    let hits_csv = cfg.hits_bf_csv_path();
    let checkpoint_path = cfg.collider_cursor_path();
    ensure_hits_csv(&hits_csv)?;

    let filter_dir = crate::collider::resolve_bf_dir(cfg)?;
    let bf_filters = load_all_bf(&filter_dir)?;
    anyhow::ensure!(!bf_filters.is_empty(), "未找到 BF 过滤器，请先 fetch + build-filter");
    let bf = Arc::new(bf_filters);
    // 预计算所有路径的 (account, index) 列表（供 GPU kernel 使用）
    let n_paths = paths_per as u32;
    let mut accounts_host = vec![0u32; n_paths as usize];
    let mut indices_host = vec![0u32; n_paths as usize];
    for pi in 0..paths_per {
        let (acc, idx) = path_index_to_account_index(pi, &candidates);
        accounts_host[pi as usize] = acc;
        indices_host[pi as usize] = idx;
    }

    // ── 初始化 CUDA ────────────────────────────────────────────────────────
    let api = CudaApi::load()?;
    cu!(api, (api.cu_init)(0));
    let mut dev: CUdevice = 0;
    cu!(api, (api.cu_device_get)(&mut dev, 0));
    let mut ctx: usize = 0;
    cu!(api, (api.cu_ctx_create)(&mut ctx, 0, dev));
    log::info!("CUDA 设备初始化完成，ctx={:#x}", ctx);

    // ── 加载 PTX kernel ────────────────────────────────────────────────────
    let ptx_c = std::ffi::CString::new(PTX).context("PTX 含空字节")?;
    let mut module: CUmodule = 0;
    cu!(api, (api.cu_module_load_data)(&mut module, ptx_c.as_ptr() as *const c_void));
    let func_name = std::ffi::CString::new("derive_addresses").unwrap();
    let mut func: CUfunction = 0;
    cu!(api, (api.cu_module_get_function)(&mut func, module, func_name.as_ptr()));
    log::info!("GPU kernel 加载完成 | {} 条路径/seed", paths_per);

    // ── 上传固定数据（paths 不变，只需上传一次） ──────────────────────────
    let acc_bytes: Vec<u8> = accounts_host.iter().flat_map(|v| v.to_le_bytes()).collect();
    let idx_bytes: Vec<u8> = indices_host.iter().flat_map(|v| v.to_le_bytes()).collect();
    let d_accounts = DevBuf::alloc(&api, acc_bytes.len())?;
    let d_indices  = DevBuf::alloc(&api, idx_bytes.len())?;
    d_accounts.copy_from(&acc_bytes)?;
    d_indices.copy_from(&idx_bytes)?;

    // ── 主循环 ─────────────────────────────────────────────────────────────
    let start_n = load_checkpoint(&checkpoint_path);
    let total_hits = AtomicU64::new(0);
    let total_gen  = AtomicU64::new(0);
    let start = Instant::now();
    println!("  GPU 碰撞器启动 | BF {} 个 | 路径/ID={} | CPU PBKDF2 线程={} | 断点 N={}",
        bf.len(), paths_per, num_cpu_threads, start_n);
    println!("  命中 → {} | 检查点 → {}", hits_csv.display(), checkpoint_path.display());

    let pool = rayon::ThreadPoolBuilder::new().num_threads(num_cpu_threads).build()?;
    let mut n = start_n;

    // 预分配 GPU 缓冲区（按 BATCH_SEEDS 固定大小）
    let seeds_bytes = BATCH_SEEDS * 64;
    let addrs_bytes = BATCH_SEEDS * n_paths as usize * 20;
    let d_seeds   = DevBuf::alloc(&api, seeds_bytes)?;
    let d_out     = DevBuf::alloc(&api, addrs_bytes)?;

    loop {
        let batch_start_id = n / paths_per;
        // CPU 并行计算 BATCH_SEEDS 个 BIP39 seed
        let seeds_flat: Vec<u8> = pool.install(|| {
            (0..BATCH_SEEDS).into_par_iter().flat_map(|i| {
                let id = batch_start_id + i as u64;
                match id_to_bip32_seed(&seed_key, id) {
                    Ok(s) => s.to_vec(),
                    Err(_) => vec![0u8; 64],
                }
            }).collect()
        });

        // 上传 seeds → GPU
        d_seeds.copy_from(&seeds_flat)?;

        // 启动 kernel
        let total_threads = BATCH_SEEDS as u32 * n_paths;
        let grid = (total_threads + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let mut p0: u64 = d_seeds.ptr;
        let mut p1: u64 = d_accounts.ptr;
        let mut p2: u64 = d_indices.ptr;
        let mut p3: u32 = BATCH_SEEDS as u32;
        let mut p4: u32 = n_paths;
        let mut p5: u64 = d_out.ptr;
        let mut params: [*mut c_void; 6] = [
            &mut p0 as *mut u64 as _,
            &mut p1 as *mut u64 as _,
            &mut p2 as *mut u64 as _,
            &mut p3 as *mut u32 as _,
            &mut p4 as *mut u32 as _,
            &mut p5 as *mut u64 as _,
        ];
        cu!(api, (api.cu_launch_kernel)(func, grid, 1, 1, BLOCK_SIZE, 1, 1, 0, 0, params.as_mut_ptr(), core::ptr::null_mut()));
        cu!(api, (api.cu_ctx_synchronize)());

        // 下载地址
        let mut addrs_out = vec![0u8; addrs_bytes];
        d_out.copy_to(&mut addrs_out)?;
        total_gen.fetch_add(total_threads as u64, Ordering::Relaxed);

        // CPU BF 过滤（使用三指纹精确检查）
        let bf_ref = bf.as_slice();
        let csv_path = &hits_csv;
        let hits_this_batch = std::sync::Mutex::new(0u64);
        (0..total_threads as usize).for_each(|i| {
            let addr_slice = &addrs_out[i*20..(i+1)*20];
            let mut addr = [0u8; 20];
            addr.copy_from_slice(addr_slice);
            if contains_bf(bf_ref, &addr) {
                // 反推 seed_idx/path_idx
                let seed_idx = i / n_paths as usize;
                let path_idx = i % n_paths as usize;
                let id = batch_start_id + seed_idx as u64;
                if let Ok((phrase, seed)) = id_to_mnemonic_and_seed(&seed_key, id) {
                    let (acc, idx) = path_index_to_account_index(path_idx as u64, &candidates);
                    let path_str = format!("m/44'/60'/{}'/0'/{}", acc, idx);
                    // 私钥需 CPU 重新派生（GPU 输出的是地址，不含私钥）
                    if let Ok(privkey) = derive_privkey_from_seed(&seed, acc, idx) {
                        let _ = append_hit(csv_path, &addr, &privkey, &path_str, &phrase);
                        *hits_this_batch.lock().unwrap() += 1;
                    }
                }
            }
        });
        let batch_hits = *hits_this_batch.lock().unwrap();
        total_hits.fetch_add(batch_hits, Ordering::Relaxed);

        n += BATCH_SEEDS as u64 * paths_per;
        let _ = save_checkpoint(&checkpoint_path, n);

        let elapsed = start.elapsed().as_secs_f64();
        let rate = total_gen.load(Ordering::Relaxed) as f64 / elapsed.max(0.001);
        print!("\r  N={} | ID={} | GPU速度 {:.0}/s | 已生成 {} | 命中 {}  ",
            n, batch_start_id + BATCH_SEEDS as u64,
            rate, total_gen.load(Ordering::Relaxed), total_hits.load(Ordering::Relaxed));
        let _ = std::io::stdout().flush();
    }
}

// ── 辅助函数 ─────────────────────────────────────────────────────────────

fn id_to_bip32_seed(seed_key: &[u8; 32], id: u64) -> Result<[u8; 64]> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(seed_key).unwrap();
    mac.update(&id.to_le_bytes());
    let h1 = mac.finalize().into_bytes();
    let mut mac = HmacSha256::new_from_slice(seed_key).unwrap();
    mac.update(&h1);
    mac.update(&(id.wrapping_add(1)).to_le_bytes());
    let h2 = mac.finalize().into_bytes();
    let mut entropy = [0u8; 32];
    entropy[..16].copy_from_slice(&h1[..16]);
    entropy[16..].copy_from_slice(&h2[..16]);
    let m = bip32::Mnemonic::from_entropy(entropy, bip32::Language::English);
    let seed_ref = m.to_seed("");
    let mut out = [0u8; 64];
    out.copy_from_slice(seed_ref.as_ref());
    Ok(out)
}

fn id_to_mnemonic_and_seed(seed_key: &[u8; 32], id: u64) -> Result<(String, [u8; 64])> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(seed_key).unwrap();
    mac.update(&id.to_le_bytes());
    let h1 = mac.finalize().into_bytes();
    let mut mac = HmacSha256::new_from_slice(seed_key).unwrap();
    mac.update(&h1);
    mac.update(&(id.wrapping_add(1)).to_le_bytes());
    let h2 = mac.finalize().into_bytes();
    let mut entropy = [0u8; 32];
    entropy[..16].copy_from_slice(&h1[..16]);
    entropy[16..].copy_from_slice(&h2[..16]);
    let m = bip32::Mnemonic::from_entropy(entropy, bip32::Language::English);
    let phrase = m.phrase().to_string();
    let seed_ref = m.to_seed("");
    let mut seed = [0u8; 64];
    seed.copy_from_slice(seed_ref.as_ref());
    Ok((phrase, seed))
}

fn derive_privkey_from_seed(seed: &[u8; 64], account: u32, index: u32) -> Result<[u8; 32]> {
    use bip32::{DerivationPath, XPrv};
    let path_str = format!("m/44'/60'/{}'/0'/{}", account, index);
    let path: DerivationPath = path_str.parse().map_err(|e| anyhow::anyhow!("{:?}", e))?;
    let xprv = XPrv::derive_from_path(seed, &path).map_err(|e| anyhow::anyhow!("{:?}", e))?;
    let mut sk = [0u8; 32];
    sk.copy_from_slice(&xprv.to_bytes());
    Ok(sk)
}

