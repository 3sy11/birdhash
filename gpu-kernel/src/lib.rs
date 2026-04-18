//! GPU kernel: BIP32 派生 + secp256k1 + Keccak-256 → Ethereum 地址
//! 每线程处理 1 个 (seed_idx, path_idx) → 1 个地址
//! CPU 负责 BIP39/PBKDF2；GPU 负责 BIP32 × 5级 + secp256k1 × 2 + Keccak-256

#![no_std]
#![feature(abi_ptx, asm_experimental_arch)]
#![allow(clippy::all, unused)]

#[panic_handler]
fn panic_handler(_: &core::panic::PanicInfo) -> ! { loop {} }

/// 提供 memcmp 实现避免 JIT 解析失败（数组 == 会生成 memcmp 调用）
#[no_mangle]
pub unsafe extern "C" fn memcmp(a: *const u8, b: *const u8, len: usize) -> i32 {
    let mut i = 0usize;
    while i < len {
        let x = *a.add(i) as i32;
        let y = *b.add(i) as i32;
        if x != y { return x - y; }
        i += 1;
    }
    0
}

// ── CUDA 线程 ID ──────────────────────────────────────────────────────────────
#[inline(always)] unsafe fn tid_x()  -> u32 { let r:u32; core::arch::asm!("mov.u32 {0},%tid.x;",out(reg32)r); r }
#[inline(always)] unsafe fn bid_x()  -> u32 { let r:u32; core::arch::asm!("mov.u32 {0},%ctaid.x;",out(reg32)r); r }
#[inline(always)] unsafe fn bdim_x() -> u32 { let r:u32; core::arch::asm!("mov.u32 {0},%ntid.x;",out(reg32)r); r }
#[inline(always)] fn gid() -> u32 { unsafe { bid_x() * bdim_x() + tid_x() } }

// ── SHA-512 ───────────────────────────────────────────────────────────────────
const K512: [u64; 80] = [
    0x428a2f98d728ae22,0x7137449123ef65cd,0xb5c0fbcfec4d3b2f,0xe9b5dba58189dbbc,
    0x3956c25bf348b538,0x59f111f1b605d019,0x923f82a4af194f9b,0xab1c5ed5da6d8118,
    0xd807aa98a3030242,0x12835b0145706fbe,0x243185be4ee4b28c,0x550c7dc3d5ffb4e2,
    0x72be5d74f27b896f,0x80deb1fe3b1696b1,0x9bdc06a725c71235,0xc19bf174cf692694,
    0xe49b69c19ef14ad2,0xefbe4786384f25e3,0x0fc19dc68b8cd5b5,0x240ca1cc77ac9c65,
    0x2de92c6f592b0275,0x4a7484aa6ea6e483,0x5cb0a9dcbd41fbd4,0x76f988da831153b5,
    0x983e5152ee66dfab,0xa831c66d2db43210,0xb00327c898fb213f,0xbf597fc7beef0ee4,
    0xc6e00bf33da88fc2,0xd5a79147930aa725,0x06ca6351e003826f,0x142929670a0e6e70,
    0x27b70a8546d22ffc,0x2e1b21385c26c926,0x4d2c6dfc5ac42aed,0x53380d139d95b3df,
    0x650a73548baf63de,0x766a0abb3c77b2a8,0x81c2c92e47edaee6,0x92722c851482353b,
    0xa2bfe8a14cf10364,0xa81a664bbc423001,0xc24b8b70d0f89791,0xc76c51a30654be30,
    0xd192e819d6ef5218,0xd69906245565a910,0xf40e35855771202a,0x106aa07032bbd1b8,
    0x19a4c116b8d2d0c8,0x1e376c085141ab53,0x2748774cdf8eeb99,0x34b0bcb5e19b48a8,
    0x391c0cb3c5c95a63,0x4ed8aa4ae3418acb,0x5b9cca4f7763e373,0x682e6ff3d6b2b8a3,
    0x748f82ee5defb2fc,0x78a5636f43172f60,0x84c87814a1f0ab72,0x8cc702081a6439ec,
    0x90befffa23631e28,0xa4506cebde82bde9,0xbef9a3f7b2c67915,0xc67178f2e372532b,
    0xca273eceea26619c,0xd186b8c721c0c207,0xeada7dd6cde0eb1e,0xf57d4f7fee6ed178,
    0x06f067aa72176fba,0x0a637dc5a2c898a6,0x113f9804bef90dae,0x1b710b35131c471b,
    0x28db77f523047d84,0x32caab7b40c72493,0x3c9ebe0a15c9bebc,0x431d67c49c100d4c,
    0x4cc5d4becb3e42b6,0x597f299cfc657e2a,0x5fcb6fab3ad6faec,0x6c44198c4a475817,
];
const IV512: [u64; 8] = [
    0x6a09e667f3bcc908,0xbb67ae8584caa73b,0x3c6ef372fe94f82b,0xa54ff53a5f1d36f1,
    0x510e527fade682d1,0x9b05688c2b3e6c1f,0x1f83d9abfb41bd6b,0x5be0cd19137e2179,
];

fn sha512_block(state: &mut [u64; 8], block: &[u8; 128]) {
    let mut w = [0u64; 80];
    for i in 0..16 { w[i] = u64::from_be_bytes([block[i*8],block[i*8+1],block[i*8+2],block[i*8+3],block[i*8+4],block[i*8+5],block[i*8+6],block[i*8+7]]); }
    for i in 16..80 {
        let s0 = w[i-15].rotate_right(1)^w[i-15].rotate_right(8)^(w[i-15]>>7);
        let s1 = w[i-2].rotate_right(19)^w[i-2].rotate_right(61)^(w[i-2]>>6);
        w[i] = w[i-16].wrapping_add(s0).wrapping_add(w[i-7]).wrapping_add(s1);
    }
    let [mut a,mut b,mut c,mut d,mut e,mut f,mut g,mut h] = *state;
    for i in 0..80 {
        let s1 = e.rotate_right(14)^e.rotate_right(18)^e.rotate_right(41);
        let ch = (e&f)^(!e&g);
        let t1 = h.wrapping_add(s1).wrapping_add(ch).wrapping_add(K512[i]).wrapping_add(w[i]);
        let s0 = a.rotate_right(28)^a.rotate_right(34)^a.rotate_right(39);
        let maj = (a&b)^(a&c)^(b&c);
        let t2 = s0.wrapping_add(maj);
        h=g;g=f;f=e;e=d.wrapping_add(t1);d=c;c=b;b=a;a=t1.wrapping_add(t2);
    }
    state[0]=state[0].wrapping_add(a);state[1]=state[1].wrapping_add(b);
    state[2]=state[2].wrapping_add(c);state[3]=state[3].wrapping_add(d);
    state[4]=state[4].wrapping_add(e);state[5]=state[5].wrapping_add(f);
    state[6]=state[6].wrapping_add(g);state[7]=state[7].wrapping_add(h);
}

/// SHA-512，输入最多 256 字节（BIP32 场景）
fn sha512(data: &[u8], out: &mut [u8; 64]) {
    let mut state = IV512;
    let mut buf = [0u8; 128];
    let mut buf_len = 0usize;
    let total_bits = (data.len() as u64) * 8;
    let mut pos = 0usize;
    while pos < data.len() {
        let take = (data.len()-pos).min(128-buf_len);
        buf[buf_len..buf_len+take].copy_from_slice(&data[pos..pos+take]);
        buf_len += take; pos += take;
        if buf_len == 128 { sha512_block(&mut state, &buf); buf_len = 0; buf = [0u8;128]; }
    }
    buf[buf_len] = 0x80; buf_len += 1;
    if buf_len > 112 {
        while buf_len < 128 { buf[buf_len] = 0; buf_len += 1; }
        sha512_block(&mut state, &buf);
        buf = [0u8;128]; buf_len = 0;
    }
    while buf_len < 112 { buf[buf_len] = 0; buf_len += 1; }
    buf[112..120].copy_from_slice(&[0u8;8]);
    buf[120..128].copy_from_slice(&total_bits.to_be_bytes());
    sha512_block(&mut state, &buf);
    for i in 0..8 { out[i*8..i*8+8].copy_from_slice(&state[i].to_be_bytes()); }
}

fn hmac_sha512(key: &[u8], msg: &[u8], out: &mut [u8; 64]) {
    let mut k = [0u8; 128];
    if key.len() <= 128 { k[..key.len()].copy_from_slice(key); }
    else { let mut kh=[0u8;64]; sha512(key,&mut kh); k[..64].copy_from_slice(&kh); }
    let mut ipad=[0u8;128]; let mut opad=[0u8;128];
    for i in 0..128 { ipad[i]=k[i]^0x36; opad[i]=k[i]^0x5c; }
    // inner hash: SHA512(ipad || msg)
    let mut inner_buf = [0u8; 256];
    inner_buf[..128].copy_from_slice(&ipad);
    let mlen = msg.len().min(128);
    inner_buf[128..128+mlen].copy_from_slice(&msg[..mlen]);
    let mut inner = [0u8; 64];
    sha512(&inner_buf[..128+mlen], &mut inner);
    // outer hash: SHA512(opad || inner)
    let mut outer_buf = [0u8; 192];
    outer_buf[..128].copy_from_slice(&opad);
    outer_buf[128..192].copy_from_slice(&inner);
    sha512(&outer_buf, out);
}

// ── BIP32 ─────────────────────────────────────────────────────────────────────

fn bip32_master(seed: &[u8; 64]) -> ([u8; 32], [u8; 32]) {
    let mut out = [0u8; 64];
    hmac_sha512(b"Bitcoin seed", seed, &mut out);
    let mut key=[0u8;32]; let mut chain=[0u8;32];
    key.copy_from_slice(&out[..32]); chain.copy_from_slice(&out[32..]);
    (key, chain)
}

fn bip32_ckd_hard(key: &[u8;32], chain: &[u8;32], index: u32) -> ([u8;32],[u8;32]) {
    let mut data=[0u8;37]; data[0]=0x00;
    data[1..33].copy_from_slice(key);
    data[33..37].copy_from_slice(&(index|0x80000000u32).to_be_bytes());
    let mut out=[0u8;64]; hmac_sha512(chain,&data,&mut out);
    let mut il=[0u8;32]; il.copy_from_slice(&out[..32]);
    let child_key = scalar_add_mod_n(&il, key);
    let mut child_chain=[0u8;32]; child_chain.copy_from_slice(&out[32..]);
    (child_key, child_chain)
}

fn bip32_ckd_normal(key: &[u8;32], chain: &[u8;32], index: u32) -> ([u8;32],[u8;32]) {
    let mut pub_key=[0u8;33];
    secp256k1_pubkey_compressed(key, &mut pub_key);
    let mut data=[0u8;37];
    data[..33].copy_from_slice(&pub_key);
    data[33..37].copy_from_slice(&index.to_be_bytes());
    let mut out=[0u8;64]; hmac_sha512(chain,&data,&mut out);
    let mut il=[0u8;32]; il.copy_from_slice(&out[..32]);
    let child_key = scalar_add_mod_n(&il, key);
    let mut child_chain=[0u8;32]; child_chain.copy_from_slice(&out[32..]);
    (child_key, child_chain)
}

// ── 256-bit 整数（8×u32 小端） ────────────────────────────────────────────────

type U256 = [u32; 8];

// secp256k1 参数（小端 u32）
const P: U256   = [0xFFFFFC2F,0xFFFFFFFE,0xFFFFFFFF,0xFFFFFFFF,0xFFFFFFFF,0xFFFFFFFF,0xFFFFFFFF,0xFFFFFFFF];
const N: U256   = [0xD0364141,0xBFD25E8C,0xAF48A03B,0xBAAEDCE6,0xFFFFFFFE,0xFFFFFFFF,0xFFFFFFFF,0xFFFFFFFF];
const GX: U256  = [0x16F81798,0x59F2815B,0x2DCE28D9,0x029BFCDB,0xCE870B07,0x55A06295,0xF9DCBBAC,0x79BE667E];
const GY: U256  = [0xFB10D4B8,0x9C47D08F,0xA6855419,0xFD17B448,0x0E1108A8,0x5DA4FBFC,0x26A3C465,0x483ADA77];
const ZERO: U256 = [0u32;8];
const ONE: U256  = [1,0,0,0,0,0,0,0];
// 2^256 - P = 2^32 + 977 = 0x100000000 + 0x3D1
const TWO256_MINUS_P: U256 = [0x000003D1, 0x00000001, 0,0,0,0,0,0];

#[inline(always)]
fn u256_is_zero(a: &U256) -> bool {
    a[0]|a[1]|a[2]|a[3]|a[4]|a[5]|a[6]|a[7] == 0
}
#[inline(always)]
fn u256_eq(a: &U256, b: &U256) -> bool {
    a[0]==b[0] && a[1]==b[1] && a[2]==b[2] && a[3]==b[3] && a[4]==b[4] && a[5]==b[5] && a[6]==b[6] && a[7]==b[7]
}

fn u256_from_be(b: &[u8; 32]) -> U256 {
    let mut r=[0u32;8];
    for i in 0..8 { r[7-i]=u32::from_be_bytes([b[i*4],b[i*4+1],b[i*4+2],b[i*4+3]]); }
    r
}
fn u256_to_be(a: &U256, b: &mut [u8; 32]) {
    for i in 0..8 { let w=a[7-i].to_be_bytes(); b[i*4..i*4+4].copy_from_slice(&w); }
}
fn u256_cmp(a: &U256, b: &U256) -> i32 {
    for i in (0..8).rev() { if a[i]>b[i]{return 1;} if a[i]<b[i]{return -1;} }
    0
}
fn u256_add(a: &U256, b: &U256) -> (U256, bool) {
    let mut r=[0u32;8]; let mut c=0u64;
    for i in 0..8 { let s=a[i] as u64+b[i] as u64+c; r[i]=s as u32; c=s>>32; }
    (r, c!=0)
}
fn u256_sub(a: &U256, b: &U256) -> U256 { // a >= b 必须满足
    let mut r=[0u32;8]; let mut bw=0i64;
    for i in 0..8 { let s=a[i] as i64-b[i] as i64-bw; r[i]=s as u32; bw=if s<0{1}else{0}; }
    r
}

// ── 域运算 mod p ──────────────────────────────────────────────────────────────

fn fe_add(a: &U256, b: &U256) -> U256 {
    let (r, c) = u256_add(a, b);
    if c {
        // r + 2^256 - P = r + TWO256_MINUS_P（无溢出，因为 r < P 在进位时成立）
        let (r2, _) = u256_add(&r, &TWO256_MINUS_P);
        r2
    } else if u256_cmp(&r, &P) >= 0 {
        u256_sub(&r, &P)
    } else {
        r
    }
}
fn fe_sub(a: &U256, b: &U256) -> U256 {
    if u256_cmp(a, b) < 0 {
        // a - b mod p = p - (b - a)
        u256_sub(&P, &u256_sub(b, a))
    } else {
        u256_sub(a, b)
    }
}
fn fe_neg(a: &U256) -> U256 { if u256_is_zero(a){ZERO}else{u256_sub(&P,a)} }

fn u256_mul_wide(a: &U256, b: &U256) -> [u32; 16] {
    let mut acc=[0u64;16];
    for i in 0..8 { for j in 0..8 { acc[i+j]+=a[i] as u64*b[j] as u64; } }
    let mut r=[0u32;16]; let mut c=0u64;
    for i in 0..16 { let s=acc[i]+c; r[i]=s as u32; c=s>>32; }
    r
}

/// Solinas reduction: t mod p，p = 2^256 - 2^32 - 977
fn fe_reduce_wide(t: &[u32; 16]) -> U256 {
    let mut acc=[0u64;9];
    for i in 0..8 { acc[i]+=t[i] as u64; }
    for i in 0..8 { acc[i]+=t[i+8] as u64*977; acc[i+1]+=t[i+8] as u64; }
    for i in 0..8 { acc[i+1]+=acc[i]>>32; acc[i]&=0xFFFFFFFF; }
    // 处理 acc[8] 溢出（最多需要两次）
    for _ in 0..2 {
        let over=acc[8];
        if over==0 { break; }
        acc[0]+=over*977; acc[1]+=over; acc[8]=0;
        for i in 0..8 { acc[i+1]+=acc[i]>>32; acc[i]&=0xFFFFFFFF; }
    }
    let mut r: U256=[0;8];
    for i in 0..8 { r[i]=acc[i] as u32; }
    if u256_cmp(&r,&P)>=0 { u256_sub(&r,&P) } else { r }
}

fn fe_mul(a: &U256, b: &U256) -> U256 { fe_reduce_wide(&u256_mul_wide(a, b)) }
fn fe_sqr(a: &U256) -> U256 { fe_mul(a, a) }

fn fe_pow(base: &U256, exp: &U256) -> U256 {
    let mut result=ONE; let mut b=*base;
    for i in 0..8 { let mut w=exp[i]; for _ in 0..32 { if w&1==1{result=fe_mul(&result,&b);} b=fe_sqr(&b); w>>=1; } }
    result
}

/// 模逆 a^(p-2) mod p
fn fe_inv(a: &U256) -> U256 {
    // p-2 = FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2D
    let exp: U256 = [0xFFFFFC2D,0xFFFFFFFE,0xFFFFFFFF,0xFFFFFFFF,0xFFFFFFFF,0xFFFFFFFF,0xFFFFFFFF,0xFFFFFFFF];
    fe_pow(a, &exp)
}

// ── 标量运算 mod n ────────────────────────────────────────────────────────────

fn scalar_add_mod_n(a_be: &[u8;32], b_be: &[u8;32]) -> [u8;32] {
    let a=u256_from_be(a_be); let b=u256_from_be(b_be);
    let (s,c)=u256_add(&a,&b);
    let r=if c||u256_cmp(&s,&N)>=0{u256_sub(&s,&N)}else{s};
    let mut out=[0u8;32]; u256_to_be(&r,&mut out); out
}

// ── secp256k1 点运算（Jacobian 坐标）────────────────────────────────────────

type PointJ=([u32;8],[u32;8],[u32;8]);

fn point_is_inf(p: &PointJ) -> bool { u256_is_zero(&p.2) }

/// Jacobian 倍点（secp256k1, a=0）—— dbl-2009-l
fn point_double(p: &PointJ) -> PointJ {
    if point_is_inf(p) { return (ZERO,ZERO,ZERO); }
    let (x,y,z)=p;
    let a=fe_sqr(x);
    let b=fe_sqr(y);
    let c=fe_sqr(&b);
    // D = 2*((X+B)^2 - A - C)
    let xb=fe_add(x,&b);
    let xb2=fe_sqr(&xb);
    let d_half=fe_sub(&fe_sub(&xb2,&a),&c);
    let d=fe_add(&d_half,&d_half);
    // E = 3*A, F = E^2
    let e=fe_add(&fe_add(&a,&a),&a);
    let f=fe_sqr(&e);
    // X3 = F - 2*D
    let x3=fe_sub(&f,&fe_add(&d,&d));
    // Y3 = E*(D-X3) - 8*C
    let c2=fe_add(&c,&c); let c4=fe_add(&c2,&c2); let c8=fe_add(&c4,&c4);
    let y3=fe_sub(&fe_mul(&e,&fe_sub(&d,&x3)),&c8);
    // Z3 = 2*Y*Z
    let z3=fe_mul(&fe_add(y,y),z);
    (x3,y3,z3)
}

/// Jacobian+Affine 混合加法 —— madd-2007-bl
fn point_add_affine(p: &PointJ, qx: &U256, qy: &U256) -> PointJ {
    if point_is_inf(p) { return (*qx,*qy,ONE); }
    let (x1,y1,z1)=p;
    let z1z1=fe_sqr(z1);
    let u2=fe_mul(qx,&z1z1);
    let s2=fe_mul(qy,&fe_mul(z1,&z1z1));
    let h=fe_sub(&u2,x1);
    if u256_is_zero(&h) {
        if u256_eq(&s2, y1) { return point_double(p); }
        else { return (ZERO,ZERO,ZERO); }
    }
    let hh=fe_sqr(&h);
    let hhh=fe_mul(&h,&hh);
    let r=fe_sub(&s2,y1);
    let x1hh=fe_mul(x1,&hh);
    let x3=fe_sub(&fe_sub(&fe_sqr(&r),&hhh),&fe_add(&x1hh,&x1hh));
    let y3=fe_sub(&fe_mul(&r,&fe_sub(&x1hh,&x3)),&fe_mul(y1,&hhh));
    let z3=fe_mul(z1,&h);
    (x3,y3,z3)
}

/// 标量乘法 k×G（k 为大端 32 字节），返回仿射坐标 (x,y)（各 32 字节大端）
fn point_scalar_mul_g(k_be: &[u8;32]) -> ([u8;32],[u8;32]) {
    let mut r: PointJ=(ZERO,ZERO,ZERO);
    let mut first=true;
    for bi in 0..32 {
        let byte=k_be[bi];
        for bit in (0..8).rev() {
            if !first { r=point_double(&r); }
            if (byte>>bit)&1==1 {
                if first { r=(GX,GY,ONE); first=false; }
                else { r=point_add_affine(&r,&GX,&GY); }
            }
        }
    }
    if first { return ([0u8;32],[0u8;32]); } // k == 0（不应出现）
    let z_inv=fe_inv(&r.2);
    let z2=fe_sqr(&z_inv);
    let z3=fe_mul(&z2,&z_inv);
    let ax=fe_mul(&r.0,&z2); let ay=fe_mul(&r.1,&z3);
    let mut xb=[0u8;32]; let mut yb=[0u8;32];
    u256_to_be(&ax,&mut xb); u256_to_be(&ay,&mut yb);
    (xb,yb)
}

fn secp256k1_pubkey_compressed(privkey_be: &[u8;32], out: &mut [u8;33]) {
    let (x,y)=point_scalar_mul_g(privkey_be);
    out[0]=if y[31]&1==0{0x02}else{0x03};
    out[1..33].copy_from_slice(&x);
}

fn secp256k1_pubkey_uncompressed_xy(privkey_be: &[u8;32], out: &mut [u8;64]) {
    let (x,y)=point_scalar_mul_g(privkey_be);
    out[..32].copy_from_slice(&x);
    out[32..].copy_from_slice(&y);
}

// ── Keccak-256 ────────────────────────────────────────────────────────────────

const KECCAK_RC: [u64; 24] = [
    0x0000000000000001,0x0000000000008082,0x800000000000808A,0x8000000080008000,
    0x000000000000808B,0x0000000080000001,0x8000000080008081,0x8000000000008009,
    0x000000000000008A,0x0000000000000088,0x0000000080008009,0x000000008000000A,
    0x000000008000808B,0x800000000000008B,0x8000000000008089,0x8000000000008003,
    0x8000000000008002,0x8000000000000080,0x000000000000800A,0x800000008000000A,
    0x8000000080008081,0x8000000000008080,0x0000000080000001,0x8000000080008008,
];
const KECCAK_PILN: [usize;24] = [10,7,11,17,18,3,5,16,8,21,24,4,15,23,19,13,12,2,20,14,22,9,6,1];
const KECCAK_ROTC: [u32;24] = [1,3,6,10,15,21,28,36,45,55,2,14,27,41,56,8,25,43,62,18,39,61,20,44];

fn keccak_f(st: &mut [u64; 25]) {
    for r in 0..24 {
        let mut c=[0u64;5];
        for x in 0..5 { c[x]=st[x]^st[x+5]^st[x+10]^st[x+15]^st[x+20]; }
        let mut d=[0u64;5];
        for x in 0..5 { d[x]=c[(x+4)%5]^c[(x+1)%5].rotate_left(1); }
        for x in 0..5 { for y in 0..5 { st[x+y*5]^=d[x]; } }
        let mut last=st[1];
        for i in 0..24 { let j=KECCAK_PILN[i]; let tmp=st[j]; st[j]=last.rotate_left(KECCAK_ROTC[i]); last=tmp; }
        for y in 0..5 {
            let mut t=[0u64;5];
            for x in 0..5 { t[x]=st[x+y*5]; }
            for x in 0..5 { st[x+y*5]=t[x]^((!t[(x+1)%5])&t[(x+2)%5]); }
        }
        st[0]^=KECCAK_RC[r];
    }
}

/// Keccak-256（Ethereum 版，非 SHA3），输入最多 135 字节
fn keccak256(input: &[u8], out: &mut [u8; 32]) {
    const RATE: usize = 136;
    let mut st=[0u64;25];
    let mut buf=[0u8;136];
    let len=input.len().min(RATE-1);
    buf[..len].copy_from_slice(&input[..len]);
    buf[len]=0x01;
    buf[RATE-1]|=0x80;
    for i in 0..(RATE/8) {
        st[i]^=u64::from_le_bytes([buf[i*8],buf[i*8+1],buf[i*8+2],buf[i*8+3],buf[i*8+4],buf[i*8+5],buf[i*8+6],buf[i*8+7]]);
    }
    keccak_f(&mut st);
    for i in 0..4 { out[i*8..i*8+8].copy_from_slice(&st[i].to_le_bytes()); }
}

// ── CUDA Kernel 入口 ──────────────────────────────────────────────────────────

/// 每线程: (seed_idx, path_idx) → 20 字节以太坊地址
/// seeds_ptr: [n_seeds × 64] BIP32 root seed（大端）
/// accounts_ptr: [n_paths × 4] account 值（u32 主机序）
/// indices_ptr:  [n_paths × 4] index 值（u32 主机序）
/// out_addrs:    [n_seeds × n_paths × 20] 输出
#[no_mangle]
pub unsafe extern "ptx-kernel" fn derive_addresses(
    seeds_ptr: *const u8,
    accounts_ptr: *const u32,
    indices_ptr: *const u32,
    n_seeds: u32,
    n_paths: u32,
    out_addrs: *mut u8,
) {
    let id = gid();
    if n_paths == 0 || n_seeds == 0 { return; }
    if id >= n_seeds * n_paths { return; }
    let seed_idx = id / n_paths;
    let path_idx = id % n_paths;

    let mut seed = [0u8; 64];
    let sb = seeds_ptr.add(seed_idx as usize * 64);
    for i in 0..64 { seed[i] = *sb.add(i); }

    let account = *accounts_ptr.add(path_idx as usize);
    let index   = *indices_ptr.add(path_idx as usize);

    // m/44'/60'/account'/0'/index（最后一级非硬化）
    let (mk,mc)   = bip32_master(&seed);
    let (k1,c1)   = bip32_ckd_hard(&mk,&mc,44);
    let (k2,c2)   = bip32_ckd_hard(&k1,&c1,60);
    let (k3,c3)   = bip32_ckd_hard(&k2,&c2,account);
    let (k4,c4)   = bip32_ckd_hard(&k3,&c3,0);
    let (k5,_)    = bip32_ckd_normal(&k4,&c4,index);

    let mut pubxy=[0u8;64];
    secp256k1_pubkey_uncompressed_xy(&k5, &mut pubxy);
    let mut hash=[0u8;32];
    keccak256(&pubxy, &mut hash);

    let out_base = out_addrs.add(id as usize * 20);
    for i in 0..20 { *out_base.add(i) = hash[12+i]; }
}
