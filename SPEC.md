# birdhash — 技术规格书

> 以太坊私钥碰撞检测系统 | Rust | 三级存储架构 | 分布式回查

---

## 1. 项目概述

birdhash 是一个用 Rust 编写的以太坊地址碰撞检测工具。核心思路：无限生成随机以太坊私钥+地址，与链上真实地址进行碰撞匹配。如果匹配成功，则获得该链上地址的私钥控制权。

**核心挑战**：以太坊地址空间 2¹⁶⁰，碰撞概率极低，需要海量生成+高效存储+极致碰撞才有可能。

---

## 2. 系统架构

### 2.1 三组件

```
┌─────────────┐                          ┌─────────────┐
│  Generator   │──→ BinaryFuse8 (L1) ──→│             │
│  seed+counter│──→ Bloom 10%   (L2) ──→│  Collider   │──→ hits.txt
│  30M keys/s  │                         │  双向碰撞    │
└─────────────┘                          │  L1/L2/L3   │
                                         │             │
┌─────────────┐                          │  Scanner    │
│   Fetcher    │──→ BinaryFuse16 ──────→│  分布式 L3   │
│  纯内存 705MB │    (in-memory)          └─────────────┘
└─────────────┘
```

| 组件 | 职责 | 数据流 |
|------|------|--------|
| **Generator** | 确定性生成私钥+地址 (seed+counter) | 输出 L1 BinaryFuse8 filter 分片 |
| **Fetcher** | 遍历以太坊区块，获取链上地址 | 纯内存 BinaryFuse16 filter |
| **Collider** | 双向碰撞 + 批量重生成验证 | 读取 L1/L2 filter，输出 hits.txt |
| **Scanner** | 分布式 L3 回查 (过期层) | distribute/worker/collect |

### 2.2 三级存储

```
┌───────────────────────────────────────────────┐
│  L1 活跃层  BinaryFuse8                        │
│  ~965 MB/分片 (10 亿条)  FP 0.39%              │
│  碰撞: O(1) filter 查询                        │
├───────────────────────────────────────────────┤
│  L2 归档层  Bloom 10%                          │
│  ~572 MB/分片  FP 10%  (比 L1 省 41%)           │
│  碰撞: O(1) 查询 → 命中后批量重生成验证          │
├───────────────────────────────────────────────┤
│  L3 过期层  零磁盘                              │
│  仅 cursor 记录 counter 范围                    │
│  碰撞: 分布式重生成 (CPU/GPU)                   │
└───────────────────────────────────────────────┘
```

**生命周期流转**：磁盘满 80% → 最老 L1 分片降级 L2 (重生成→构建 Bloom 10%) → L2 满 → 最老 L2 删除降级 L3 (cursor 记录范围)

---

## 3. 核心算法

### 3.1 确定性私钥生成

```
master_seed: [u8; 32]     ← data/master_seed.key (一次生成，安全备份)
counter: u64              ← 全局递增，永不重复

privkey = HMAC-SHA256(master_seed, counter.to_le_bytes())
pubkey  = secp256k1(privkey)
address = keccak256(pubkey[1..])[12..32]   // 后 20 字节
```

**关键特性**：给定 seed + counter → 可任意时刻重算同一个私钥 → 无需持久化 privkey 文件

### 3.2 滤波器参数

| 滤波器 | 用途 | FP | bits/elem | 10 亿条大小 | 每 TB 覆盖 |
|--------|------|-----|-----------|------------|-----------|
| **BinaryFuse8** | Gen L1 | 0.39% | 8.05 | **965 MB** | **1.09 万亿** |
| BinaryFuse16 | Fetch (内存) | 0.0015% | 16.10 | — | — |
| Bloom 10% | Gen L2 归档 | 10% | 4.79 | **572 MB** | 1.83 万亿 |

### 3.3 双向碰撞

| 方向 | 触发时机 | 数据流 |
|------|---------|--------|
| gen → fetch | Generator 生成时实时查 fetch filter | 内存 O(1) |
| fetch → gen | Fetcher 新地址查 gen filter (L1→L2→L3) | 磁盘/分布式 |

**批量碰撞策略** (fetch_vs_gen):
1. 加载 gen filter 分片
2. 遍历所有待碰 fetch 地址 → 收集候选 HashSet
3. 重生成该分片全部 10 亿条 key (~29 秒 12 线程) → 逐条查候选
4. 真实命中 → hits.txt，假阳性丢弃

---

## 4. 性能数据

| 指标 | 值 |
|------|-----|
| 生成速率 | **~30M keys/sec** (12 线程, secp256k1 C binding) |
| L1 刷盘速率 | ~28.7 MB/sec (HDD 无压力) |
| 满一个分片 | 每 33.3 秒 |
| 分片重生成 | **~29 秒** (12 线程) |
| Fetcher 全量内存 | ~705 MB (3.5 亿地址) |
| 每日新增链上地址 | ~25 万 (~10 MB) |

### 磁盘覆盖量 (L1 60% + L2 40%)

| 总磁盘 | L1+L2 总覆盖 | L3 开始累积时间 |
|--------|-------------|---------------|
| 1 TB | 1.26 万亿 | ~12 小时 |
| 4 TB | 5.06 万亿 | ~2 天 |
| 8 TB | 10.1 万亿 | ~4 天 |
| 16 TB | 20.2 万亿 | ~8 天 |

### 分布式 L3 回查

| Workers | 回查 1 万亿 counter |
|---------|-------------------|
| 1 台 4 核 | 23 小时 |
| 5 台 4 核 | **4.6 小时** |
| 1 GPU (可选) | **1.4 小时** |

---

## 5. 代码结构

```
birdhash/
├── Cargo.toml           # 16 依赖, edition 2021, release LTO
├── config.toml          # 运行时配置 (shard_size/l1_ratio/watermark)
├── src/
│   ├── main.rs          # CLI 入口 (clap, 9 子命令)
│   ├── keygen.rs        # ✅ 确定性密钥生成 (HMAC-SHA256→secp256k1→keccak256)
│   ├── filter.rs        # ✅ BinaryFuse8/16 + BloomFilter (bitvec)
│   ├── cursor.rs        # ✅ 6 种游标结构体 + 通用 JSON load/save
│   ├── config.rs        # ✅ AppConfig + 路径方法 + ensure_dirs
│   ├── stats.rs         # ✅ 系统状态采集 + 打印
│   ├── generator.rs     # 🔲 Generator 主循环 (Phase 4)
│   ├── fetcher.rs       # 🔲 Fetcher 区块遍历 (Phase 5)
│   ├── collider.rs      # 🔲 L1/L2 碰撞 (Phase 6)
│   ├── scanner.rs       # 🔲 分布式 L3 (Phase 7)
│   ├── lifecycle.rs     # 🔲 磁盘生命周期 (Phase 7)
│   └── gpu/             # 🔲 GPU 加速 (Phase 9, 可选)
└── data/
    ├── master_seed.key
    ├── generator/       # L1 filter_gen_*.bin + L2 archive_gen_*.bin
    ├── fetcher/         # filter_fetch.bin + new_addrs.bin
    ├── results/         # collider_cursor.json + hits.txt
    └── tasks/           # 分布式回查 task/result JSON
```

✅ = 已实现并通过测试 | 🔲 = 骨架已建，待实现

### 5.1 模块依赖

```
main.rs
  ├─ keygen.rs      (零依赖，纯密码学)
  ├─ filter.rs      (零依赖，纯数据结构)
  ├─ cursor.rs      (零依赖，纯序列化)
  ├─ config.rs      (→ rayon)
  ├─ stats.rs       (→ config, cursor)
  ├─ generator.rs   (→ config, cursor, keygen, filter)
  ├─ fetcher.rs     (→ config, cursor, keygen)
  ├─ collider.rs    (→ config, cursor, keygen, filter)
  ├─ scanner.rs     (→ cursor, keygen)
  └─ lifecycle.rs   (→ config, cursor, keygen, filter)
```

---

## 6. CLI 接口

```bash
birdhash <COMMAND>

Commands:
  init              # 初始化 master_seed + data 目录
  generate          # 运行 Generator (seed+counter → L1 BinaryFuse8)
    -t, --threads   # 线程数 (默认全部核心)
  fetch             # 运行 Fetcher (区块遍历 → 内存 BinaryFuse16)
    -r, --rpc       # ETH RPC 端点 URL
  collide           # 运行 Collider (L1/L2 双向碰撞)
    --regenerate-range <START> <END>  # L3 本地回查
  status            # 显示三级存储状态
  purge             # 磁盘生命周期管理
    --keep-last N   # 保留最近 N 个 L1 分片
    --archive       # 降级到 L2 (而非删除)
  scan-distribute   # L3 分布式: 切分任务
    --range <START> <END>
    --chunk-size    # 每块 counter 数 (默认 100 亿)
    --fetch-addrs   # 新地址文件路径
    --out           # 输出目录
  scan-worker       # L3 分布式: Worker 执行
    --task          # 任务 JSON 路径
    --output        # 结果输出目录
  scan-collect      # L3 分布式: 合并结果
    --results-dir   # 结果目录
```

---

## 7. 数据格式

### 7.1 generator_cursor.json

```json
{
  "master_seed_hash": "a1b2c3d4e5f60718",
  "current_counter": 54321000000,
  "l1_shard_count": 12,
  "l2_shard_count": 30,
  "current_shard_entries": 321000000,
  "total_generated": 54321000000,
  "purged_epochs": [
    {
      "counter_range": [0, 30000000000],
      "purged_at": "2026-03-05T10:00:00Z",
      "collided_with_fetch_version": 5
    }
  ],
  "last_updated": "2026-03-05T18:00:00Z"
}
```

### 7.2 fetcher_cursor.json

```json
{
  "start_block": 0,
  "end_block": 19500000,
  "last_synced_block": 1245832,
  "total_addresses": 350000000,
  "filter_version": 5,
  "new_addrs_since_last_export": 250000,
  "last_updated": "2026-03-05T12:00:00Z"
}
```

### 7.3 collider_cursor.json

```json
{
  "gen_vs_fetch": { "last_gen_counter": 54321000000, "fetch_filter_version": 5, "total_checked": 54321000000 },
  "fetch_vs_gen_l1": { "completed_fetch_block_range": [0, 1245832], "current_fetch_offset": 8765432, "gen_l1_shard_version": 12 },
  "fetch_vs_gen_l2": { "gen_l2_shard_version": 30, "last_checked_l2_shard": 28 },
  "incremental": { "pending_new_l1_shards": [12, 13], "pending_new_l2_shards": [30] },
  "hits": 0,
  "last_updated": "2026-03-05T18:00:00Z"
}
```

### 7.4 scan task/result

```json
// task
{ "task_id": 42, "master_seed": "hex...", "counter_start": 420000000000, "counter_end": 430000000000, "fetch_addresses_file": "new_addrs.bin", "fetch_count": 250000 }
// worker state
{ "task_id": 42, "last_processed": 425678901234, "status": "interrupted", "hits": [] }
```

### 7.5 文件命名

| 文件 | 格式 | 大小 |
|------|------|------|
| `filter_gen_{id:08}.bin` | BinaryFuse8 serde_json | ~965 MB |
| `archive_gen_{id:08}.bin` | Bloom 10% 自定义二进制 | ~572 MB |
| `filter_fetch.bin` | BinaryFuse16 serde_json | ~705 MB |
| `new_addrs.bin` | raw [u8; 20] × N | ~10 MB/天 |
| `hits.txt` | `{counter}\t{privkey_hex}\t{addr_hex}\n` | 极小 |
| `master_seed.key` | raw 32 bytes | 32 B |

---

## 8. 依赖

| crate | 版本 | 用途 |
|-------|------|------|
| secp256k1 | 0.29 | 椭圆曲线 (C FFI, ~3M ops/s/thread) |
| tiny-keccak | 2.0 | Keccak-256 哈希 |
| hmac + sha2 | 0.12 / 0.10 | HMAC-SHA256 确定性密钥推导 |
| xorf | 0.11 | BinaryFuse8/16 概率滤波器 |
| bitvec | 1 | Bloom filter 位向量 |
| rayon | 1.10 | 数据并行 |
| clap | 4 (derive) | CLI 解析 |
| serde + serde_json | 1 | 序列化 |
| chrono | 0.4 | 时间戳 |
| rand | 0.8 | master_seed 生成 |
| ctrlc | 3.4 | 优雅退出信号 |
| anyhow | 1 | 错误处理 |
| hex | 0.4 | 十六进制编码 |
| log + env_logger | 0.4 / 0.11 | 日志 |

**编译优化** (release profile):
```toml
opt-level = 3
lto = "thin"
codegen-units = 1
strip = true
```

---

## 9. 开发进度

| Phase | 内容 | 状态 |
|-------|------|------|
| 1 | 需求分析 + 技术选型 + 三级架构设计 | ✅ complete |
| 2 | 项目脚手架 (11 模块, 6 测试通过) | ✅ complete |
| 3 | 核心基础层 (keygen 增强 / filter 优化 / cursor 集成测试) | 🔲 pending |
| 4 | Generator 主循环 (rayon 并行 + L1 刷盘 + 实时碰) | 🔲 pending |
| 5 | Fetcher (RPC 遍历 + 纯内存 BinaryFuse16) | 🔲 pending |
| 6 | Collider (L1/L2 碰撞 + 批量重生成 + 断点续碰) | 🔲 pending |
| 7 | 磁盘生命周期 + 分布式 L3 回查 | 🔲 pending |
| 8 | CLI 集成 + 优化 + 交付 | 🔲 pending |
| 9 | GPU 加速 (wgpu, L3 回查 6-11x 加速) | 📋 not_planned |

---

## 10. 安全注意

- `data/master_seed.key` 是**唯一秘密**，丢失则无法恢复任何已生成私钥
- 建议 `chmod 600 data/master_seed.key`
- `.gitignore` 已排除 `master_seed.key` 和所有 `.bin` 数据文件
- 分布式回查的 task JSON 包含 master_seed hex，传输需加密

---

## 11. 运维手册

### 日常运行

```bash
# 1. 初始化（首次）
birdhash init

# 2. 三个组件可并行运行
birdhash generate &          # 后台持续生成
birdhash fetch --rpc $RPC &  # 后台持续抓取
birdhash collide &           # 后台持续碰撞

# 3. 定期检查
birdhash status
```

### 磁盘满处理

```bash
# 查看当前状态
birdhash status

# 降级最老的 L1 分片到 L2（省 41% 磁盘）
birdhash purge --archive

# 或直接删除最老分片（降级 L3，零磁盘）
birdhash purge --keep-last 100
```

### 分布式 L3 回查

```bash
# 主机：切分任务
birdhash scan-distribute --range 0 1000000000000 \
  --chunk-size 10000000000 --fetch-addrs new_addrs.bin --out tasks/

# 分发 task_*.json + new_addrs.bin + birdhash 二进制到各 Worker

# Worker：执行（支持断点续算）
birdhash scan-worker --task task_00042.json --output results/

# 主机：收集结果
birdhash scan-collect --results-dir results/
```
