# Findings & Decisions — birdhash 最终架构

## 一、系统概览

三组件 + 三级存储的以太坊私钥碰撞系统。

```
┌─────────────┐   BinaryFuse8 (L1)   ┌─────────────┐
│  Generator   │ ──────────────────→  │             │
│  seed+counter│   Bloom 10% (L2)     │  Collider   │ → hits.txt
│  确定性生成   │ ──────────────────→  │  双向碰撞    │
└─────────────┘                       │  + 断点续碰  │
                                      │             │
┌─────────────┐   BinaryFuse16       │  L3: 分布式  │
│   Fetcher    │ ──────────────────→  │  回查 (GPU/  │
│  纯内存       │   (~705 MB)          │  零散 CPU)   │
└─────────────┘                       └─────────────┘
```

---

## 二、以太坊地址生成流程

```
Private Key (32 bytes)
  → secp256k1 公钥 (去 0x04 = 64 bytes)
    → Keccak-256(64 bytes) → 32 bytes
      → 后 20 bytes = Ethereum Address
```

---

## 三、确定性私钥生成

```
master_seed: [u8; 32]  (一次生成，安全保存于 data/master_seed.key)
counter: u64            (全局递增)

privkey_i = HMAC-SHA256(master_seed, counter_i.to_le_bytes())
```

- 给定 counter → 任何时候可重算同一个 privkey → 推导同一个地址
- 无需存储 privkey+addr 对，只存滤波器即可
- counter 递增 → 永不重复
- 若 HMAC 结果 ≥ secp256k1 曲线阶 n（概率 < 2⁻¹²⁸），跳过

**安全注意**：`master_seed.key` 是唯一需安全备份的秘密，丢失则所有已生成 key 无法恢复。

---

## 四、滤波器选型

### Gen 侧：BinaryFuse8

| 参数 | 值 |
|------|-----|
| fingerprint | 8 bit |
| 误判率 | 1/256 ≈ **0.39%** |
| bits/element | **8.05** |
| bytes/element | **1.006** |
| 查询 | O(1), 固定 3 次内存访问 |
| 构建 | 静态，累积满 N 条后一次构建 |
| Rust crate | `xorf` v0.12.0 BinaryFuse8 |
| **每 TB 覆盖** | **1.09 万亿地址** |

### Fetch 侧：BinaryFuse16

| 参数 | 值 |
|------|-----|
| 误判率 | 0.0015% |
| bytes/element | ~2.013 |
| 全量 3.5 亿地址 | **~705 MB** |
| 用途 | gen_vs_fetch 实时碰撞，需低噪声 |

### L2 归档层：Bloom 10%

| 参数 | 值 |
|------|-----|
| 误判率 | 10% |
| bits/element | 4.79 |
| 10 亿条大小 | **572 MB** (vs L1 的 965 MB, 省 41%) |
| 用途 | 旧分片降级后仍可 O(1) 碰撞 |

---

## 五、三级存储架构（核心）

```
┌──────────────────────────────────────────────────────────────────┐
│  L1 活跃层                                                       │
│  BinaryFuse8, ~965 MB/分片, 0.39% FP                             │
│  碰撞方式: 加载 filter → O(1) 查询                                │
│  磁盘占比: 主体                                                   │
├──────────────────────────────────────────────────────────────────┤
│  L2 归档层                                                       │
│  Bloom 10% FP, ~572 MB/分片 (省 41%)                              │
│  碰撞方式: 加载 archive → O(1) 查询 → 命中后批量重生成验证          │
│  磁盘占比: 剩余空间                                               │
├──────────────────────────────────────────────────────────────────┤
│  L3 过期层                                                       │
│  零磁盘，仅 cursor 记录 counter 范围                               │
│  碰撞方式: 分布式回查 (scan-distribute/worker/collect)             │
│  算力来源: 零散 CPU / GPU                                         │
└──────────────────────────────────────────────────────────────────┘
```

### 5.1 生命周期流转

```
Generator 生成新分片
       │
       ▼
  ┌─ L1 活跃层 ─┐
  │  磁盘满 80%? │──否──→ 继续生成
  └───── 是 ────┘
       │
       ▼
  最老 L1 分片 → 重生成该分片地址(~29s) → 构建 Bloom 10% → 降级 L2
  删除原 BinaryFuse8 → 净省 393 MB/分片 (41%)
       │
  ┌─ L2 归档层 ─┐
  │  归档区满?   │──否──→ 继续
  └───── 是 ────┘
       │
       ▼
  最老 L2 分片 → 删除 → 降级 L3
  cursor 记录 purged counter 范围
```

### 5.2 磁盘分配模型（推荐 60/40）

| 总磁盘 | L1 (60%) | L2 (40%) | L1 覆盖 | L2 覆盖 | **L1+L2 总覆盖** |
|--------|---------|---------|---------|---------|-----------------|
| 1 TB | 0.6 TB | 0.4 TB | 5960 亿 | 6680 亿 | **1.26 万亿** |
| 4 TB | 2.4 TB | 1.6 TB | 2.39 万亿 | 2.67 万亿 | **5.06 万亿** |
| 8 TB | 4.8 TB | 3.2 TB | 4.77 万亿 | 5.34 万亿 | **10.1 万亿** |
| 16 TB | 9.6 TB | 6.4 TB | 9.54 万亿 | 10.7 万亿 | **20.2 万亿** |
| 64 TB | 38.4 TB | 25.6 TB | 38.2 万亿 | 42.7 万亿 | **80.9 万亿** |

L1+L2 内的地址碰撞为 **毫秒级 O(1)**，L3 需分布式回查。

### 5.3 L3 回查耗时 vs 磁盘（持续运行后）

生成速率 2.592 万亿/天。L3 大小 = 累计生成 − L1+L2 容量。

| 运行时长 | **4 TB** (GPU) | **8 TB** (GPU) | **16 TB** (GPU) | **64 TB** (GPU) |
|---------|---------------|---------------|----------------|----------------|
| 1 周 | 3.6 h | **0** ✅ | 0 ✅ | 0 ✅ |
| 1 月 | 4.2 天 | 3.9 天 | 3.3 天 | **0** ✅ |
| 3 月 | 13.2 天 | 12.6 天 | 12.3 天 | 8.8 天 |
| 半年 | 26.7 天 | 25.9 天 | 25.7 天 | 22.2 天 |
| 1 年 | 54.5 天 | 53.6 天 | 53.2 天 | 50.1 天 |

> ✅ = L3 为空，所有地址在 L1+L2 中有索引，零回查

### 5.4 每 TB 磁盘的边际收益

```
每追加 1 TB 磁盘:
  → L1+L2 多兜住 1.264 万亿地址
  → 每次 L3 全量回查省 1.76 小时 (GPU) / 10 小时 (5 台 CPU)
  → L3 开始累积的时间推迟 ~12 小时
```

---

## 六、Generator 设计

### 6.1 分片参数

| 参数 | 值 |
|------|-----|
| 每分片条数 | **10 亿** |
| L1 滤波器 | BinaryFuse8, **~965 MB/片** |
| L2 归档 | Bloom 10%, **~572 MB/片** |
| 构建内存 | ~1-2 GB (临时) |
| 恢复时间 | **~29 秒** (12 线程) |
| 命名 | L1: `filter_gen_{id:08}.bin`, L2: `archive_gen_{id:08}.bin` |

### 6.2 写入速率

```
30M keys/sec × 1.006 bytes = ~28.7 MB/sec (L1 刷盘)
每 33.3 秒满一个分片 → 任何磁盘类型均无压力
```

### 6.3 generator_cursor.json

```json
{
  "master_seed_hash": "sha256 前 16 hex",
  "current_counter": 54_321_000_000,
  "l1_shard_count": 12,
  "l2_shard_count": 30,
  "current_shard_entries": 321_000_000,
  "total_generated": 54_321_000_000,
  "purged_epochs": [
    {
      "counter_range": [0, 30_000_000_000],
      "purged_at": "2026-03-05T10:00:00Z",
      "collided_with_fetch_version": 5
    }
  ],
  "last_updated": "2026-03-05T18:00:00Z"
}
```

---

## 七、Fetcher 设计（纯内存）

### 7.0 获取器提取的地址来源（全覆盖）

凡能通过私钥或确定性规则推导的地址均被收集，用于碰撞：

| 来源 | 说明 |
|------|------|
| **块元信息** | `miner` / `author`（出块者） |
| **EIP-4895** | `withdrawals[].address`（提款地址） |
| **交易** | `from`（发送方）、`to`（接收方） |
| **合约创建** | RPC 返回的 `creates`；若无则用 CREATE(sender, nonce) = keccak256(rlp([sender, nonce]))[12..32] 计算 |

CREATE 合约地址由 RLP + Keccak-256 本地计算，不依赖 receipt。CREATE2 需解析 input（salt + initcode hash），当前未实现，可按需扩展。

### 7.1 以太坊地址增长实际数据

| 指标 | 数值 |
|------|------|
| 累计唯一地址 (2026 初) | ~3.5 亿 |
| 每天新增 | **~25 万** |
| 每月新增 | ~750 万 |
| 每年新增 | ~9000 万 |

### 7.2 内存占用

| 数据 | 结构 | 内存 |
|------|------|------|
| 全量 3.5 亿 (BinaryFuse16) | filter | **~705 MB** |
| 每日新增 25 万 | Vec<[u8;20]> | **~10 MB** |
| 每年新增 9000 万 | HashSet | ~3.6 GB |

**结论：纯内存完全无压力，不需要额外复杂数据结构。**

### 7.3 FetcherState

```rust
struct FetcherState {
    filter: BinaryFuse16,       // 全量 ~705 MB, gen_vs_fetch 实时碰
    new_addrs: Vec<[u8; 20]>,   // 新增池 ~10 MB/天, 供回查任务
    cursor: FetcherCursor,      // 区块遍历进度
}
```

### 7.4 fetcher_cursor.json

```json
{
  "start_block": 0,
  "end_block": 19500000,
  "last_synced_block": 1245832,
  "total_addresses": 350_000_000,
  "filter_version": 5,
  "new_addrs_since_last_export": 250000,
  "last_updated": "2026-03-05T12:00:00Z"
}
```

---

## 八、Collider 碰撞设计

### 8.1 双向碰撞

| 方向 | 做什么 | 数据来源 |
|------|--------|---------|
| gen_vs_fetch | Generator 生成时实时查 fetch filter | L1 实时、内存 |
| fetch_vs_gen | Fetcher 新地址查 gen filter (L1→L2→L3) | 磁盘/分布式 |

### 8.2 各层碰撞方式

**L1 碰撞 (活跃层)：**
```
加载 filter_gen_{B}.bin (BinaryFuse8, 965 MB)
  → 遍历 fetch 地址, 收集候选
  → 命中候选 → 重生成分片范围验证 (~29 秒)
  → 真实命中 → 输出
```

**L2 碰撞 (归档层)：**
```
加载 archive_gen_{B}.bin (Bloom 10%, 572 MB)
  → 同上流程, 但假阳性更多 (10% vs 0.39%)
  → 每个候选分片仍只需重生成一次 (~29 秒)
```

**L3 碰撞 (过期层, 分布式回查)：**
```
coordinator: birdhash scan-distribute
  → 切分旧 counter 范围为 task_*.json
  → 分发给零散 Worker (笔记本/云/朋友电脑)
worker: birdhash scan-worker
  → 纯计算, 零磁盘, 断点续算
collect: birdhash scan-collect
  → 合并结果
```

### 8.3 collider_cursor.json

```json
{
  "gen_vs_fetch": {
    "last_gen_counter": 54_321_000_000,
    "fetch_filter_version": 5,
    "total_checked": 54_321_000_000
  },
  "fetch_vs_gen_l1": {
    "completed_fetch_block_range": [0, 1245832],
    "current_fetch_offset": 8765432,
    "gen_l1_shard_version": 12
  },
  "fetch_vs_gen_l2": {
    "gen_l2_shard_version": 30,
    "last_checked_l2_shard": 28
  },
  "incremental": {
    "pending_new_l1_shards": [12, 13],
    "pending_new_l2_shards": [30]
  },
  "hits": 0,
  "last_updated": "2026-03-05T18:00:00Z"
}
```

### 8.4 批量碰撞策略

```
对每个 gen filter/archive 分片:
  1. 加载分片
  2. 遍历所有待碰 fetch 地址 → 收集候选 HashSet
  3. 重生成该分片 10 亿条 key (~29 秒 12 线程)
     → 每个 addr 查候选 HashSet O(1)
  4. 真实命中 → hits.txt; 假阳性 → 丢弃
```

---

## 九、分布式回查（L3）

### 9.1 架构

```
Coordinator (主机)
  ├─ birdhash scan-distribute → task_*.json (元数据 + new_addrs.bin)
  │
  ├─ Worker A (闲置笔记本): birdhash scan-worker --task task_00.json
  ├─ Worker B (云服务器):    birdhash scan-worker --task task_01.json
  ├─ Worker C (朋友电脑):    birdhash scan-worker --task task_02.json
  │
  └─ birdhash scan-collect --results-dir results/
```

### 9.2 任务文件

```json
{
  "task_id": 42,
  "master_seed": "hex...",
  "counter_start": 420_000_000_000,
  "counter_end":   430_000_000_000,
  "fetch_addresses_file": "new_addrs.bin",
  "fetch_count": 250000
}
```

任务极小：元数据几 KB + 新地址列表 ~10 MB/天。Worker 只需一个 birdhash 二进制。

### 9.3 Worker 断点续算

```json
{ "task_id": 42, "last_processed": 425_678_901_234, "status": "interrupted", "hits": [] }
```

### 9.4 分布式算力

| Workers | 合计算力 | 回查 1 万亿 counter |
|---------|---------|-------------------|
| 1 台 4 核 | 12M/s | 23 小时 |
| 3 台 4 核 | 36M/s | 7.7 小时 |
| 5 台 4 核 | 60M/s | **4.6 小时** |
| 10 台混合 | 100M/s | 2.8 小时 |
| 1 GPU | 200M/s | **1.4 小时** |

### 9.5 CLI

```bash
birdhash scan-distribute --range 0 1000000000000 --chunk-size 10000000000 \
  --fetch-addrs new_addrs.bin --out tasks/
birdhash scan-worker --task tasks/task_00042.json --output results/result_00042.json
birdhash scan-collect --results-dir results/
```

---

## 9.5+ Fetcher 历史全量获取 + 归档方案

### 问题
原 Fetcher 只有基础 RPC 调用，缺乏：
1. 重试 / 指数退避 → 长时间全量同步中 RPC 偶发失败会中断
2. 区块归档 → `all_addrs.bin` 只存地址，无法追溯来源区块
3. RPC URL 仅 CLI 传入 → 每次都要手动输，不方便
4. 批量大小固定 10 → 吞吐低

### 方案：分段归档 + 增强 RPC

**Archive 分段文件** `data/fetcher/archive_{start:09}_{end:09}.bin`:
```
Header (32 bytes):
  magic: "BHFA" (4)
  version: u32 le (4)
  start_block: u64 le (8)
  end_block: u64 le (8)  // inclusive
  total_addrs: u64 le (8)

Records (变长, 连续):
  block_number: u64 le (8)
  addr_count: u16 le (2)
  addrs: [u8; 20] × addr_count
```

- 每 10 万块一个归档文件，~22M 块 → ~220 个文件
- 每文件约 300-600 MB（取决于区块交易密度）
- 追加写入当前段，写满切换下一段
- 地址同时 append 到 `all_addrs.bin` 供 filter 构建

**RPC 增强**:
- 可配重试次数 + 指数退避 (1s → 2s → 4s → 8s → 16s)
- 可配 batch_size (默认 10，可调至 50+)
- RPC URL 支持 config.toml 配置，CLI `--rpc` 覆盖
- 请求间可选限速 (避免被 provider 封)

**Config 新增**:
```toml
[fetcher]
rpc_url = "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"
batch_size = 10       # blocks per JSON-RPC batch
retry_count = 5       # max retries per request
retry_base_ms = 1000  # initial backoff
archive_segment = 100000  # blocks per archive file
```

## 十、GPU 加速（Phase 9, 可选）

GPU 的最大价值在 **L3 回查**（纯计算, 零磁盘 I/O）:

```
CPU 上传 new_fetch_addrs (HashSet, 几 MB) → GPU 显存
GPU 批量并行: HMAC-SHA256 → secp256k1 → keccak256 → HashSet check
CPU ← 仅回传命中 (counter, addr) 对
```

| 场景 | CPU (12T) | GPU | 加速比 |
|------|----------|-----|--------|
| L3 回查 (零磁盘) | 35M/s | 200-400M/s | **6-11x** |
| Generator (受磁盘) | 35M/s | 受磁盘限制 | ~1x |

技术方案首选 wgpu (跨平台 Vulkan/Metal/DX12)。

---

## 十一、完整目录结构

```
birdhash/
├── Cargo.toml
├── config.toml
├── src/
│   ├── main.rs              # CLI 入口 (clap 子命令)
│   ├── keygen.rs             # seed+counter → privkey → addr
│   ├── filter.rs             # BinaryFuse8/16 + Bloom 归档 (xorf)
│   ├── cursor.rs             # 通用游标 JSON 读写
│   ├── generator.rs          # Generator (确定性 + 实时碰 + L1 刷盘)
│   ├── fetcher.rs            # Fetcher (纯内存, BinaryFuse16 + 新增池)
│   ├── collider.rs           # Collider (L1/L2 碰撞 + 断点续碰)
│   ├── scanner.rs            # 分布式回查 (distribute/worker/collect)
│   ├── lifecycle.rs          # 磁盘生命周期 (L1→L2→L3 降级 + purge)
│   ├── stats.rs              # 实时统计
│   ├── config.rs             # 配置加载
│   └── gpu/                  # Phase 9 (可选)
│       ├── mod.rs
│       ├── compute.rs
│       └── shaders/
├── data/
│   ├── master_seed.key                    # 32B 种子 (chmod 600)
│   ├── generator/
│   │   ├── generator_cursor.json
│   │   ├── filter_gen_{id:08}.bin         # L1 (~965 MB, BinaryFuse8)
│   │   └── archive_gen_{id:08}.bin        # L2 (~572 MB, Bloom 10%)
│   ├── fetcher/
│   │   ├── fetcher_cursor.json
│   │   ├── filter_fetch.bin               # 全量 BinaryFuse16 (~705 MB)
│   │   └── new_addrs.bin                  # 新增地址导出
│   ├── results/
│   │   ├── collider_cursor.json
│   │   └── hits.txt
│   └── tasks/                             # 分布式回查任务/结果
│       ├── task_{id:05}.json
│       └── result_{id:05}.json
├── task_plan.md
├── findings.md
└── progress.md
```

---

## 十二、依赖清单

| crate | 用途 |
|-------|------|
| secp256k1 | 椭圆曲线 (C binding, FFI) |
| tiny-keccak | Keccak-256 |
| hmac + sha2 | HMAC-SHA256 确定性密钥推导 |
| xorf | BinaryFuse8/16 滤波器 |
| rayon | 多线程并行 |
| clap (derive) | CLI |
| serde + serde_json | cursor/config 序列化 |
| rand | master_seed 初始生成 |
| ctrlc | 优雅退出 |

---

## 获取器职责分离（Phase 10 设计）

- **获取器仅负责**：按区间 [start_block, end_block] + 指定 rpc_url 拉取块的**原始数据**并落盘。
- **输出约定**：目录 `{start_block}-{end_block}/` 下两个文件：
  - 原始块：一行一块的文本（建议 JSONL），按块号顺序追加。
  - 检查点：如 `checkpoint.json`，含 start/end、last_fetched_block、status，用于断点续跑。
- **不再由获取器完成**：all_addrs.bin、filter_fetch.bin、new_addrs.bin、AddressStore、BinaryFuse16 构建、BHFA 归档（或改为可选/仅作原始块另一种格式）。上述碰撞用中间数据改由**碰撞器（或独立导入流水线）**在后续计划中读取原始块文件后生成。

## Issues Encountered
| Issue | Resolution |
|-------|------------|
| 全量存 privkey+addr ~131 TB/天 | 确定性生成, 丢弃文件 |
| bloom 0.0001% 仍 8.7 TB/天 | BinaryFuse8 → 2.4 TB/天 (省 3.6x) |
| 磁盘满了无法继续 | 三级架构: L1→L2→L3 自动降级 |
| 旧 epoch 回查线性增长 | 分布式回查 + 零散 CPU 利用 |
| Fetcher 存储需求 | 纯内存, 每天新增仅 ~10 MB |

## Resources
- secp256k1: https://docs.rs/secp256k1
- tiny-keccak: https://docs.rs/tiny-keccak
- hmac + sha2: https://docs.rs/hmac, https://docs.rs/sha2
- **xorf (BinaryFuse8/16): https://docs.rs/xorf**
- rayon: https://docs.rs/rayon
- wgpu: https://docs.rs/wgpu
- Xor Filters 论文: https://arxiv.org/pdf/1912.08258
- Etherscan 地址统计: https://etherscan.io/chart/address
