# birdhash

以太坊地址碰撞检测器 — 三级存储架构 (L1 / L2 / L3)

## 架构

```
Generator (seed+counter)  ──→  L1 BinaryFuse8  ──→  Collider  → hits.txt
                          ──→  L2 Bloom 10%    ──→
Fetcher (chain addrs)     ──→  BinaryFuse16    ──→  L3 分布式回查
```

- **Generator**: 确定性生成私钥+地址 (HMAC-SHA256 + secp256k1 + keccak256)，存 BinaryFuse8 滤波器分片
- **Fetcher**: 遍历链上地址 → 追加式存储 + BinaryFuse16 filter (~705 MB 全量)
- **Collider**: 双向碰撞 L1/L2 + 批量重生成验证 + 断点续碰
- **Scanner**: 分布式 L3 回查，利用零散 CPU/GPU 算力

## 三级存储

| 层 | 滤波器 | 每分片 | FP | 碰撞方式 |
|----|--------|--------|-----|---------|
| L1 活跃 | BinaryFuse8 | ~965 MB (10 亿条) | 0.39% | O(1) 加载查询 |
| L2 归档 | Bloom 10% | ~572 MB | 10% | O(1) + 重生成验证 |
| L3 过期 | 无 (零磁盘) | — | — | 分布式重生成 |

## 快速开始

```bash
cargo build --release

# 初始化 (创建 master_seed + 数据目录)
birdhash init

# 启动生成器 (可选 --threads)
birdhash generate
birdhash generate --threads 8

# 启动 Fetcher：从创世块 0 补到最新
birdhash fetch
# 首次运行会从 block 0 开始；断点续跑会从 cursor 继续。若要从 0 重新补全，清空 data/fetcher/ 后重跑。
# 可选：birdhash fetch --rpc https://...

# 运行碰撞 (L1 + L2)
birdhash collide

# 本地 L3 重生成碰撞
birdhash collide --regenerate-range 0 1000000000

# 查看状态
birdhash status
```

## 配置

所有命令支持 `--config path/to/config.toml`，默认读取当前目录 `config.toml`。

```toml
[general]
data_dir = "data"
threads = 0              # 0 = 全部核心

[generator]
shard_size = 1_000_000_000  # 10 亿条/分片

[disk]
l1_ratio = 0.6           # L1 占磁盘预算 60%
watermark = 0.8           # 80% 触发 L1→L2 降级

[fetcher]
# 单 URL 或多 URL（失败时按序降级）
# rpc_url = "https://..."
# rpc_urls = ["https://eth.llamarpc.com", "https://cloudflare-eth.com"]
poll_interval_secs = 12  # 实时轮询间隔(秒)，0=只跑历史不轮询
```

## 磁盘生命周期

```bash
# L1→L2 降级 (保留最近 5 个 L1 分片)
birdhash purge --archive --keep-last 5

# L2→L3 淘汰 (保留最近 3 个 L2 分片)
birdhash purge --keep-last 3
```

## 分布式 L3 回查

```bash
# 1. 协调器: 切分任务
birdhash scan-distribute --range 0 1000000000000 \
  --chunk-size 10000000000 --fetch-addrs data/fetcher/new_addrs.bin

# 2. Worker (可分发到任意机器, 只需二进制 + task.json + fetch addrs)
birdhash scan-worker --task data/tasks/task_00042.json --output results/

# 3. 收集结果
birdhash scan-collect --results-dir results/
```

Worker 支持断点续算，中断后重新运行同一 task 即可从上次进度继续。

## 关键参数

- 生成速率: ~30M keys/sec (12 线程)
- 1 TB 磁盘 ≈ 1.09 万亿 L1 地址覆盖
- 4 TB 磁盘: L1+L2 ≈ 5 万亿有索引覆盖
- `master_seed.key` **必须安全备份**，丢失则无法恢复已生成的私钥

## 目录结构

```
data/
├── master_seed.key              # 32B 种子 (必须安全备份)
├── generator/
│   ├── generator_cursor.json
│   ├── filter_gen_*.bin         # L1 BinaryFuse8 (~965 MB)
│   └── archive_gen_*.bin        # L2 Bloom 10% (~572 MB)
├── fetcher/
│   ├── fetcher_cursor.json
│   ├── filter_fetch.bin         # BinaryFuse16 (~705 MB)
│   ├── all_addrs.bin            # 追加式全量地址
│   └── new_addrs.bin            # 新增地址导出
├── results/
│   ├── collider_cursor.json
│   └── hits.txt                 # 命中记录
└── tasks/                       # 分布式回查
    ├── task_*.json
    ├── state_*.json             # Worker 断点状态
    └── result_*.json
```

## 测试

```bash
cargo test           # 47 个单元测试
cargo clippy         # 0 warnings
cargo fmt --check    # 格式检查
```
