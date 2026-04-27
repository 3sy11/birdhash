# birdhash

Ethereum 块拉取与地址过滤器：拉取链上块、提取地址并构建 BinaryFuse16 过滤器，供碰撞器做地址命中检测。

## 构建

```bash
cargo build --release
# 可执行文件: target/release/birdhash
```

## 配置

默认读取当前目录 `config.toml`，可通过 `-c/--config` 指定。常用项：

- `[general]`：`data_dir`（默认 `data`）、`assets_dir`（默认 `assets`）
- `[fetcher]`：`rpc_url` 或 `rpc_urls`、`timeout_secs`、`poll_interval_secs` 等

未配置 RPC 时，fetch 相关命令需通过 `--rpc <URL>` 传入。

---

## 命令行一览

| 命令 | 说明 |
|------|------|
| `init` | 创建 data/assets 目录并生成 generator 种子 |
| `fetch` | 按批次拉取区块（指定批次或持续拉取最新批） |
| `build-filter` | 扫描所有 fetcher 目录的原始数据，生成 BF 过滤器到 data/filter/ |
| `filter-query` | 查询地址是否在 BF 中（从 data/filter/ 加载） |
| `fetch-test` | 拉取单块并打印块信息与提取的地址 |
| `collide` | 碰撞器：多线程生成地址与 BF 碰撞，命中写 CSV |
| `id-info` | 查询某 ID 的助记词/地址/私钥，或导出派生 CSV |

---

## 命令与示例

### init

创建 data、fetcher、results 等目录，并生成 `assets/generator_seed.key`。

```bash
birdhash init
birdhash -c /path/to/config.toml init
```

### fetch

按批次拉取区块，数据写入 `data/fetcher/ranges/<start>-<end>/`（chunk_*.jsonl）。不指定 `--batch` 时拉取最新批次；若配置了 `poll_interval_secs` 会轮询新区块持续拉取。

```bash
# 拉取最新批次（单批）
birdhash fetch --rpc https://eth.llamarpc.com

# 拉取指定批次（如第 247 批）
birdhash fetch --batch 247 --rpc https://eth.llamarpc.com

# 多批次并行拉取
birdhash fetch --batch 245,246,247 --rpc https://eth.llamarpc.com

# 指定输出目录（默认 data/fetcher/ranges）
birdhash fetch --batch 247 --output-dir /path/to/ranges --rpc https://eth.llamarpc.com
```

### build-filter

自动扫描 `data/` 下所有 `fetcher*` 目录（`data/fetcher/`、`data/fetcher_eth/`、`data/fetcher_bnb/` ...）的 ranges 数据，增量构建 BF 过滤器，输出到统一目录 `data/filter/`。已有 BF 中已存在的地址自动跳过，已有同名 BF 文件不会重复构建。

```bash
# 自动扫描所有 fetcher 目录，增量构建
birdhash build-filter

# 仅处理指定批次
birdhash build-filter --batch 246,247

# 手动指定 source 和 output
birdhash build-filter --source /path/to/ranges --output /path/to/filter
```

### filter-query

查询单个地址是否命中 BF，输出 `1`（命中）或 `0`。默认从 `data/filter/` 加载，如为空自动回退到 `data/fetcher/`。

```bash
birdhash filter-query 0xdadb0d80178819f2319190d340ce9a924f783711

# 指定 BF 目录或单个 .bin 文件
birdhash filter-query 0xafc8bc72c756ddc1957b276f5fbcb989dad93730 --filter data/filter
birdhash filter-query 0x0000000000000000000000000000000000000001 --filter data/filter/filter.1-130.bin
```

### fetch-test

拉取单个区块并打印 number、hash、miner、transactions 等及提取出的地址列表，用于调试。

```bash
# 拉取最新块
birdhash fetch-test --rpc https://eth.llamarpc.com

# 拉取指定块高
birdhash fetch-test --rpc https://eth.llamarpc.com --block 19000000
```

### collide

启动碰撞器：多线程按 ID 派生地址，与 BF 碰撞，命中写入 `data/results/hits_bf.csv`，检查点 `data/results/collider_cursor.json`。

```bash
# 默认 4 线程（纯 CPU 模式）
birdhash collide

# 指定线程数
birdhash collide --threads 8

# GPU 加速模式（需要 NVIDIA 驱动，无需 CUDA Toolkit）
# CPU 负责 BIP39/PBKDF2，GPU 批量执行 BIP32+secp256k1+Keccak-256
birdhash collide --gpu
birdhash collide --gpu --threads 8   # --threads 控制 CPU PBKDF2 并行度
```

**GPU 模式说明**
- 需要已安装 NVIDIA 驱动（`nvcuda.dll`），不需要 CUDA Toolkit / nvcc
- Kernel 编译为 PTX，由驱动 JIT 加速到本机 GPU 架构
- 每轮 batch 512 个 seed × 全部路径，并行度可达数万线程
- 推荐 `--threads` 设为 CPU 核心数（PBKDF2 并行度），GPU 自动满负荷运行

### id-info

查看某 ID 对应的助记词、地址、私钥等；加 `--all` 导出该 ID 下全部派生到 CSV。

```bash
birdhash id-info 0
birdhash id-info 12345 --all
```

---

## 目录约定

- `data/`：数据根目录（可由 config 覆盖）
  - `data/fetcher/`、`data/fetcher_eth/`、`data/fetcher_bnb/` ...：各链原始块数据（ranges/）
  - `data/filter/`：所有 BF 过滤器（filter.*.bin），build-filter 统一输出到此
  - `data/results/`：碰撞结果（hits_bf.csv）、检查点（collider_cursor.json）
- `assets/`：种子、派生候选等

详见 [COLLIDER.md](COLLIDER.md) 了解碰撞器流程与 BF 热更新。
