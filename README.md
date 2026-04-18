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
| `build-filter` | 合并 ranges 下小文件并生成 BF 过滤器到 data/fetcher |
| `filter-query` | 查询地址是否在 BF 中 |
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

扫描 `data/fetcher/ranges`（或 `--data-dir`/`--source` 指定目录），合并各批次小文件为 `blocks.jsonl`，再根据「当前仍有 range 数据的批次」生成 BF，输出到 `data/fetcher`（或 `--output`）。会读取 fetcher 的 meta，不合并、不包含「当前正在写入」的批次。**仅会写入本次涉及批次的 BF 文件，不会删除目录下已有的其他 `filter.*.bin`**（例如已把 BF 放到 data、删掉部分 range 以省空间后，再跑 build-filter 只会为仍存在 range 的批次生成/覆盖对应 BF，不会动其它已建好的 BF）。

```bash
# 使用默认 data 目录，处理所有可合并批次并生成 BF
birdhash build-filter

# 指定 data 目录（ranges = <data_dir>/fetcher/ranges，输出 = <data_dir>/fetcher）
birdhash build-filter --data-dir /path/to/data

# 仅对指定批次生成 BF
birdhash build-filter --batch 246,247

# 自定义数据源与输出目录
birdhash build-filter --source /path/to/ranges --output /path/to/fetcher
```

### filter-query

查询单个地址是否命中 BF，输出 `1` 或 `0`。未指定 `--filter` 时从 config 的 `data_dir/fetcher` 加载所有 `filter.*.bin`。

```bash
# 使用默认 data/fetcher 下的 BF
birdhash filter-query 0xdadb0d80178819f2319190d340ce9a924f783711

# 指定 BF 目录或单个 .bin 文件
birdhash filter-query 0xafc8bc72c756ddc1957b276f5fbcb989dad93730 --filter data/fetcher
birdhash filter-query 0x0000000000000000000000000000000000000001 --filter data/fetcher/filter.247-247.bin
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

- `data/`：数据根目录（可由 config 或 `--data-dir` 覆盖）
  - `data/fetcher/`：meta.json、BF 文件（filter.*.bin）
  - `data/fetcher/ranges/<start>-<end>/`：各批次块数据（chunk_*.jsonl 或 blocks.jsonl）
  - `data/results/`：碰撞结果、检查点
- `assets/`：种子、派生候选等

详见 [COLLIDER.md](COLLIDER.md) 了解碰撞器流程与 BF 热更新。
