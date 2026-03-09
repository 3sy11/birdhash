# birdhash

以太坊区块获取器 — 按批拉取块数据、构建地址过滤器 (BinaryFuse16)。

## 命令

| 命令 | 说明 |
|------|------|
| `init` | 创建数据目录与 fetcher 目录 |
| `fetch` | 拉取块到 ranges；不指定 batch 时拉取最新一批，追平后按 `poll_interval_secs` 轮询新区块 |
| `build-filter` | 从已拉取块构建地址过滤器（无参=全量批次，`--batch`=指定或合并批） |
| `filter-query` | 查询地址是否在过滤器中，输出 0/1（先加载全部 filter 再查询） |
| `fetch-test` | 拉取单块并打印块信息与提取的地址（调试用） |
| `gen-derivation-candidates` | 生成派生路径 index 候选，写入 `assets/derivation_candidates/derivation_candidates.txt`；**account 取值范围 0-111（含111），index 取值范围为该文件所列之值** |

## 快速开始

```bash
cargo build --release

# 创建目录
birdhash init

# 拉取最新一批（追平后若 poll_interval_secs>0 则轮询新区块）
birdhash fetch

# 拉取指定批
birdhash fetch --batch 1 2 3 4
birdhash fetch --rpc https://eth.llamarpc.com

# 构建过滤器（无参=数据目录下全部批次）
birdhash build-filter
birdhash build-filter --batch 1 2 3 4

# 查询地址
birdhash filter-query 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf
birdhash filter-query 0x... --filter data/fetcher

# 调试单块
birdhash fetch-test --rpc https://eth.llamarpc.com --block 18000000

# 生成派生路径 index 候选（account 取 0-111，index 取生成文件中的值）
birdhash gen-derivation-candidates
birdhash gen-derivation-candidates --output assets/derivation_candidates/derivation_candidates.txt
```

## 配置

默认读取当前目录 `config.toml`，可用 `--config path/to/config.toml` 覆盖。

```toml
[general]
data_dir = "data"
assets_dir = "assets"   # 助记词表、派生候选等

[fetcher]
# rpc_url = "https://..."
# rpc_urls = ["https://eth.llamarpc.com", "https://cloudflare-eth.com"]
poll_interval_secs = 12   # 最新批追平后轮询间隔(秒)，0=只跑历史不轮询
```

## 获取器行为

- **批与目录**：块按批存储于 `data/fetcher/ranges/`。批 1 = 块 1..100000，批 2 = 100001..200000，以此类推。范围目录名为 `{seg_start}-{seg_end}`（如 `0-99999`），其内为 `blocks.jsonl` + `checkpoint.json`。
- **最新批次**：拉取最新一批时，块先写入 `latest/`，按 1000 块/文件共 100 个文件（`chunk_000.jsonl` … `chunk_099.jsonl`），并每 100 块重建当前 chunk 的小过滤器 `filter.{batch_id}.00.bin` … `.99.bin`。满 10 万块时合并为单块文件、归档为范围目录、构建 `filter.{batch_id}-{batch_id}.bin` 并写元信息，删除 100 个小过滤器与 chunk 文件，新建空 `latest` 继续拉取。
- **轮询**：当不指定 `--batch`（即拉取最新一批）且配置中 `poll_interval_secs > 0` 时，追平当前链高后进入循环：每隔 `poll_interval_secs` 秒检查链最高高度，若有新区块则从当前 checkpoint 逐批拉取到最新，然后继续检查高度，往复循环。

## 过滤器

- **命名**：大过滤器 `filter.{min_batch}-{max_batch}.bin`（批次 ID），小过滤器（latest 用）`filter.{batch_id}.00.bin` … `.99.bin`。每个大 filter 对应元信息文件（如 `filter.44-44_meta.json`）记录构建批次与最高块高。
- **filter-query**：不传 `--filter` 时使用 fetcher 目录下所有 `filter.*.bin`；可传目录或单文件。先全部加载到内存再查询，加载列表会打印到 stderr。

## 派生路径候选（account / index）

- **account** 取值范围：**0～111**（含 111）。
- **index** 取值范围：由命令 `gen-derivation-candidates` 生成的文件 `assets/derivation_candidates/derivation_candidates.txt` 中所列之值（一行一个）。生成规则：特定条件数（仅 2 种数码、易记结构等）+ 全范围 0-1000 + 倒数 100。

## 目录结构

```
assets/                              # 资源目录（不在 data 下）
├── wordlist/
│   └── bip39_en.txt                 # BIP39 英文助记词表
└── derivation_candidates/
    └── derivation_candidates.txt    # gen-derivation-candidates 生成，index 候选

data/
└── fetcher/
    ├── filter.1-1.bin, filter.2-2.bin, ...   # 按批过滤器
    ├── filter.44-44_meta.json               # 元信息
    ├── filter.44.00.bin ... filter.44.99.bin  # latest 小过滤器（满 10 万后删除）
    └── ranges/
        ├── latest/                           # 当前最新批（100 个 chunk_xxx.jsonl）
        │   ├── chunk_000.jsonl ... chunk_099.jsonl
        │   └── checkpoint.json
        ├── 0-99999/
        ├── 100000-199999/
        └── ...
```

## 测试

```bash
cargo test
cargo clippy
cargo fmt --check
```
