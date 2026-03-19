# birdhash collide — 碰撞器

单进程 N 线程生成地址，实时与 BF(BinaryFuse16) 过滤器碰撞。命中即追加到 CSV。

## 架构

```
┌───────────────────────────────────────────┐
│              birdhash collide              │
├───────────────────────────────────────────┤
│  启动时加载 data/fetcher/filter.*.bin     │
│  每 60 秒热更新 BF 过滤器                │
├───────────────────────────────────────────┤
│  N 个 worker 线程：                       │
│    N(序号) → ID → 助记词 → seed           │
│    → (account, index) → 私钥 + 地址       │
│    → 地址查 BF → 命中则写 hits_bf.csv     │
├───────────────────────────────────────────┤
│  主线程：保存检查点 + 刷新进度行          │
└───────────────────────────────────────────┘
```

- **无布谷鸟过滤器**，仅 BF 碰撞
- **无 socket / 多进程**，单进程完成所有工作
- **生成的私钥地址只在内存中短暂停留**，碰撞完即释放，不写磁盘
- **检查点**：`data/results/collider_cursor.json`，记录 `next_address_index`，断点续碰
- **命中输出**：`data/results/hits_bf.csv`，表头 `地址,私钥,派生路径,助记词`
- **BF 热更新**：每 60 秒扫描 `data/fetcher/filter.*.bin` 重新加载

## 前置条件

1. 已运行 `birdhash init` 创建目录
2. 已运行 `birdhash gen-derivation-candidates` 生成候选 index 文件
3. 首次运行会自动创建 `assets/generator_seed.key`（32 字节种子）
4. 已通过 `birdhash fetch` + `birdhash build-filter` 构建 BF 过滤器

## CLI

```bash
# 编译
cargo build --release

# 启动碰撞器（默认 4 线程）
./target/release/birdhash collide

# 指定线程数
./target/release/birdhash collide --threads 8

# 指定配置文件
./target/release/birdhash collide -c config.toml --threads 16
```

## 运行时输出

```
  碰撞器启动 | BF 90 个 | 种子 assets/generator_seed.key | 候选 14 | 路径/ID 1568 | 线程 8 | 断点 N=0
  命中写入 data/results/hits_bf.csv | 检查点 data/results/collider_cursor.json
  N=1234567 | ID=787 | 速度 2600/s | 已生成 1234567 | 命中 0 | BF 90 个
```

进度行固定位置刷新，不滚动。

## 其他常用命令

```bash
# 初始化目录
./target/release/birdhash init

# 拉取区块
./target/release/birdhash fetch --rpc https://your-rpc-url

# 构建 BF 过滤器
./target/release/birdhash build-filter

# 查询地址是否在 BF 中
./target/release/birdhash filter-query 0x1234...abcd

# 查看某 ID 的助记词/私钥/地址
./target/release/birdhash id-info 42

# 导出某 ID 全部派生为 CSV
./target/release/birdhash id-info 42 --all
```
