# Progress Log

## Session: 2026-03-02

### Phase 1: 需求与技术选型 + 架构设计 (最终版)
- **Status:** complete
- 迭代历程:
  - v1-v3: 基础三组件架构 + bloom 参数调研
  - v4: 存储膨胀分析 → 发现 privkey+addr 全存 ~131 TB/天 不可行
  - v5: 确定性生成 (seed+counter), 丢弃 gen 文件
  - v6: bloom-only 方案 → 仍 8.7 TB/天
  - v7: BinaryFuse8 替代 Bloom → 2.4 TB/天 (省 3.6x), 分片 10 亿/片
  - v8: 磁盘满策略 → 碰完即删 + 循环 epoch + 按需回查
  - v9: Fetcher 纯内存设计确认 (每天新增仅 ~10 MB)
  - v10: 分布式回查设计 (scan-distribute/worker/collect)
  - **v11 最终版: 三级存储架构 (L1 BinaryFuse8 / L2 Bloom 10% / L3 分布式回查)**
- 关键决策:
  - Gen: BinaryFuse8, 10 亿/片, ~965 MB, 0.39% FP
  - Fetch: BinaryFuse16 纯内存 ~705 MB, 无额外数据结构
  - L1→L2 降级省 41% 磁盘, L2→L3 零磁盘分布式回查
  - GPU 主要价值在 L3 回查 (6-11x 加速)
  - 4 TB 磁盘 L1+L2 ≈ 5 万亿有索引覆盖
- Files: task_plan.md, findings.md, progress.md

### Phase 2: 项目脚手架
- **Status:** complete
- Actions:
  - `cargo init --vcs none` + Cargo.toml 含 16 个依赖 (edition 2021)
  - 11 个 src 模块: main/keygen/filter/cursor/config/generator/fetcher/collider/scanner/lifecycle/stats
  - keygen.rs: 完整实现 KeyGen (HMAC-SHA256 → secp256k1 → keccak256), load_or_create_seed, seed_hash_id
  - filter.rs: BinaryFuse8/16 build/save/load + BloomFilter (bitvec) insert/contains/save/load
  - cursor.rs: 6 个 cursor struct + Default impl + 通用 load/save/load_or_default
  - config.rs: AppConfig 全部路径 + ensure_dirs
  - main.rs: clap 9 子命令 (init/generate/fetch/collide/status/purge/scan-distribute/scan-worker/scan-collect)
  - stats.rs: gather_status + print_status
  - config.toml + data/{generator,fetcher,results,tasks}/ + .gitignore + README.md
  - `cargo check` ✅, `cargo test` ✅ 6/6 passed
- Files: Cargo.toml, src/*.rs, config.toml, README.md, .gitignore

### Phase 3: 核心基础层
- **Status:** complete
- Actions:
  - keygen.rs: +rayon 并行 (par_batch_addresses, par_batch_fingerprints), +derive_keypair, +privkey_hex_to_addr_hex
  - keygen.rs: +8 个测试 (含 ETH 已知向量 privkey=1, 推导链一致性, 并行=顺序验证)
  - filter.rs: serde_json → **bincode** 序列化 (关键优化: 10 亿条 4 GB JSON → 965 MB binary)
  - filter.rs: addr_to_u64 从 8 字节截取改为 SipHash-style 20 字节全混合
  - filter.rs: +atomic_write (write .tmp → rename), +9 个测试 (FP 率验证 + round-trip)
  - cursor.rs: save_cursor → atomic (write .json.tmp → rename), +PartialEq derive, +5 个 round-trip 测试
  - main.rs: init 命令 (ensure_dirs + seed + gen_cursor), status 命令 (gather + print)
  - Cargo.toml: +bincode 1
  - `cargo test` 23/23 ✅
- Findings:
  - privkey=1 验证通过, 我们的 secp256k1+keccak256 推导链标准正确
  - bincode 是 xorf filter 最优序列化选择 (serde_json 会 4x 膨胀)
  - BinaryFuse8 实测 FP < 1% (理论 0.39%), Bloom 实测 FP ~10% (符合设计)

### Phase 4: Generator
- **Status:** complete
- Actions:
  - generator.rs: 重写为完整主循环 Generator struct (config/cursor/keygen/shard_fps/fetch_filter/shutdown)
  - generator.rs: run() 主循环 — par_batch_fingerprints → 累积 Vec<u64> → shard_size 满 flush_shard()
  - generator.rs: flush_shard() — build_fuse8 → save_fuse8 → clear → l1_shard_count++ → save_cursor
  - generator.rs: gen_vs_fetch 实时碰撞 — 可选 load BinaryFuse16, 逐 fingerprint contains 检查
  - generator.rs: resume_partial_shard() — cursor.current_shard_entries>0 时重新生成部分 shard
  - generator.rs: Ctrl+C — ctrlc::set_handler + AtomicBool, 二次按直接 exit(1)
  - generator.rs: 实时统计 — \r 覆写行 (elapsed/total/rate M/s/shard%/L1_files/gvf_candidates)
  - generator.rs: +3 测试 (shard 文件验证, partial resume, 完整 tiny run)
  - main.rs: +cmd_generate() — seed hash 校验 / threads 配置 / load_fetch_filter / Generator::run()
  - `cargo test` 26/26 ✅
- Findings:
  - BATCH_SIZE=100K 在 tiny test 中 shard_size=500 时正常工作 (batch 自动 min 到 shard 剩余)
  - 并发测试需每 test 独立 tmp dir, 否则 remove_dir_all 互相影响
  - BinaryFuse8 构建+保存 500 条 < 1ms, 正式 10 亿条预计 ~30s build + 数秒 write

### Phase 5: Fetcher
- **Status:** complete
- Actions:
  - fetcher.rs: 完整重写 (~280 行), 包含 AddressStore / EthRpc / Fetcher / 导入导出 / 测试
  - AddressStore: append-only 平文件 `all_addrs.bin`, open/append/read_all_fingerprints
  - EthRpc: ureq JSON-RPC 客户端, get_latest_block_number + 批量 get_blocks_addresses
  - extract_addresses_from_block: 解析 block JSON, 提取 tx from/to 地址
  - parse_hex_addr: 0x 前缀兼容 + 40 hex chars → [u8;20]
  - Fetcher::run(): 批量取块 → dedup(filter) → append store → cursor → 定期 rebuild filter
  - rebuild_filter: read_all_fingerprints → sort+dedup → build_fuse16 → save + 导出 new_addrs
  - new_addrs.bin: 8-byte LE count header + flat [u8;20] 数组
  - config.rs: +all_addrs_path()
  - main.rs: +cmd_fetch() (load cursor → Fetcher::new → run)
  - Cargo.toml: +ureq 2 (features=["json"])
  - +7 测试 (store/new_addrs/parse_hex/block_json/filter_rebuild)
  - `cargo test` 33/33 ✅
- Findings:
  - 近似 dedup 用 BinaryFuse16.contains() 代替 HashSet, 内存从 ~5.6 GB 降到 ~705 MB
  - all_addrs.bin 追加式设计: 重启无需重遍历已同步块, 只需 resume from cursor
  - filter rebuild 需读 all_addrs → 计算全量 fingerprint, 350M 地址约 7 GB 磁盘 I/O
  - 批量 JSON-RPC (10 blocks/request) 大幅降低 HTTP 开销

### Phase 6: Collider + 三级碰撞
- **Status:** complete
- Actions:
  - collider.rs: 完整重写 (~270 行), L1/L2 碰撞 + regenerate_range + hit 记录 + 测试
  - collide_fuse8_shard: load L1 BinaryFuse8 → scan fetch addrs → HashSet<Address> candidates → rayon regenerate → exact match
  - collide_bloom_shard: load L2 Bloom → 同流程 (10% FP → 更多 candidates, 同一次 regen)
  - regenerate_and_verify: rayon par_iter [start, end) → derive_address → candidates.contains → derive_keypair
  - scan_range_vs_set: L3 本地纯重生成, 分 1M chunk 扫描, 支持 Ctrl+C 中断
  - record_hits: append hits.txt (timestamp | counter | privkey_hex | addr_hex)
  - load_fetch_addresses: 优先 all_addrs.bin, fallback new_addrs.bin
  - fetcher.rs: AddressStore +read_all_addresses() 方法
  - main.rs: +cmd_collide() 支持 --regenerate-range START END 切换 L3 模式
  - +5 测试 (L1 planted hit / L1 no hits / L2 bloom planted hit / regen planted hit / hit format)
  - `cargo test` 38/38 ✅
- Findings:
  - L1 candidates 量级: 350M fetch × 0.39% FP ≈ 1.365M candidates/shard → HashSet ~82 MB, 完全可行
  - L2 candidates 量级: 350M fetch × 10% FP ≈ 35M candidates/shard → HashSet ~2.1 GB, 大但可接受
  - regenerate_and_verify 全程 rayon 并行, 1B 条约 29s (12 线程), 远快于 filter 加载时间
  - planted hit 测试证明: filter→候选→重生成→精确匹配→命中记录 全链路正确

### Phase 7: 磁盘生命周期 + 分布式回查
- **Status:** complete
- Actions:
  - lifecycle.rs: 完整重写 (~150 行)
    - downgrade_l1_to_l2: 遍历旧 L1 shard → KeyGen regen → BloomFilter::new → save → 删 L1 → 更新 cursor
    - purge_l2_to_l3: 遍历旧 L2 archive → 删文件 → push PurgedEpoch 到 cursor
    - disk_usage: 按前缀统计 L1/L2 磁盘占用
    - +3 测试 (downgrade creates bloom / purge records epoch / disk_usage)
  - scanner.rs: 完整重写 (~200 行)
    - distribute: 切分 [start,end) → task_{id:05}.json (master_seed_hex + range + fetch_addrs_path)
    - worker: load task → decode seed → load fetch addrs → HashSet → rayon 分 chunk 扫描 → 断点续算 state_{id}.json → result_{id}.json
    - collect: 读 result_*.json → merge hits → sort by counter → write scan_hits.txt
    - load_fetch_addrs_for_scan: 兼容 new_addrs.bin (8-byte header) 和 all_addrs.bin (flat)
    - +4 测试 (distribute files / worker planted hit / worker resume / collect merge)
  - main.rs: +cmd_purge (--keep-last + --archive 切换两种降级) / +cmd_scan_distribute/worker/collect
  - `cargo test` 45/45 ✅
- Findings:
  - L1→L2 降级: 每 shard 需 regen 全部地址建 Bloom (~29s for 1B), 净省 41% 磁盘 (965→572 MB)
  - L2→L3 淘汰: 零成本删除, 仅 cursor 追加 PurgedEpoch 记录
  - Worker 完全无状态: 只需 birdhash 二进制 + task.json + fetch addrs 文件, 适合分发到任意机器
  - Worker 断点续算: state_{id}.json 记录 last_processed, 重启从断点继续

### Phase 8: CLI 集成 + 优化 & 交付
- **Status:** complete
- Actions:
  - config.rs: 重写, +TomlConfig/TomlGeneral/TomlGenerator/TomlDisk 反序列化结构, AppConfig::load() 支持 partial override
  - Cargo.toml: +toml 0.8 依赖
  - main.rs: 重写, +全局 --config 参数, 所有 cmd_* 接收 AppConfig 而非 self-construct
  - cargo clippy --fix + 手动修复 (saturating_sub / field_reassign_with_default / needless_borrow 等) → 0 warnings
  - cargo fmt: 全项目统一格式化
  - 修复 fmt 后测试模块缺少 `use crate::keygen` 的编译错误
  - README.md: 全面更新 CLI 用法 / 配置说明 / 磁盘生命周期 / 分布式回查 / 目录结构 / 测试
  - +2 config 测试 (load_missing_file / load_partial_toml)
  - `cargo test` 47/47 ✅, `cargo clippy` 0 warnings

### Phase 8.5: Fetcher 历史全量获取 + 归档
- **Status:** complete
- Actions:
  - archive.rs: 全新模块, BHFA 二进制分段归档 (ArchiveWriter + read/list API + 4 测试)
  - config.rs: +rpc_url/rpc_batch_size/rpc_retry_count/rpc_retry_base_ms/archive_segment + TomlFetcher
  - config.toml: 完整 [fetcher] 配置节
  - fetcher.rs: RPC 指数退避重试 + BHFA 归档集成 + miner/creates 地址提取 + genesis 起始
  - main.rs: +mod archive, Fetch --rpc 改 Option (CLI > config.toml)
  - `cargo test` **52/52** ✅, `cargo clippy` **0 warnings**

## 5-Question Reboot Check
| Question | Answer |
|----------|--------|
| Where am I? | **Phase 8.5 complete — Fetcher 全量获取+归档** (52 测试, 0 clippy warnings) |
| Where am I going? | Phase 9 GPU 加速 (可选, not_planned) |
| What's the goal? | 三级存储 + 确定性生成 + 三组件碰撞 + 分布式回查 + **全量归档** ✅ |
| What have I learned? | BHFA 分段归档紧凑高效; RPC 重试退避对长时间同步至关重要 |
| What have I done? | **P1-P8.5 完成**: +Fetcher 历史全量获取 + BHFA 分段归档 + RPC 增强 |
