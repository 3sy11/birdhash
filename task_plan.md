# Task Plan: birdhash — 以太坊私钥碰撞系统

## Goal
用 Rust 构建**三级存储架构**的以太坊地址碰撞系统。Generator 确定性生成私钥+地址（seed+counter），存入三级滤波器（L1 活跃 / L2 归档 / L3 过期+分布式回查）；Fetcher 获取链上地址（纯内存）；Collider 双向碰撞 + 断点续碰。可选 GPU 加速 L3 回查。

## Current Phase
Phase 8.5 (Fetcher 历史全量获取 + 归档) → complete

## Phases

### Phase 1: 需求与技术选型 + 架构设计
- [x] 密码学库调研 (secp256k1 C binding ~3M keys/sec/thread)
- [x] 三组件架构 (Generator / Fetcher / Collider)
- [x] 确定性生成 (seed+counter)，无需存私钥文件
- [x] 滤波器选型：Gen 侧 BinaryFuse8 (8.05 bits/addr, 0.39% FP)
- [x] 分片 10 亿/片 (~965 MB), 恢复 ~29 秒
- [x] 存储膨胀分析 → BinaryFuse8 ~2.4 TB/天 (比初版 bloom 省 3.6x)
- [x] **三级存储架构 (L1 活跃 / L2 归档 / L3 分布式回查)**
- [x] Fetcher 纯内存设计 (BinaryFuse16 ~705 MB, 每天新增 ~10 MB)
- [x] 分布式回查 (scan-distribute / scan-worker / scan-collect)
- [x] 磁盘预算分析 (4 TB 磁盘: L1+L2 ~5 万亿有索引覆盖)
- **Status:** complete

### Phase 2: 项目脚手架
- [x] `cargo init` + Cargo.toml (secp256k1, tiny-keccak, hmac, sha2, xorf, rayon, clap, serde, bitvec, chrono, hex, anyhow, ctrlc, env_logger)
- [x] 11 个模块骨架 (main/keygen/filter/cursor/config/generator/fetcher/collider/scanner/lifecycle/stats)
- [x] keygen: 完整 seed+counter → privkey → addr 实现 + 3 个单元测试
- [x] filter: BinaryFuse8/16 build/save/load + Bloom 10% (bitvec) + 3 个单元测试
- [x] cursor: 全部 cursor struct 定义 (Generator/Fetcher/Collider/ScanTask/ScanWorkerState) + 通用 load/save
- [x] config: AppConfig 全部路径方法 + ensure_dirs
- [x] stats: gather_status + print_status
- [x] CLI: clap derive, 9 个子命令 (init/generate/fetch/collide/status/purge/scan-distribute/scan-worker/scan-collect)
- [x] config.toml + data 目录 + .gitignore + README.md
- [x] `cargo check` ✅ `cargo test` ✅ (6/6 passed)
- **Status:** complete

### Phase 3: 核心基础层
- [x] keygen: rayon 并行 (par_batch_addresses/par_batch_fingerprints) + derive_keypair + privkey_hex_to_addr_hex
- [x] keygen: 已知向量测试 (privkey=1 → 7e5f4552...) + 推导链一致性验证
- [x] filter: serde_json → **bincode** 二进制序列化 (10 亿条从 ~4 GB JSON 降到 ~965 MB)
- [x] filter: addr_to_u64 哈希优化 (SipHash-style 混合全部 20 字节)
- [x] filter: atomic_write (write .tmp → rename 崩溃安全)
- [x] filter: FP 率验证测试 (BinaryFuse8 < 1%, Bloom 5%~15%)
- [x] cursor: atomic save (write .json.tmp → rename) + PartialEq derive
- [x] cursor: 5 个 round-trip 测试 (Generator/Fetcher/Collider/ScanTask/load_or_default)
- [x] main.rs: `init` 命令实现 (ensure_dirs + load_or_create_seed + gen_cursor 初始化)
- [x] main.rs: `status` 命令实现 (gather_status + print_status)
- [x] 添加 bincode 依赖
- [x] `cargo test` ✅ **23/23 passed**
- **Status:** complete

### Phase 4: Generator
- [x] Generator 主循环: rayon par_batch_fingerprints → 内存累积 → shard_size 满时 build_fuse8 → save L1
- [x] 实时碰 fetch filter (gen_vs_fetch: load BinaryFuse16 + 逐条 contains 检查)
- [x] generator_cursor.json 持久化 (counter + shard_count + current_shard_entries + purged_epochs)
- [x] 断点续跑: resume_partial_shard (重新生成部分 shard fingerprints)
- [x] Ctrl+C 优雅退出 (ctrlc::set_handler + AtomicBool, 二次 Ctrl+C 强制退出)
- [x] 实时统计面板: \r 覆写 (elapsed/total/rate/shard%/L1_files/gvf_candidates)
- [x] main.rs cmd_generate: seed hash 校验 + threads 配置 + load_fetch_filter
- [x] 3 个测试: shard 刷盘验证 / partial shard resume / 完整运行 tiny
- [x] `cargo test` ✅ **26/26 passed**
- **Status:** complete

### Phase 5: Fetcher
- [x] EthRpc: ureq JSON-RPC 客户端 (eth_blockNumber + 批量 eth_getBlockByNumber)
- [x] AddressStore: 追加式平文件 all_addrs.bin ([u8;20] × N, 持久化全量地址)
- [x] new_addrs.bin 导入/导出 (8-byte count header + flat [u8;20])
- [x] Fetcher 主循环: 批量取块 → extract from/to → 近似 dedup (filter) → append store → cursor
- [x] filter rebuild: 读 all_addrs → sort+dedup fingerprints → build_fuse16 → save + 导出 new_addrs
- [x] 链高度追赶: 定期刷新 end_block 跟随链增长
- [x] Ctrl+C 优雅退出 + 定期 save_state (cursor + new_addrs)
- [x] main.rs cmd_fetch 接通 (load cursor → Fetcher::new → run)
- [x] 7 个测试: store append/read / new_addrs round-trip / parse_hex_addr valid+no_prefix+invalid / block JSON 解析 / filter rebuild from store
- [x] `cargo test` ✅ **33/33 passed**
- **Status:** complete

### Phase 6: Collider + 三级碰撞
- [x] L1 碰撞: load BinaryFuse8 → scan fetch addrs → 收集 HashSet<Address> candidates → rayon 重生成 shard → 精确匹配
- [x] L2 碰撞: load Bloom 10% → 同流程 (更多 candidates, 仍一次 regen/shard)
- [x] regenerate_range: 本地 L3 纯重生成碰撞 (无 filter, 分 chunk 扫描, 支持 Ctrl+C)
- [x] hits.txt 命中记录: timestamp | counter | privkey | addr, append 模式
- [x] collider_cursor.json 断点续碰 (l1_shard_version / l2_last_checked / hits)
- [x] Ctrl+C 优雅退出 (完成当前 shard 后停止)
- [x] main.rs cmd_collide: 支持 `--regenerate-range START END` 切换 L3 模式
- [x] fetcher AddressStore: +read_all_addresses() 供 Collider 加载全量地址
- [x] 5 个测试: L1 planted hit / L1 no hits / L2 bloom planted hit / regen planted hit / hit format
- [x] `cargo test` ✅ **38/38 passed**
- **Status:** complete

### Phase 7: 磁盘生命周期 + 分布式回查
- [x] lifecycle: downgrade_l1_to_l2 (regen shard → Bloom 10% → 删 L1, 净省 41%)
- [x] lifecycle: purge_l2_to_l3 (删 L2 archive → 记录 purged_epoch)
- [x] lifecycle: disk_usage 统计 L1/L2 磁盘占用
- [x] scanner: distribute (切分 counter range → task_{id}.json)
- [x] scanner: worker (加载 task → regen range → check fetch set → 断点续算 state)
- [x] scanner: collect (合并 result_*.json → sort → scan_hits.txt)
- [x] main.rs: cmd_purge (--keep-last + --archive 切换 L1→L2 / L2→L3)
- [x] main.rs: cmd_scan_distribute / cmd_scan_worker / cmd_scan_collect
- [x] 7 个测试: L1→L2 downgrade / L2→L3 purge+epoch / disk_usage / distribute / worker hit / worker resume / collect merge
- [x] `cargo test` ✅ **45/45 passed**
- **Status:** complete

### Phase 8: CLI 集成 + 优化 & 交付
- [x] config.toml 解析: toml crate + TomlConfig struct + AppConfig::load() (partial override)
- [x] CLI 全局 --config 参数, 所有 cmd_* 统一接收 AppConfig
- [x] cargo clippy: 自动修复 + 手动修复 → **0 warnings**
- [x] cargo fmt: 全项目格式化
- [x] README.md: 完整 CLI 用法 + 配置 + 生命周期 + 分布式回查 + 目录结构 + 测试说明
- [x] config.rs +2 测试 (load_missing / load_partial_toml)
- [x] `cargo test` ✅ **47/47 passed**
- **Status:** complete

### Phase 8.5: Fetcher 历史全量获取 + 归档
- [x] archive.rs: 分段归档模块 (BHFA 格式, 10万块/文件, 读写 + append + resume)
- [x] config.rs: 新增 fetcher 配置 (rpc_url / batch_size / retry_count / retry_base_ms / archive_segment)
- [x] fetcher.rs: RPC 重试 + 指数退避 (call_with_retry, 可配次数+基数)
- [x] fetcher.rs: 集成 archive 写入 (每块地址写 BHFA 归档 + all_addrs.bin)
- [x] fetcher.rs: 提取 miner/validator 地址 + 合约创建地址 (tx.creates)
- [x] main.rs: fetch 命令 RPC URL 从 config 读取, --rpc CLI 覆盖, genesis 起始
- [x] config.toml: 添加 [fetcher] 完整配置项 (rpc_url/batch_size/retry/archive_segment)
- [x] 测试: 4 个 archive 测试 + 9 个 fetcher 测试 = 52/52 ✅, clippy 0 warnings
- **Status:** complete

### Phase 9: GPU 加速（可选）
- [ ] GPU 加速 L3 回查 (wgpu compute shader: HMAC→secp256k1→keccak256→HashSet check)
- [ ] GPU 生成器模式 (受磁盘限制, 收益有限)
- **Status:** not_planned

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| **三级存储 L1/L2/L3** | L1 快速碰撞 + L2 省盘归档 + L3 零磁盘分布式回查 |
| Gen: BinaryFuse8 | 8.05 bits/addr, 0.39% FP, 查询 3 次访问 |
| Fetch: BinaryFuse16 纯内存 | ~705 MB 全量, 每天新增 ~10 MB, 无压力 |
| 确定性生成 seed+counter | 可重算私钥, 无需存文件 |
| 分片 10 亿条 | ~965 MB/片, 恢复 ~29 秒 |
| L2 归档 Bloom 10% | 比 L1 省 41% 磁盘, O(1) 查询 |
| L3 分布式回查 | scan-distribute/worker/collect, 零散 CPU 可利用 |
| 断点续碰 + 续算 | 所有组件均可中断恢复 |

## Notes
- **1 TB 磁盘 = 1.09 万亿地址 L1 覆盖**
- **4 TB 磁盘: L1+L2 ≈ 5 万亿有索引, L3 无限(分布式回查)**
- 写入 ~29 MB/sec, HDD 轻松承载
- Fetcher: 以太坊每天新增 ~25 万地址, 纯内存毫无压力
- 5 台零散笔记本回查算力 ≈ 1 块 GPU
- master_seed.key 必须安全备份
