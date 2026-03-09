# Findings & Decisions — 碰撞器 / 生成器

## Requirements（来自用户）

- **架构**：**获取器、生成器、碰撞器** 均为**独立进程**（各自 CLI）；可选由**主进程**统一启动三者（子进程或线程），实现数据通信。
- **生成器**（独立进程，先设计）：
  - 使用**助记词序列** + **派生路径规则**生成私钥与对应地址。
  - **派生地址的生成规则**：路径 m / purpose' / coin_type' / account' / change / index。account 与 index 共用候选：在「正数1万、倒数1万」范围内，保留**从 0-9 中任取 1～3 个数（作为数码）组成的数**；脚本生成 derivation_candidates.txt，生成时直接使用。
  - 助记词生成需**可回溯**、**确定性**，按 **ID 递进** 生成新的助记词顺序（ID 递增 → 确定性的下一个序列）。
  - 需**准备助记词表**（已做：BIP39 英文 2048 词，见下）。
  - 需**额外配置生成器种子 key 文件**，与 **ID** 共同生成 (privkey, address)，确保**每台机器启动时起点不同**；路径可配置（如 `generator_seed.key`）。
  - 生成的地址放入**布谷鸟过滤器**（Cuckoo filter），因为需要**持续更新**过滤器中的地址。
- **生成器进程打印**：不滚屏，单行刷新；打印当前计算的 ID、前 3 个助记词 + "..."、当前 account id、当前 index id，一行不滚，便于确认进程在运行。
- 参考 birdhash/SPEC.md，但**不会采用全部**（例如不沿用原 SPEC 的 L1 BinaryFuse8 + L2 Bloom 分级，改为布谷鸟以支持增删）。
- 先写计划与设计，再往里添加实现细节。

## 与 SPEC.md 的对照

| SPEC 内容 | 是否采用 | 说明 |
|-----------|----------|------|
| Generator 用 seed+counter → BinaryFuse8 | 否 | 改为助记词+派生路径，过滤器改为布谷鸟 |
| 三级存储 L1/L2/L3 | 待定 | 首阶段可能只做「生成器过滤器 + 碰撞」单层，后续再考虑分级 |
| Fetcher BinaryFuse16 | 是 | 获取器已实现，碰撞器读取现有 filter.*.bin |
| 双向碰撞 gen→fetch / fetch→gen | 是 | 目标不变，实现方式随生成器改动 |
| keygen 模块 HMAC-SHA256 + counter | 待定 | 若保留可与助记词路径并存（两套生成源） |

## Research Findings

- **布谷鸟过滤器**：支持插入与删除，适合「持续更新」；误判率 ε ≤ b/2^(f−1)，空间约 (log₂(1/ε)+1+log₂b)/α bits/元素（α≈0.955）。0.001% 误判需 f≥20 bits，约 2.6 bytes/地址。Rust 有 `cuckoofilter` 等 crate，地址 20 字节可哈希取指纹。
- **BIP39/BIP32/BIP44**：助记词 → seed → 分层确定性密钥；以太坊常用 m/44'/60'/0'/0/account_index。
- **助记词序列**：按 **ID** 确定性、可回溯地生成「助记词顺序」；ID 递进则得到下一个序列。同一 ID 永远得到同一序列，便于重放与断点。
- **助记词表**：已准备 `birdhash/assets/wordlist/bip39_en.txt`，BIP39 英文 2048 词，一行一词；生成时按该表枚举。
- **派生地址生成规则**：m / purpose' / coin_type' / account' / change / index；account 与 index 共用候选，范围为「正数1万」+「倒数1万」，筛选条件为从 0-9 中任取 1～3 个数（数码）组成的数；脚本输出 data/derivation_candidates/derivation_candidates.txt。

## Technical Decisions

| Decision | Rationale |
|----------|-----------|
| 生成器地址用布谷鸟过滤器 | 需持续增删；Xor/BinaryFuse 不可变，布谷鸟支持动态更新 |
| 三组件均为独立进程 | 获取器、生成器、碰撞器各自 CLI 启动；可选主进程启动三子进程或三线程并负责通信 |
| 先实现生成器再实现碰撞器 | 生成器是碰撞侧数据源，接口稳定后再做碰撞与主进程编排 |
| 计划文件放在 birdhash/ | 与 SPEC.md 同目录，碰撞器/生成器代码也在此仓库 |
| 助记词表 assets/wordlist/bip39_en.txt | BIP39 英文 2048 词，确定性枚举助记词序列的词汇来源 |
| account 取值范围 | 0-111（含 111） |
| index 取值范围 | 由命令 `birdhash gen-derivation-candidates` 生成的文件 derivation_candidates.txt 中所列之值（一行一个） |
| index 候选生成 | 项目内命令 `birdhash gen-derivation-candidates`（Rust），输出 assets/derivation_candidates/derivation_candidates.txt；不再依赖 Python 脚本 |
| 生成器进程输出 | 单行刷新不滚屏：当前 ID、前3助记词+省略号、account id、index id |
| 布谷鸟：只存地址+ID | 生成 (privkey,address) 时地址入过滤器，私钥仅保留 ID 便于命中后重算，不存私钥以最大化内存 |
| 布谷鸟：命名=ID 范围 | 分片文件命名即 ID 范围，如 cuckoo_00000000-00999999.bin；索引=该范围 |
| 布谷鸟：0.001% FPR | 指纹 f≥20 bits，约 2.6 bytes/地址；1000 万地址/分片≈26 MB；可配置 shard_capacity 与 cuckoo_max_memory_mb |
| 布谷鸟：落盘 | 分片满且该分片 ID 范围算完后落盘一次 |
| 布谷鸟：内存淘汰 | 内存达上限时释放最早分片内存并删除其已落盘文件（FIFO） |
| 布谷鸟：磁盘淘汰 | 磁盘达上限时删除最早落盘文件（FIFO），用内存保证可用性 |
| 布谷鸟：启动 | 启动时加载所有已落盘可读过滤器，从下一 ID 继续生成 |
| 布谷鸟：检查点 | 落盘过滤器+文件名管理；重启后读文件名得 ID 范围，下一 ID=max(end_id)+1 |
| 生成器种子 key | 单独种子文件（如 generator_seed.key），与 ID 共同参与键值对生成，每台机不同种子→起点不同 |

## 布谷鸟过滤器设计（生成器侧）

- **存什么**：只把 **address** 插入布谷鸟；**私钥不存**，只保留 **ID**（命中后按 ID 重算）。
- **分片命名**：文件名 = ID 范围，如 `cuckoo_00000000-00999999.bin`；索引即该 ID 范围。
- **误判率 0.001%**：ε ≤ b/2^(f−1) → f ≥ 20 bits；约 2.6 bytes/地址；1000 万地址 ≈ 26 MB/分片。可配置 `shard_capacity`、`cuckoo_max_memory_mb`、`cuckoo_max_disk_mb`。
- **落盘**：当**分片达到指定大小**且**该分片 ID 范围全部计算完成**后，**落盘一次**（写该分片文件）。
- **内存淘汰**：当**内存达上限**时，对最早分片（FIFO）：**释放内存**并**删除其已落盘文件**，该分片彻底移除。
- **磁盘淘汰**：当**磁盘达上限**时，**删除最早落盘的文件**（FIFO），用内存保证可用性。
- **启动**：生成器启动时**加载所有已落盘且可读的布谷鸟过滤器**，然后从**下一 ID** 继续生成。
- **检查点**：**不单独存检查点**；由**落盘过滤器 + 文件名**管理：重启后**读目录下 cuckoo_*.bin 文件名**，解析 ID 范围，**下一生成 ID = max(end_id) + 1**。

## 三组件可执行命令（CLI）

- **获取器（独立进程）**：`birdhash fetch`、`birdhash build-filter`（已实现）。
- **生成器（独立进程）**：`birdhash gen-derivation-candidates`（已实现）、`birdhash init-generator-seed`（待实现）、`birdhash generate`（待实现）。
- **碰撞器（独立进程）**：`birdhash collide`（待实现）。
- **主进程编排（可选）**：`birdhash run` 启动三者（三子进程或三线程），实现数据通信（待定）。

是否用主进程启动三组件：可以。方案 A = 主进程 spawn 三子进程，通过共享目录或 IPC 通信；方案 B = 主进程内三线程，通过 channel/共享结构通信。

## 待解决问题（数据存储）

| 问题 | 描述 |
|------|------|
| **链上地址碰撞 → 布谷鸟过滤器** | 用链上获取的地址与生成地址碰撞后，结果/中间数据需写入布谷鸟过滤器（支持持续更新）；存储格式、容量与持久化待定。 |
| **生成地址碰撞 → BinaryFuse 过滤器** | 用生成出来的地址去碰撞 BinaryFuse 过滤器（如 fetcher 的 filter）；查询与批量碰撞的读写方式、与生成器侧布谷鸟的配合待定。 |

## Issues Encountered

| Issue | Resolution |
|-------|------------|
| （暂无） |            |

## Resources

- birdhash/SPEC.md — 现有架构与数据格式参考
- birdhash/assets/wordlist/bip39_en.txt — BIP39 英文助记词表（2048 词）
- birdhash/assets/derivation_candidates/derivation_candidates.txt — index 候选（由 gen-derivation-candidates 生成）
- .cursor/skills/planning-with-files/SKILL.md — 计划与 findings/progress 的用法

## Visual/Browser Findings

- （暂无）
