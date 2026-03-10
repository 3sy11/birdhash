# Findings & Decisions — 碰撞器 / 生成器

## Requirements（来自用户）

- **架构**：**获取器、生成器、碰撞器** 均为**独立进程**；**获取器内存 BinaryFuse 通过共享内存提供给生成器与碰撞器**；放弃原三级存储（L1/L2/L3）。
- **生成器**（独立进程，**仅助记词模式**，移除原 keygen）：
  - 使用**助记词序列** + **派生路径规则**生成私钥与对应地址；**purpose / coin_type / change 可配置，使用默认值**。
  - **(种子 + ID)** 的生成方式得到**乱序的助记词**（确定性但非顺序枚举），确保每台机起点不同且打散。
  - 派生路径 m / purpose' / coin_type' / account' / change / index；account 0-111，index 从 assets/derivation_candidates 读取（由 init 集成生成）。
  - 生成的地址放入**布谷鸟过滤器**；新地址追加写 data/generator/out_addrs.bin 供碰撞器读。
- **碰撞器**：与**共享内存**通信（读 BinaryFuse）；与**获取器**通信获得**更新进来的新地址**（data/fetcher/new_addrs.bin）；**新链上地址与布谷鸟碰撞**；**生成器新地址与 BinaryFuse 碰撞**；命中写 hits。
- **init**：**生成 index 候选**与**生成种子**均集成到 `birdhash init` 中。
- **生成器进程打印**：不滚屏，单行刷新；当前 ID、前 3 助记词+"..."、account id、index id。

## 与 SPEC.md 的对照

| SPEC 内容 | 是否采用 | 说明 |
|-----------|----------|------|
| Generator 用 seed+counter → BinaryFuse8 | 否 | 改为助记词+派生路径，过滤器改为布谷鸟 |
| 三级存储 L1/L2/L3 | **否** | 放弃原方案 |
| Fetcher BinaryFuse16 | 是 | 获取器写入**共享内存**，生成器与碰撞器只读 |
| 双向碰撞 gen→fetch / fetch→gen | 是 | 碰撞器：新链上地址 vs 布谷鸟；生成器新地址 vs BinaryFuse |
| keygen 模块 HMAC-SHA256 + counter | **否** | 生成器仅助记词模式，移除原 keygen |

## Research Findings

- **布谷鸟过滤器**：支持插入与删除，适合「持续更新」；误判率 ε ≤ b/2^(f−1)，空间约 (log₂(1/ε)+1+log₂b)/α bits/元素（α≈0.955）。0.001% 误判需 f≥20 bits，约 2.6 bytes/地址。Rust 有 `cuckoofilter` 等 crate，地址 20 字节可哈希取指纹。
- **BIP39/BIP32/BIP44**：助记词 → seed → 分层确定性密钥；以太坊常用 m/44'/60'/0'/0/account_index。
- **助记词序列**：**(种子 + ID)** 得到**乱序助记词**（确定性、可回溯，但非顺序枚举）；同一 (种子,ID) 得到同一序列。
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
| 放弃三级存储 | 不再采用 L1/L2/L3 |
| 生成器仅助记词模式 | 移除原 keygen（HMAC-SHA256+counter） |
| 种子+ID → 乱序助记词 | 确定性但乱序，非顺序枚举 |
| purpose/coin_type/change | 可配置，使用默认值 |
| 通信：共享内存 BinaryFuse | 获取器写，生成器与碰撞器只读 |
| 通信：文件 new_addrs / out_addrs | 获取器→碰撞器新链上地址；生成器→碰撞器新地址；碰撞器增量读 |
| 通信：布谷鸟 | 生成器写磁盘，碰撞器从磁盘加载 |
| init 集成 | 生成 index 候选 + 生成种子 均集成到 birdhash init |
| index 候选生成 | 由 init 集成，输出 assets/derivation_candidates/derivation_candidates.txt |
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

## 三组件通信方案（具体）

- **共享内存**：BinaryFuse 由获取器写入共享内存（如 POSIX shm / mmap），生成器与碰撞器只读挂载；更新时需双缓冲或版本/锁避免读端看到中间状态。
- **获取器 → 碰撞器（新链上地址）**：获取器追加写 `data/fetcher/new_addrs.bin`（20 字节/地址），碰撞器按偏移或长度增量读取，与布谷鸟（从磁盘加载）碰撞。
- **生成器 → 碰撞器（新生成地址）**：生成器追加写 `data/generator/out_addrs.bin`，碰撞器增量读取，与 BinaryFuse（共享内存）碰撞。
- **布谷鸟**：生成器写 cuckoo_*.bin 到磁盘；碰撞器从磁盘加载布谷鸟分片做 fetch→gen 查询。

## 三组件可执行命令（CLI）

- `birdhash init`：创建目录 + **集成**生成 index 候选 + 生成/检查种子（待调整）。
- **获取器**：`birdhash fetch`、`birdhash build-filter`（BinaryFuse 写入共享内存，待接）。
- **生成器**：`birdhash generate`（只读共享内存 BinaryFuse，种子+ID→乱序助记词，写 out_addrs.bin）。
- **碰撞器**：`birdhash collide`（读共享内存、new_addrs、out_addrs，从磁盘加载布谷鸟，写 hits）。

## 待解决问题（若有）

| 问题 | 描述 |
|------|------|
| BinaryFuse 更新一致性 | 获取器更新共享内存时读端一致性；双缓冲或版本/锁 |

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
