# Task Plan: birdhash — 获取器职责分离与重构

## Goal
将**获取器（Fetcher）**职责限定为**仅拉取并持久化原始块数据**；接收传入的起始块 ID、结束块 ID 与 RPC URL，将结果写入以「起始ID-结束ID」命名的文件夹（内含原始块一行一块的文本文件 + 区间内检查点文件，便于断点续跑）。**碰撞用中间/临时数据**（如 all_addrs、filter_fetch、new_addrs 等）的生成逻辑移至**碰撞器（Collider）**，在后续计划中实现。

## Current Phase
Phase 10 已完成；Phase 11 待后续实施

## Phases

### Phase 10: 获取器重构 — 仅原始数据 + 区间 + 检查点
- [x] **10.1 接口与 CLI**：fetch 命令接收 `--start-block N`、`--end-block M`、`--rpc <URL>`（必填或 config）；范围由本次调用参数唯一确定。
- [x] **10.2 输出目录与文件**：输出根目录默认 `data/fetcher/ranges`，可 `--output-dir` 覆盖；写入子目录 `{start_block}-{end_block}/`，内含 `blocks.jsonl`（一行一块 JSON）与 `checkpoint.json`。
- [x] **10.3 获取器逻辑精简**：移除 Fetcher 大循环、archive、filter 重建、all_addrs/new_addrs 写入；仅 run_fetch_range：RPC 拉块 → 一行 JSON 追加 blocks.jsonl → 定期写检查点。
- [x] **10.4 断点续跑**：检查点存在且 status != done 时从 last_fetched_block+1 继续，追加同一 blocks.jsonl，完成后 status=done。
- [x] **10.5 配置**：config 新增 fetcher_ranges_dir()；CLI --rpc 覆盖 config。
- **Status:** complete

### Phase 11（后续计划）: 碰撞器消费原始数据并生成中间数据
- [ ] 碰撞器（或独立「导入」步骤）读取获取器产出的「一行一块」文件（及检查点确认 done），解析块内地址，生成碰撞所需的中间数据：如 all_addrs、BinaryFuse16 filter、new_addrs 等；写入位置与格式由碰撞器/流水线设计决定。
- [ ] 与现有 Collider 集成：Collider 可依赖「由原始数据生成的」filter_fetch / 地址列表，而不依赖获取器直接写这些文件。
- **Status:** not_started（后续计划，不在此次实施）

## 获取器新设计摘要（Phase 10）

| 项目 | 内容 |
|------|------|
| **输入** | `start_block`、`end_block`、`rpc_url`（必显式传入或 config 默认） |
| **输出目录** | `{output_root}/{start_block}-{end_block}/` |
| **文件 1** | 块的原始信息：一行一块的文本文件（如 JSON 或 JSONL），按块号顺序追加 |
| **文件 2** | 检查点：如 `checkpoint.json`，含 start/end、last_fetched_block、status、时间戳 |
| **职责** | 仅拉取并持久化原始块；不做地址去重、不建 filter、不写 all_addrs/new_addrs |
| **断点续跑** | 读同目录检查点，从 last_fetched_block+1 继续，追加块文件并更新检查点 |

## Key Questions
1. 原始块「一行一块」的格式：纯 JSON 一行（便于 JSONL 解析）还是自定义二进制？→ 建议 JSONL 以便碰撞器/脚本通用解析。
2. 输出根目录默认值：`data/fetcher/ranges/` 还是可配置？→ 建议 config `fetcher_output_dir` 或 `data_dir/fetcher/ranges`。
3. 多任务并行：多进程分别跑不同 start-end 时，各写各自目录，互不干扰；无需共享 cursor。

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| 获取器只产出原始块 + 检查点 | 职责单一，碰撞与索引由下游完成 |
| 目录名 `{start}-{end}` | 区间明确，多任务可并行写不同目录 |
| 一行一块文本（JSONL） | 易解析、易断点续写、易被其他工具消费 |
| 检查点单独文件 | 与块数据分离，更新检查点不影响大文件 |
| 碰撞中间数据移至碰撞器 | 后续计划中实现，本计划不实现 |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| （待实施后按需填写） |         |            |

## Notes
- 现有 Fetcher 中与 filter、AddressStore、archive（BHFA）、new_addrs 相关的逻辑均在 Phase 10 中从获取器移除或改为「仅写原始块」。
- Phase 11 为后续计划，仅在此记录目标：碰撞器（或独立流水线）读原始块 → 生成 filter_fetch / all_addrs / new_addrs 等供 Collider 使用。
- 更新 phase 状态：pending → in_progress → complete；重大决策前重读本计划。
