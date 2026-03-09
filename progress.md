# Progress Log — 碰撞器执行计划

## Session: 2026-03-02

### Phase 1: 需求与约束确认
- **Status:** in_progress
- **Started:** 2026-03-02
- Actions taken:
  - 阅读 planning-with-files SKILL 与模板，创建 task_plan.md / findings.md / progress.md
  - 阅读 birdhash/SPEC.md 前 300 行，提取 Generator/Fetcher/Collider 与滤波器相关描述
  - 编写碰撞器目标：双线程（生成器线程 + 碰撞器线程），生成器采用助记词序列 + 派生路径 → 私钥/地址，地址存布谷鸟过滤器（持续更新）
  - 在 findings.md 中记录与 SPEC 的对照（采用/不采用）
- 补充：派生地址生成规则占位（逻辑构建时再定）；助记词可回溯、确定性、按 ID 递进；准备助记词表。
- 助记词表：已创建 data/wordlist/bip39_en.txt（BIP39 英文 2048 词，来自 Bitcoin BIPs 仓库）。
- Files created/modified:
  - birdhash/task_plan.md（新建，后增派生规则占位与助记词 ID 递进、词表）
  - birdhash/findings.md（新建，后增词表与派生规则占位）
  - birdhash/progress.md（本文件）
  - birdhash/data/wordlist/bip39_en.txt（新建，BIP39 英文词表）

### 计划调整（生成器输出与数据存储）
- **Status:** complete
- Actions taken: 生成器进程打印约定写入计划；数据存储两个待解决问题写入 task_plan 与 findings。
- 生成器进程打印：不滚屏，单行刷新；显示当前 ID、前 3 个助记词 + "..."、account id、index id。
- 待解决问题：(1) 链上地址碰撞结果 → 布谷鸟过滤器 的存储；(2) 生成地址碰撞 BinaryFuse 过滤器 的读写与配合。
- Files created/modified: task_plan.md（Phase 2 增「生成器进程打印」、新增「待解决问题」）；findings.md（需求与 Technical Decisions 增打印约定、新增「待解决问题」）；progress.md（本段）。

### 布谷鸟过滤器设计
- **Status:** complete
- Actions taken: 将布谷鸟设计写入 task_plan Phase 2 与 findings。
- 要点：地址入过滤器、私钥仅存 ID 可重算；命名=ID 范围；0.001% 误判 f≥20、约 2.6 bytes/地址、可配置分片容量与 max_memory；接近上限时 FIFO 释放最早分片。
- Files created/modified: task_plan.md（「布谷鸟过滤器设计」小节+Decisions）；findings.md（Research+Technical Decisions+「布谷鸟过滤器设计」节）；progress.md（本段）。

### 布谷鸟落盘、淘汰与检查点
- **Status:** complete
- Actions taken: 更新执行计划与 findings：落盘条件（分片满+ID 范围算完）；内存达限释放内存并删已落盘文件；磁盘达限删最早落盘文件；启动加载所有已落盘可读过滤器并续跑；检查点=落盘文件及文件名，重启后由文件名推断下一 ID。
- Files created/modified: task_plan.md（布谷鸟设计小节+Decisions）；findings.md（布谷鸟设计+Technical Decisions）；progress.md（本段）。

### Phase 2–5
- **Status:** pending
- Actions taken: —
- Files created/modified: —

## Test Results

| Test | Input | Expected | Actual | Status |
|------|-------|----------|--------|--------|
| （暂无） |       |          |        |        |

## Error Log

| Timestamp | Error | Attempt | Resolution |
|-----------|-------|---------|------------|
| （暂无）  |       |         |            |

## 5-Question Reboot Check

| Question | Answer |
|----------|--------|
| Where am I? | Phase 1（需求与约束确认） |
| Where am I going? | Phase 2 生成器设计（助记词+派生路径+布谷鸟）→ Phase 3 双线程协作 → Phase 4 实现 → Phase 5 文档 |
| What's the goal? | 碰撞器双线程；生成器：助记词序列+派生路径→私钥/地址，布谷鸟过滤器持续更新；生成器单行打印 ID/前3词/account/index；两处数据存储待解决 |
| What have I learned? | 见 findings.md（SPEC 对照、布谷鸟选型、助记词/派生路径、待解决问题） |
| What have I done? | 计划调整：生成器进程输出规范 + 链上碰撞→布谷鸟、生成地址碰撞→BinaryFuse 两待解决问题写入 task_plan/findings/progress |
