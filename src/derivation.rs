//! 派生路径常量。account 仅取前 11 个（0-10）；index 由外部 derivation_candidates 文件提供。

/// account 上界（含）：0..=ACCOUNT_MAX 共 11 个 account
pub const ACCOUNT_MAX: u32 = 10;
