# smoldot-pow 快照来源说明

## 1. 目录用途

本目录保存 `wuminapp` 当前使用的 PoW 轻节点内核固定快照。

约束：

- 目录内不保留 `.git`
- 不提交 `target/` 等编译产物
- Flutter / Rust 构建只依赖此目录，不再回指仓库根目录临时 `smoldot/`

## 2. 上游来源

- 上游项目仓库字段：`https://github.com/smol-dot/smoldot`
- 本次收编前本地来源：`file:///Users/rhett/.cargo/git/db/smoldot-df5fc45614b6d921`
- 收编基线提交：`f471baac1f0fa821569c42ebb14c4f8533ba77ad`

## 3. 收编时已存在的本地 PoW 改动

以下文件在收编前已经带有本地修改：

- `lib/src/chain/blocks_tree.rs`
- `lib/src/chain/blocks_tree/finality.rs`
- `lib/src/chain/blocks_tree/verify.rs`
- `lib/src/chain/chain_information.rs`
- `lib/src/chain/chain_information/build.rs`
- `lib/src/header.rs`
- `lib/src/sync/warp_sync.rs`
- `lib/src/verify.rs`
- `lib/src/verify/header_only.rs`
- `light-base/src/sync_service/standalone.rs`
- `lib/src/verify/pow.rs`（新增）

说明：

- 这些改动是当前 `PoW + GRANDPA` 轻节点实验基线的一部分
- 后续必须整理进独立 GitHub fork，再按显式同步流程回灌到本目录

## 4. 后续同步规则

1. 先在独立 GitHub fork 上完成修改与验证。
2. 再将 fork 的固定提交快照同步到本目录。
3. 每次同步都必须更新本文件中的基线提交与改动说明。
