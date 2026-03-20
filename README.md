# astock

`astock` 是一个独立于 `aks-mcp` 的 A 股研究与选股项目。

它不采集原始行情，不维护市场数据底座，也不修改 `aks-mcp`。它只负责：

- 管理选股逻辑候选池
- 用短线因子口径验证逻辑
- 生成当日候选股
- 回放历史交易日选股结果
- 基于历史因子面板做可解释的数据反推策略发现

## 项目目标

- 建立可持续扩展的选股逻辑池，而不是依赖临场拍脑袋选股
- 用历史数据验证逻辑在不同市场状态下的短线爆发率、风险收益比和稳定性
- 沉淀一套“市场状态 -> 可靠逻辑 -> 当日候选股”的标准化流程
- 输出可追溯、可复盘、可迭代的每日选股结果

## 核心流程

```text
市场数据 -> 特征面板 -> 逻辑验证 / 策略回放 / 数据反推 -> 可靠性快照 -> 当日选股
```

当前项目包含 3 条主线：

- `validation`：验证既有策略
- `selection`：按市场状态生成候选股、回放历史交易日
- `factor_lab`：从历史窗口反推出可解释候选策略

## 市场状态与策略

当前固定使用 `4` 个市场状态：

- `trend`：主线明确、强势延续
- `rotation`：市场轮动、结构性机会较多
- `weak_rotation`：轮动偏弱，更适合分歧低吸
- `panic`：市场极弱，只保留克制型反转低吸

策略数量：

- 内置 `11` 个 `manual` 策略
- 当前本地运行时额外加载 `4` 个已提升的 `factor_lab` 自动反推策略
- 当前运行时总数 `15`

状态启用：

- `trend`：`trend_pullback`、`leader_first_pullback`、`limit_up_repair`
- `rotation`：`leader_first_pullback`、`rotation_catchup`、`rotation_base_breakout`、`ma10_reclaim`、`oversold_rebound`、`limit_up_repair`，以及自动反推 `rotation` 策略
- `weak_rotation`：`rotation_catchup`、`rotation_base_breakout`、`ma10_reclaim`、`weak_rotation_dip_absorb`、`weak_rotation_failed_break_reclaim`、`fund_flow_reversal`、`weak_rotation_flat_reclaim`、`oversold_rebound`，以及自动反推 `weak_rotation` 策略
- `panic`：`fund_flow_reversal`

当前分层：

- 主力：`rotation_base_breakout`、`rotation_catchup`、`weak_rotation_dip_absorb`、`trend_pullback`
- 观察：`fund_flow_reversal`、`weak_rotation_flat_reclaim`、`ma10_reclaim`、`weak_rotation_failed_break_reclaim`、`limit_up_repair`
- 次观察：`leader_first_pullback`、`oversold_rebound`
- 自动反推：当前已提升 `4` 个策略，最新结果里 `weak_rotation` 方向更强

## 当前实现说明

- 当前验证体系是“短线低吸因子验证版”，重点看未来 `1-3` 天的命中率、爆发率、最大拉升和回撤
- 历史市场状态优先使用 `market_fund_flow`，缺失时使用特征面板做后备推断
- `discover-logics` 只负责发现候选策略
- `promote-discovered-logics` 才会把候选策略提升到运行时逻辑池
- `show-selection` 当前会展示：
  - `logic_name`
  - `holding_days`
  - `reliability_score`
  - `invalidation_level`

## 快速开始

开始前需要：

- Python `3.12+`
- 已部署且可访问的 `aks-mcp`

### 1. 安装并激活环境

```bash
cd /home/m/astock
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2. 检查数据源并初始化本地库

```bash
astock ping-source
astock init-db
```

### 3. 跑一次历史验证

```bash
astock validate-logics --start-date 2025-11-01 --end-date 2026-03-12 --symbol-limit 120 --chunk-size 20
```

### 4. 生成当日候选股

示例日期请替换为最近交易日。

```bash
astock run-selection --trade-date 2026-03-19 --symbol-limit 120 --chunk-size 20 --selection-limit 10
```

## 常用命令

```bash
astock info
astock list-logics
astock show-market --top-n 3
astock show-regime --trade-date 2026-03-19
astock show-validation --limit 15
astock show-snapshot --approved-only
astock show-selection --trade-date 2026-03-19
astock replay-selection 2026-03-16 --symbol-limit 120 --chunk-size 20 --selection-limit 10
astock discover-logics 2025-11-01 2026-03-12 --symbol-limit 120 --chunk-size 20 --candidate-limit 5
astock show-discovered-logics --approved-only --limit 10
astock promote-discovered-logics --limit 5
```

## 文档导航

- 架构文档：[`docs/architecture.md`](./docs/architecture.md)
  - 用于说明系统边界、架构分层、规范约束和后续开发规则
