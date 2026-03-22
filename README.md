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
- 自动反推出当前状态下相对有效的策略，而不是追求长期固定不变的自动策略

## 核心流程

```text
市场数据 -> 特征面板 -> 逻辑验证 / 策略回放 / 数据反推 -> 可靠性快照 -> 当日选股
```

当前项目包含 3 条主线：

- `validation`：验证既有策略
- `selection`：按市场状态生成候选股、回放历史交易日
- `factor_lab`：做因子分析、组合对照、规则宽窄实验、候选策略发现和自动研究循环
  - 当前 discovery 已支持 `rotation / weak_rotation` 子状态切分
  - 当前会对 `ranking_type` 做排序实验，再只保留最佳结果
  - 当前可按因子批次自动循环：验收因子、生成候选、分流到 `runtime / watch / retired`
  - 当前已接入首批固定候选组合蓝图，用于约束 discovery 优先围绕短线低吸模板生成组合

## 市场状态与策略

当前固定使用 `4` 个市场状态：

- `trend`：主线明确、强势延续
- `rotation`：市场轮动、结构性机会较多
- `weak_rotation`：轮动偏弱，更适合分歧低吸
- `panic`：市场极弱，只保留克制型反转低吸

策略来源：

- 内置 `11` 个 `manual` 策略
- 运行时可额外加载已提升的 `factor_lab` 自动反推策略
- 实际运行时总数以 `astock list-logics` 为准

状态启用：

- `trend`：`trend_pullback`、`leader_first_pullback`、`limit_up_repair`
- `rotation`：`leader_first_pullback`、`rotation_catchup`、`rotation_base_breakout`、`ma10_reclaim`、`oversold_rebound`、`limit_up_repair`，以及自动反推 `rotation` 策略
- `weak_rotation`：`rotation_catchup`、`rotation_base_breakout`、`ma10_reclaim`、`weak_rotation_dip_absorb`、`weak_rotation_failed_break_reclaim`、`fund_flow_reversal`、`weak_rotation_flat_reclaim`、`oversold_rebound`，以及自动反推 `weak_rotation` 策略
- `panic`：`fund_flow_reversal`

当前分层：

- 主力：`rotation_base_breakout`、`rotation_catchup`、`weak_rotation_dip_absorb`、`trend_pullback`
- 观察：`fund_flow_reversal`、`weak_rotation_flat_reclaim`、`ma10_reclaim`、`weak_rotation_failed_break_reclaim`、`limit_up_repair`
- 次观察：`leader_first_pullback`、`oversold_rebound`
- 自动反推：当前已有 `1` 条 `weak_rotation` 自动策略进入运行时并进入最新快照；自动策略只作为增量，不直接等同于实盘主力策略
  - 当前运行时自动策略：`auto_weak_rotation_repair_1_92fd9e5d`
  - 当前口径：长窗口通过 + 当前状态近端有效，才允许进入 `runtime`

## 当前实现说明

- 当前验证体系是“短线低吸因子验证版”，重点看未来 `1-3` 天的命中率、爆发率、最大拉升和回撤
- 历史市场状态优先使用 `market_fund_flow`，缺失时使用特征面板做后备推断
- `discover-logics` 会同时产出：
  - 因子白名单
  - 因子画像
  - 组合结果
  - 规则宽窄实验
  - 排序实验结果
  - `Top3 / Top5` 回放质量
  - 最终候选策略
- discovery 当前优先围绕首批固定候选组合蓝图生成组合，首批模板包括：
  - 强势回踩确认
  - 缩量平台回收
  - 板块强势中的个股分歧低吸
  - 未加速补涨启动
  - 超额实体修复
  - 波动收缩后弱转强
- `seed-factor-pool` 会初始化自动研究循环的因子候选池
- `auto-discovery-loop` 会按因子批次循环跑：
  - 因子验收
  - 组合生成
  - 规则压缩
  - replay 验收
  - `runtime / watch / retired` 分流
- 自动策略当前按“当前状态有效”分层：
  - `candidate`：刚发现，尚未证明可用
  - `watch`：长窗口或研究指标可用，但近端状态验证不足
  - `runtime`：长窗口通过，且近端 `Top5` replay 或近端 validation 通过
  - `retired`：失效或被更严格门槛清退
- `show-discovery-loop` 用于查看最近一次自动研究循环的每轮结果
- `rotation / weak_rotation` 在 discovery 中会进一步细分子状态，用于提升因子和组合的纯度
- 自动候选当前会记录：
  - `regime_detail`
  - `variant_type`
  - `ranking_type`
  - `lifecycle_state`
- `promote-discovered-logics` 当前只提升同时满足：
  - `approved_for_validation`
  - `replay_quality_passed`
  - 且 `recent replay` 或 `recent validation` 至少一条通过
  的候选
- `rolling-discovery-eval` 用于验证自动反推链是否能在滚动窗口中稳定产出可用候选
- 当前已验证跑出 `1` 条自动反推运行时策略：
  - `auto_weak_rotation_repair_1_92fd9e5d`
  - 来源主线：`weak_rotation_repair`
  - 核心组合：`pullback_from_5d_high_pct + excess_body_pct`
  - 当前依赖“近端 validation 复核”进入 `runtime`
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
astock analyze-factors --limit 10
astock analyze-factor-combos --limit 10
astock analyze-rule-variants --limit 10
astock show-replay-quality --limit 10
astock show-factor-whitelist --eligible-only --limit 10
astock show-discovered-logics --approved-only --limit 10
astock promote-discovered-logics --limit 5
astock seed-factor-pool
astock auto-discovery-loop 2025-11-01 2026-03-12 --regimes rotation,weak_rotation --symbol-limit 20 --chunk-size 10 --candidate-limit 2 --batch-size 4 --max-iterations 2 --target-runtime-candidates 1 --max-stagnation-iterations 2
astock show-discovery-loop --limit 10
astock rolling-discovery-eval 2025-11-01 2026-03-12 --regimes rotation,weak_rotation --train-days 30 --test-days 10 --follow-days 5 --step-days 15 --symbol-limit 20 --chunk-size 10 --candidate-limit 2
astock show-discovery-stability --limit 10
```

## 文档导航

- 架构文档：[`docs/architecture.md`](./docs/architecture.md)
  - 用于说明系统边界、架构分层、规范约束和后续开发规则
