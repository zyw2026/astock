# astock

`astock` 是一个独立于 `aks-mcp` 的 A 股研究与选股项目。

它不负责采集原始行情，不负责维护市场数据底座，也不修改 `aks-mcp`。它只负责：

- 定义选股逻辑候选池
- 用短线因子口径验证选股逻辑
- 用可靠逻辑生成当日候选股

## 项目目标

- 建立可持续扩展的选股逻辑候选池，而不是依赖临场拍脑袋选股
- 用历史数据验证逻辑在不同市场状态下的短线爆发率、风险收益比和稳定性
- 沉淀一套“市场状态 -> 可靠逻辑 -> 当日候选股”的标准化流程
- 输出可追溯、可复盘、可迭代的每日选股结果

## 功能概览

- 消费已部署的 `aks-mcp` 数据服务
- 管理本地选股逻辑候选池
- 用短线低吸因子口径评估逻辑在不同市场状态下的表现
- 输出每日候选股，并通过 `show-selection` 查看解释字段
- 支持指定历史交易日的策略回放与结果验证
- 在本地保存逻辑命中、验证结果和选股结果

## 状态策略

当前实现固定使用 4 个市场状态，不同状态启用不同策略。

### `trend`

适用场景：主线明确、强势方向延续性较好。

- `trend_pullback`（主线趋势回踩）
- `leader_first_pullback`（龙头首次分歧回踩）
- `limit_up_repair`（涨停后修复承接）

### `rotation`

适用场景：市场轮动为主，结构性机会多，但持续性弱于主升。

- `leader_first_pullback`（龙头首次分歧回踩）
- `rotation_catchup`（轮动补涨）
- `rotation_base_breakout`（轮动平台突破）
- `ma10_reclaim`（回踩后重回 MA10）
- `oversold_rebound`（超跌反抽）
- `limit_up_repair`（涨停后修复承接）

### `weak_rotation`

适用场景：轮动偏弱，适合分歧低吸、弱转强和修复型先手。

- `rotation_catchup`（轮动补涨）
- `rotation_base_breakout`（轮动平台突破）
- `ma10_reclaim`（回踩后重回 MA10）
- `weak_rotation_dip_absorb`（弱轮动分歧承接）
- `weak_rotation_failed_break_reclaim`（弱轮动假跌破回收）
- `fund_flow_reversal`（资金反转低吸）
- `weak_rotation_flat_reclaim`（弱轮动横盘回收）
- `oversold_rebound`（超跌反抽）

### `panic`

适用场景：市场极弱，只保留非常克制的反转型低吸。

- `fund_flow_reversal`（资金反转低吸）

## 策略分层

当前 `11` 个内置策略按最新历史回放和 validation 结果，分为以下四层。

### 主力

- `rotation_base_breakout`（轮动平台突破）
- `rotation_catchup`（轮动补涨）
- `weak_rotation_dip_absorb`（弱轮动分歧承接）
- `trend_pullback`（主线趋势回踩）

### 观察

- `fund_flow_reversal`（资金反转低吸）
- `weak_rotation_flat_reclaim`（弱轮动横盘回收）
- `ma10_reclaim`（回踩后重回 MA10）
- `weak_rotation_failed_break_reclaim`（弱轮动假跌破回收）
- `limit_up_repair`（涨停后修复承接）

### 次观察

- `leader_first_pullback`（龙头首次分歧回踩）
- `oversold_rebound`（超跌反抽）

## 当前实现说明

- 当前验证体系是“短线低吸因子验证版”，重点看未来 `1-3` 天的命中率、爆发率、最大拉升和回撤
- 历史市场状态优先使用 `market_fund_flow`，缺失时会使用特征面板做后备推断
- 当前支持两类选股模式：
  - `run-selection`：生成当日候选股
  - `replay-selection`：回放指定历史交易日，并输出后续 `1/2/3` 天表现
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
pip install -e . --no-deps
```

如需完整依赖：

```bash
pip install -e .
```

### 2. 检查数据源和初始化本地库

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

### 常用命令

查看项目信息：

```bash
astock info
```

查看内置逻辑候选池：

```bash
astock list-logics
```

查看市场概览：

```bash
astock show-market --top-n 3
```

查看市场状态：

```bash
astock show-regime --trade-date 2026-03-19
```

命令输出中的 `regime` 和 `regime_evidence` 即当前市场状态及其判断依据。

查看最新验证结果：

```bash
astock show-validation --limit 15
```

查看最新可靠性快照：

```bash
astock show-snapshot --regime weak_rotation
```

查看当日候选股明细：

```bash
astock show-selection --trade-date 2026-03-19
```

回放历史交易日选股结果：

```bash
astock replay-selection 2026-03-16 --symbol-limit 120 --chunk-size 20 --selection-limit 10
```

## 文档导航

- 架构文档：[`docs/architecture.md`](./docs/architecture.md)
  - 用于说明系统边界、架构分层、规范约束和后续开发规则
