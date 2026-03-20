# astock 架构文档

本文档用于固定 `astock` 的边界、分层和开发约束。后续开发应在此基线内扩展。

## 1. 定位

`astock` 是独立于 `aks-mcp` 的研究与选股引擎。

一句话：

`astock = 逻辑研究 + 历史验证 + 数据反推 + 逻辑调度 + 当日选股`

当前验证口径固定为“短线低吸因子验证”，不是长期持有型策略评估。

## 2. 边界

外部边界：

- `aks-mcp` 是外部依赖，不属于 `astock`
- 默认只通过 REST 接入
- 可选 MCP 或只读数据库，但不能依赖 `aks-mcp` 内部 Python 模块

内部负责：

- 逻辑定义与注册
- 标准特征构建
- 历史验证与可靠性评估
- 市场状态识别
- 当日选股与历史回放
- 数据反推候选策略发现
- 本地结果落库

内部不负责：

- 原始行情采集
- 基础数据维护
- 交易执行
- 修改 `aks-mcp` 代码

## 3. 核心原则

- 先验证逻辑，再让逻辑参与选股
- 验证对象是逻辑，不是股票
- 每个逻辑必须可历史回放
- 市场状态是逻辑启用前置条件
- 所有外部访问必须收口在 `connectors`

## 4. 分层

固定结构：

```text
src/astock/
├── app/
├── connectors/
├── factor_lab/
├── logic_pool/
├── validation/
├── selection/
├── storage/
└── cli.py
```

职责：

- `app`：配置管理与默认参数
- `connectors`：对接 `aks-mcp`，统一处理超时、重试、限流、错误
- `logic_pool`：定义逻辑契约、注册逻辑、执行逻辑
- `factor_lab`：构建因子面板、生成标签、发现候选规则、产出候选策略
- `validation`：生成历史命中、聚合验证结果、产出可靠性快照
- `selection`：识别市场状态、过滤适用逻辑、生成当日选股与历史回放
- `storage`：统一管理本地库和读写接口
- `cli`：用户入口，只做编排与展示

## 5. 主流程

历史验证：

1. `connectors` 获取历史数据
2. `validation` 构建标准特征表
3. `validation` 生成历史市场状态映射
4. `logic_pool` 执行逻辑
5. `validation` 聚合指标
6. `storage` 写入命中、结果、快照

数据反推：

1. `connectors` 获取历史数据
2. `validation` 复用标准特征构建能力
3. `factor_lab` 构建因子面板与短线标签
4. `factor_lab` 发现候选规则并生成候选策略
5. `storage` 写入候选注册区
6. `logic_pool` 只加载已提升到运行时的候选策略

当日选股：

1. `selection` 判断市场状态
2. `storage` 读取最新可靠性快照
3. `selection` 过滤适用逻辑
4. `logic_pool` 执行逻辑
5. `storage` 写入当日选股结果

历史回放：

1. `selection` 还原指定交易日市场状态
2. `storage` 读取最新可靠性快照
3. `selection` 过滤适用逻辑并执行
4. 输出后续 `1/2/3` 天表现、最大涨幅与回撤

历史市场状态映射采用双轨：

- 优先 `market_fund_flow`
- 缺失时使用特征面板后备推断

## 6. 固定契约

逻辑契约最少包含：

- `logic_id`
- `name`
- `source`
- `regime_whitelist`
- `required_datasets`
- `ranking_rule`
- `holding_days`
- `max_candidates_per_day`
- `entry_rule`
- `exit_rule`
- `invalid_rule`

固定市场状态：

- `trend`
- `rotation`
- `weak_rotation`
- `panic`

固定结果表：

- `logic_signal_hit`
- `logic_validation_result`
- `logic_reliability_snapshot`
- `daily_selection_output`
- `discovery_run_result`
- `discovered_logic_candidate`
- `runtime_discovered_logic`

## 7. 依赖约束

允许：

```text
cli -> validation / selection / factor_lab
validation / selection / factor_lab -> connectors
validation / selection / factor_lab -> logic_pool
validation / selection / factor_lab -> storage
selection -> validation
```

禁止：

- `cli -> connectors`
- `cli -> storage`
- `logic_pool -> connectors`
- `logic_pool -> storage`
- `connectors -> validation / selection / factor_lab`

## 8. 开发规范

- 一个逻辑只能有一个定义源
- validation 和 selection 必须复用同一套逻辑执行规则
- 先构建标准特征表，再执行逻辑
- 历史验证优先评估短线 `1-3` 天表现
- 历史回放必须使用历史市场状态，不能复用当前市场状态
- `discover-logics` 只发现候选；`promote-discovered-logics` 才能进入运行时
- 业务层不能散落 HTTP 请求和本地 SQL
- 长期复用能力必须进入正式模块，不能停留在一次性脚本
