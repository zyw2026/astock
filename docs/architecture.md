# astock 架构文档

本文档用于固定 `astock` 的边界、分层和开发约束。后续开发应在此架构内扩展，不再反复改边界。

## 1. 定位

`astock` 是独立于 `aks-mcp` 的研究与选股引擎，职责固定为：

- 管理选股逻辑候选池
- 用历史数据验证逻辑
- 评估逻辑可靠性
- 根据市场状态启用逻辑
- 生成当日候选股
- 回放历史交易日选股结果

一句话：

`astock = 逻辑研究 + 历史验证 + 逻辑调度 + 当日选股`

## 2. 边界

### 2.1 外部边界

`aks-mcp` 是外部依赖，不属于 `astock`。

`astock` 只允许通过以下方式消费它：

- REST
- 可选 MCP
- 可选只读数据库

默认接入方式是 REST，接入封装位于 `src/astock/connectors/rest_client.py`。

### 2.2 内部边界

`astock` 负责：

- 逻辑定义与注册
- 标准特征构建
- 逻辑执行
- 历史命中生成
- 验证结果聚合
- 可靠性快照生成
- 市场状态识别
- 当日选股输出
- 历史回放输出
- 本地结果落库

`astock` 不负责：

- 原始行情采集
- 基础数据维护
- 交易执行
- 修改 `aks-mcp` 代码
- 依赖 `aks-mcp` 内部 Python 模块

当前实现上，验证口径已经固定为“短线低吸因子验证”，不是长期持有型策略评估。

## 3. 核心原则

- 先验证逻辑，再让逻辑参与选股
- 验证对象是逻辑，不是股票
- 每个逻辑必须可历史回放
- 市场状态是逻辑启用的前置条件
- 所有外部访问必须收口在 `connectors`

## 4. 分层

当前固定结构：

```text
src/astock/
├── app/
├── connectors/
├── logic_pool/
├── validation/
├── selection/
├── storage/
└── cli.py
```

各层职责如下。

### 4.1 `app`

职责：

- 配置管理
- 运行参数默认值

不负责业务逻辑和数据访问。

### 4.2 `connectors`

职责：

- 对接 `aks-mcp`
- 统一处理超时、重试、限流、错误
- 标准化外部响应

不负责选股判断和验证结论。

### 4.3 `logic_pool`

职责：

- 定义逻辑契约
- 注册逻辑候选池
- 在标准特征表上执行逻辑

不负责外部 IO 和结果落库。

### 4.4 `validation`

职责：

- 构建历史验证所需特征
- 执行逻辑
- 产出 `logic_signal_hit`
- 聚合 `logic_validation_result`
- 产出 `logic_reliability_snapshot`

当前验证重点指标为：

- `hit_rate_3d`
- `big_move_rate_3d`
- `avg_max_return_3d`
- `max_drawdown_3d`
- `profit_drawdown_ratio`

### 4.5 `selection`

职责：

- 识别当前市场状态
- 识别历史交易日市场状态
- 读取可靠性快照
- 过滤可启用逻辑
- 生成 `daily_selection_output`
- 生成历史回放结果

当前选股结果除基础信号外，还应包含解释字段，例如：

- `logic_name`
- `holding_days`
- `reliability_score`
- `invalidation_level`

### 4.6 `storage`

职责：

- 初始化本地库
- 提供统一读写接口
- 管理快照读取口径

### 4.7 `cli`

职责：

- 作为用户入口
- 编排 validation / selection
- 输出结果

CLI 只做调用和展示，不直接实现业务逻辑。

## 5. 主流程

### 5.1 历史验证

标准链路：

1. `connectors` 获取历史数据
2. `validation` 构建标准特征表
3. `validation` 生成历史市场状态映射
4. `logic_pool` 执行逻辑
5. `validation` 聚合短线因子指标
6. `storage` 写入命中、结果、快照

历史市场状态映射当前采用双轨口径：

- 优先使用 `market_fund_flow`
- 数据缺失时，使用特征面板后备推断

### 5.2 当日选股

标准链路：

1. `connectors` 获取市场概览和当日所需数据
2. `selection` 判断市场状态
3. `storage` 读取最新可靠性快照
4. `selection` 过滤适用逻辑
5. `logic_pool` 执行逻辑
6. `storage` 写入当日选股结果

### 5.3 历史回放

标准链路：

1. `connectors` 获取指定历史交易日前后所需数据
2. `validation` 复用标准特征表构建能力
3. `selection` 还原指定交易日市场状态
4. `storage` 读取最新可靠性快照
5. `selection` 过滤适用逻辑并执行
6. `selection` 输出后续 `1/2/3` 天表现、`3` 天最大涨幅和回撤

## 6. 统一契约

### 6.1 逻辑契约

每个逻辑至少必须包含：

- `logic_id`
- `name`
- `description`
- `regime_whitelist`
- `required_datasets`
- `ranking_rule`
- `holding_days`
- `max_candidates_per_day`
- `entry_rule`
- `exit_rule`
- `invalid_rule`

### 6.2 市场状态契约

当前固定市场状态：

- `trend`
- `rotation`
- `weak_rotation`
- `panic`

新增状态前，先改文档，再改代码。

### 6.3 存储契约

当前固定结果表：

- `logic_signal_hit`
- `logic_validation_result`
- `logic_reliability_snapshot`
- `daily_selection_output`

其中 `daily_selection_output` 当前已包含解释性字段，便于复盘和人工决策。

历史回放当前为即时查询输出，不额外落独立结果表。

## 7. 依赖约束

允许的依赖方向：

```text
cli -> validation / selection
validation / selection -> connectors
validation / selection -> logic_pool
validation / selection -> storage
selection -> validation
```

禁止的依赖方向：

- `cli -> connectors`
- `cli -> storage`
- `logic_pool -> connectors`
- `logic_pool -> storage`
- `connectors -> validation`
- `connectors -> selection`

如果需要跨层复用，优先抽公共能力，不直接绕层。

## 8. 开发规范

- 一个逻辑只能有一个定义源
- validation 和 selection 必须复用同一套逻辑执行规则
- 先构建标准特征表，再执行逻辑
- 历史验证优先评估短线 `1-3` 天表现，而不是长期稳健收益
- 历史回放必须使用历史市场状态，不能复用当前市场状态
- 业务层不能散落 HTTP 请求和本地 SQL
- 长期复用能力必须进入正式模块，不能停留在一次性脚本
- 遇到上游数据缺失或未就绪时，允许显式降级，但不能伪装成正常口径

## 9. 扩展规则

### 9.1 新增逻辑

顺序固定为：

1. 定义契约
2. 注册逻辑
3. 实现执行规则
4. 补齐验证所需特征
5. 跑历史验证

### 9.2 新增接入方式

必须放在 `connectors/`，并保持统一的错误处理和限流控制。

### 9.3 新增本地表

必须先明确：

- 表的职责
- 写入方
- 读取方
- 是否属于批次快照
