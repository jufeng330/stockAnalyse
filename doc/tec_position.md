# 持仓股票列表业务与技术方案

## 1. 文档目标与范围

### 1.1 目标

本文用于为“持仓股票列表”及其 5 个子页面提供一套完整、可落地的业务与技术方案，指导后续在当前 Flask 项目中实现真实持仓管理能力。方案覆盖：

- 持仓列表 `/holding-stocks`
- 二次分析 `/holding-reanalysis`
- 买卖决策 `/position-decision`
- 复盘 `/holding-review`
- 历史记录（持仓列表页内 Tab + 列表区）
- 持仓域历史记录查询与展示

本文重点输出以下内容：

- 业务设计与页面职责
- 总体架构设计
- 分层与类设计
- 数据结构与状态机设计
- API 资源规划
- AI 交互角色与模板边界
- 页面与历史记录的关系
- 实施顺序与验证建议

### 1.2 设计依据

业务设计来源：

- `/mnt/github/stock/股票买卖模版.md`
- `doc/trading_decision_online_plan.md`

参考方案：

- `doc/tec_trading_decision.md`
- `doc/ui/holding_stocks_page.html`
- `doc/ui/holding_reanalysis_page.html`
- `doc/ui/add_position_decision_page.html`
- `doc/ui/reduce_position_decision_page.html`
- `doc/ui/sell_decision_page.html`
- `doc/ui/holding_review_page.html`
- `doc/ui/weekly_holding_review_page.html`
- `doc/ui/monthly_holding_review_page.html`
- `doc/ui/quarterly_holding_review_page.html`

当前实现参考：

- `src/stock_analyse/interfaces/web/routes/misc.py`
- `src/stock_analyse/interfaces/web/routes/trading_decision.py`
- `src/stock_analyse/interfaces/web/services/trading_decision_service.py`
- `src/stock_analyse/infrastructure/persistence/trading_decision/schema_manager.py`
- `static/ui/nav.js`
- `templates/watch_stocks.html`

### 1.3 本文范围内 / 范围外

本文范围内：

- 持仓域完整业务闭环设计
- 持仓列表、二次分析、买卖决策、复盘、历史记录的结构化方案
- 与当前 Flask 架构兼容的落地方式
- 与当前导航和原型页的对齐方案

本文范围外：

- 券商真实交易下单
- 多用户权限体系
- 全量前端框架迁移
- 本轮直接实现真实持仓 CRUD

---

## 2. 现状与设计约束

### 2.1 当前项目现状

当前“关注股票域”已经有真实页面、API 和持久化能力，核心落地在：

- `src/stock_analyse/interfaces/web/routes/trading_decision.py`
- `src/stock_analyse/interfaces/web/services/trading_decision_service.py`
- `templates/watch_stocks.html`
- `src/stock_analyse/infrastructure/persistence/trading_decision/schema_manager.py`

而“持仓股票域”仍主要由 `src/stock_analyse/interfaces/web/routes/misc.py` 直接返回 `doc/ui/*.html` 原型页。按本次设计，持仓栏目应收敛为 5 个子页面：

- `/holding-stocks`
- `/holding-reanalysis`
- `/position-decision`
- `/holding-review`
- `/holding-stocks#holding-records`

其中：

- 买卖决策页面不再让用户先选动作，而是由 AI 根据持仓上下文输出买入 / 减仓 / 卖出 / 继续观察建议
- 周 / 月 / 季度属于“复盘”页面内的三类复盘视角

这意味着：

1. 持仓域业务概念已经存在，但还未结构化落地
2. 当前持仓页面更像信息架构原型，而不是真实业务页
3. 本轮应优先把业务、技术、导航和 UI 设计统一下来，为后续真实实现提供稳定边界

### 2.2 现有可复用能力

当前项目可以直接复用的能力包括：

1. Flask App 与路由结构
2. 交易决策域现有页面聚合模式
3. SQLite 结构化记录模式
4. SSE 流式分析模式
5. AI 分析编排能力
6. 左侧树状导航机制

重点可复用模式：

- `templates/watch_stocks.html` 的“当前摘要 + 同级历史记录列表”组织方式
- `doc/tec_trading_decision.md` 的“聚合根 + 历史事实 + AI 记录”建模方式
- `static/ui/nav.js` 的分组导航与页面元信息配置方式

### 2.3 当前缺失能力

对于持仓域，当前项目仍缺少：

1. `HoldingStock` 主实体
2. 多次买入与累计成本模型
3. 买卖成交明细模型
4. 二次分析记录模型
5. AI 推断买入 / 减仓 / 卖出 / 继续观察的统一决策模型
6. 周 / 月 / 季度复盘统一模型
7. 持仓侧统一历史记录查询模型
8. 持仓状态机与摘要回填规则

### 2.4 设计约束

本方案必须满足以下约束：

1. 保持当前 Flask 项目结构，不进行框架迁移
2. 复用现有 AI 与 SSE 基础设施
3. 持仓列表必须支持多次买入累计，不允许只存单一买入价
4. 主列表展示“当前摘要”，历史记录保存“动作事实”
5. 关键动作必须保留人工确认
6. 本轮以文档与导航对齐为主，不直接扩展到完整真实持仓页实现

---

## 3. 业务方案设计

## 3.1 产品定位

持仓股票列表不是简单的资产展示页，而是“持有管理工作台”。

它负责：

- 展示当前持仓摘要
- 管理多次买入后的累计成本与仓位
- 触发持仓期内的再评估与买卖决策
- 形成周 / 月 / 季度复盘记录
- 把所有动作沉淀为可查询历史

### 3.2 页面结构总览

持仓域建议按以下 5 个子页面理解：

```text
持仓股票列表
├── 1. 持仓列表
│   ├── 当前持仓摘要列表
│   ├── 累计成本 / 买卖明细
│   └── 行内动作入口
├── 2. 二次分析
│   ├── 原逻辑是否成立
│   ├── 关键变化项
│   └── 对原计划的影响
├── 3. 买卖决策
│   ├── 基于财报、历史成交、持仓计划自动推断动作
│   ├── 输出五个固定 Tabs：触发条件 / 核心理由 / 执行注意事项 / 风险分析 / 结论
│   └── 保存统一决策记录
├── 4. 复盘
│   ├── 页面内选择通用 / 周 / 月 / 季度视角
│   ├── 输出执行质量、风险变化与下阶段动作建议
│   └── 保存统一复盘记录
└── 5. 历史记录
    ├── 二次分析记录
    ├── 买卖决策记录
    └── 复盘记录
```

### 3.3 持仓列表设计

#### 页面职责

- 展示当前所有持仓
- 体现累计成本、当前价格、收益、仓位与风险
- 提供进入动作页与历史记录的统一入口

#### 列表字段建议

至少展示：

- 股票代码
- 股票名称
- 累计买入数量
- 可卖数量
- 持仓均价
- 当前价格
- 持仓市值
- 浮动盈亏金额
- 浮动收益率
- 已实现盈亏
- 当前仓位占比
- 当前阶段
- 当前价格区间
- 当前风险状态
- 当前建议动作
- 最近一次复核时间

#### 关键业务规则

1. 支持新增买入，多次买入后自动累计成本
2. 持仓均价必须由买入批次重算得出
3. 买卖明细必须单独保留，不能只在主表覆盖
4. 卖出后要区分已实现盈亏与剩余持仓成本

### 3.4 二次分析 / 再评估设计

#### 页面职责

用于在持仓期间重新判断：

- 最初买入逻辑是否仍成立
- 当前持仓成本与赔率是否匹配
- 行业、财务、技术、新闻、估值是否发生关键变化
- 是否需要调整持仓计划、风险级别与建议动作

#### 输出重点

- 原逻辑成立度
- 关键变化项
- 风险等级变化
- 对原计划的影响
- 是否触发补仓 / 减仓 / 卖出观察

#### 与初始分析的关系

二次分析不是重跑一遍关注股票分析，而是以“持仓上下文”为核心：

- 当前持仓均价
- 持仓批次结构
- 原进场决策
- 历史分析记录
- 近期成交与市场变化

#### 当前真实实现

当前仓库已经采用“复用股票分析记录页”的落地方案，而不是单独新建持仓再评估模板：

- 页面：`/holding-reanalysis`
- 模板：`templates/stock_analysis_record.html`
- 存储：`stock_analysis_records`
- 场景标识：`analysis_scene='holding_reanalysis'`
- 关联字段：`holding_stock_id + watch_stock_id`

也就是说，持仓再评估与关注股票分析共用一套记录基础设施，但在页面标题、历史侧栏、输入上下文与 tabs 展示层面按场景分流。

### 3.5 买卖决策设计

买卖决策作为持仓栏目的第 3 个子页面，已经从原型模式切换为真实 AI 决策页：用户只提供执行参数，不再手动选择“补仓 / 减仓 / 卖出”。最终动作由 AI 结合持仓上下文直接输出。

#### AI 角色

- 固定角色：`股票分析师`

#### 核心输入

- 当前持仓摘要
- 财报数据：`company_profile`、`financial_indicators`、`reports`
- 历史成交数据：持仓 `trades`、`lots`
- 持仓计划数据：最近 `trade_plan_analysis_records`
- 辅助背景：最近 `entry_decision_records`、`stock_analysis_records`
- 市场快照：技术面、情绪、新闻、市场上下文

#### 固定输出

输出必须是五个固定 Tabs，且顺序固定：

1. `触发条件`
2. `核心理由`
3. `执行注意事项`
4. `风险分析`
5. `结论`

每个 Tab 都必须先给顶部结论，再给底部证据列表；最后一个 `结论` Tab 必须综合前四项，输出：

- `recommended_action`：`buy / reduce / sell / watch`
- `decision_status`：`buy_candidate / reduce_candidate / sell_candidate / observe`
- `confidence`
- `conclusion_summary`

#### 页面交互

- 页面：`/position-decision?holding_stock_id=...`
- 模板：`templates/position_decision.html`
- 运行：SSE 流式日志 + 后台异步任务
- 历史：支持按 `record_id` 回放同一套五个 Tabs 展示

#### 当前真实实现

当前仓库已经落地独立的买卖决策链路：

- 页面路由：`src/stock_analyse/interfaces/web/routes/trading_decision.py`
- 服务装配：`TradingDecisionService.build_position_decision_context()`
- AI 编排：`PositionDecisionOrchestrator`
- 存储：`position_decision_records`
- 仓位摘要回填：`holding_stocks.suggested_action + last_review_at`

这意味着买卖决策已不再是静态原型页，也不再把 `decision_type` 当作请求输入，而是作为 AI 输出结果落库。

#### 结果字段

- `decision_type`：AI 推荐动作
- `decision_status`：动作候选状态
- `trigger_summary`
- `reason_summary`
- `execution_summary`
- `risk_summary`
- `conclusion_summary`
- `confidence`
- `tabs_json`
- `evidence_json`
- `raw_result_json`

#### 历史与回放

- 同一持仓通过 `holding_stock_id` 聚合买卖决策历史
- 若持仓关联关注股票，则记录同步保留 `watch_stock_id`
- 历史页与实时生成页使用同一五个 Tabs 渲染协议
- 页面保存与运行后自动保存都写入同一张记录表

#### 推荐动作语义

- `buy`：适合买入
- `reduce`：适合减仓
- `sell`：适合卖出
- `watch` / `hold`：继续观察

这样可以保证“动作”来自 AI 分析结果，而不是来自用户预设。 

### 3.6 复盘设计

复盘作为持仓栏目的第 4 个子页面已经按真实工作流统一建模，在同一页面内以 `review_type` 区分：

- 通用复盘
- 周复盘
- 月复盘
- 季度复盘

#### 当前真实页面与链路

- 页面：`/holding-review?holding_stock_id=...&record_id=...`
- 模板：`templates/holding_review.html`
- 路由：`src/stock_analyse/interfaces/web/routes/trading_decision.py`
- 服务：`TradingDecisionService.build_holding_review_page_data()`
- 异步执行：SSE + 后台任务 + `HoldingReviewOrchestrator`
- 存储：`holding_review_records`
- 历史回放：同页通过 `record_id` 回放

#### 与二次分析 / 买卖决策的边界

- 二次分析：回答“原买入逻辑是否仍成立，发生了哪些变化”
- 买卖决策：回答“现在更适合买入 / 减仓 / 卖出 / 继续观察什么动作”
- 复盘：回答“这段持仓过程中结果如何、过程是否合格、纪律哪里做对或做错、下一步该如何改进”

也就是说，复盘不是再跑一遍股票分析，也不是再给一次动作建议，而是把：

- 历史成交
- 原始进场决策
- 最近再评估
- 最近买卖决策
- 财报与市场数据

放到同一个“结果 + 过程 + 纪律 + 后续动作”的框架里沉淀。

#### 固定输出结构

当前真实实现不再使用自由文本复盘，而是固定输出：

- `performance_summary`
- `execution_summary`
- `risk_summary`
- `discipline_summary`
- `next_action_summary`
- `conclusion_tag`
- `tabs`

页面只显示固定 4 个 Tabs：

1. `执行与卖出复盘`
2. `结果复盘`
3. `方法与纪律`
4. `后续动作`

每个 Tab 都必须满足“顶部结论 + 底部理由列表”。

#### 周复盘

偏跟踪变化，强调：

- 本周收益变化
- 执行动作
- 风险暴露
- 下周重点

#### 月复盘

偏总结执行情况，强调：

- 收益来源
- 结构偏差
- 仓位纪律
- 优化方向

#### 季度复盘

偏评估方法正确性，强调：

- 策略有效性
- 收益质量
- 经验教训
- 下季度调整方向

#### 结论标签

当前实现支持以下结构化标签：

- `logic_ok`
- `need_recheck`
- `execution_issue`
- `risk_rising`
- `prepare_reduce`
- `prepare_sell`

这些标签既用于页面展示，也用于后续历史筛选与聚合。
### 3.7 历史记录设计

#### 展示方式

持仓域历史记录采用“Tab + 列表”方式。

当前实现中，持仓再评估历史优先按 `holding_stock_id` 查询；若该持仓同时关联了关注股票，则记录也会保留 `watch_stock_id`，这样历史中心和关注股票链路仍可复用原有聚合逻辑。

至少包含：

- 二次分析记录
- 买卖决策记录（由 AI 输出买入 / 减仓 / 卖出 / 继续观察）
- 复盘记录（页面内再区分通用 / 周 / 月 / 季度）

#### 查询维度

支持按以下条件过滤：

- 股票代码 / 名称
- 记录类型
- 结论标签
- 风险等级
- 决策动作
- 复盘类型
- 创建时间

---

## 4. 总体架构设计

### 4.1 总体设计原则

推荐将“持仓股票列表”作为交易决策中心下的第二个真实业务子域，与 watch-stocks 域形成对称结构：

1. `HoldingStock` 作为主聚合根
2. `HoldingLot` / `HoldingTrade` 保存买卖事实
3. 各动作页保存独立记录
4. 主列表保存当前摘要，历史记录保存动作事实
5. AI 输出通过适配层映射到业务模型

### 4.2 推荐模块布局

```text
src/stock_analyse/
  application/
    dto/
      trading_decision/
        holding_stock_dto.py
        holding_reanalysis_dto.py
        position_decision_dto.py
        holding_review_dto.py
        holding_history_query_dto.py
    use_cases/
      trading_decision/
        list_holding_stocks.py
        get_holding_stock_detail.py
        create_holding_reanalysis.py
        create_position_decision.py
        create_holding_review.py
        list_holding_records.py
    services/
      trading_decision/
        holding_management_application_service.py

  domain/
    models/
      trading_decision/
        holding_stock.py
        holding_lot.py
        holding_trade.py
        holding_status_snapshot.py
        holding_reanalysis_record.py
        position_decision_record.py
        holding_review_record.py
    value_objects/
      trading_decision/
        holding_risk_status.py
        decision_type.py
        review_type.py
    services/
      trading_decision/
        holding_cost_calculator.py
        holding_decision_context_builder.py
        holding_summary_service.py
        holding_state_machine.py

  infrastructure/
    persistence/
      trading_decision/
        holding_stock_repository.py
        holding_trade_repository.py
        holding_reanalysis_repository.py
        position_decision_repository.py
        holding_review_repository.py
        holding_history_query_repository.py

  interfaces/
    web/
      routes/
        trading_decision.py
      services/
        trading_decision_service.py
```

---

## 5. 领域模型与类设计

### 5.1 核心实体概览

- `HoldingStock`：当前持仓摘要对象
- `HoldingLot`：买入批次对象
- `HoldingTrade`：买入/卖出成交明细
- `HoldingStatusSnapshot`：状态刷新快照
- `HoldingReanalysisRecord`：二次分析记录
- `PositionDecisionRecord`：补仓/减仓/卖出统一决策记录
- `HoldingReviewRecord`：复盘记录，保存 5 个摘要字段、结论标签、4 个 Tabs、证据列表与上下文快照
- `HoldingHistoryQueryModel`：统一历史查询读模型

### 5.2 `HoldingStock` 设计

#### 角色定位

- 持仓主列表中的一行
- 持仓域聚合根
- 保存当前摘要状态

#### 核心字段

- `id`
- `stock_code`
- `stock_name`
- `market`
- `total_buy_quantity`
- `sellable_quantity`
- `avg_cost_price`
- `current_price`
- `position_market_value`
- `unrealized_pnl_amount`
- `unrealized_pnl_ratio`
- `realized_pnl_amount`
- `position_ratio`
- `current_stage`
- `current_price_zone`
- `risk_status`
- `suggested_action`
- `last_reanalysis_record_id`
- `last_decision_record_id`
- `last_review_record_id`
- `last_reviewed_at`
- `status`

### 5.3 `HoldingLot` 设计

#### 角色定位

- 记录某一笔买入批次
- 支撑累计成本、分笔管理和明细展示

#### 核心字段

- `id`
- `holding_stock_id`
- `lot_no`
- `buy_date`
- `buy_price`
- `buy_quantity`
- `buy_amount`
- `fee_amount`
- `source_decision_record_id`
- `note`

### 5.4 `HoldingTrade` 设计

#### 角色定位

- 记录买入与卖出成交事实
- 提供买卖明细与已实现盈亏计算基础

#### 核心字段

- `id`
- `holding_stock_id`
- `trade_type`
- `trade_date`
- `price`
- `quantity`
- `amount`
- `fee_amount`
- `decision_record_id`
- `note`

### 5.5 `HoldingReanalysisRecord` 设计

#### 角色定位

- 持仓期内一次正式再评估动作
- 用于判断原逻辑是否变化

#### 核心字段

- `id`
- `holding_stock_id`
- `analysis_type`
- `logic_status`
- `change_level`
- `fundamental_change_summary`
- `valuation_summary`
- `risk_summary`
- `adjustment_suggestion`
- `conclusion_summary`
- `created_at`

### 5.6 `PositionDecisionRecord` 设计

#### 角色定位

- 统一承接补仓 / 减仓 / 卖出决策
- 支持保存结构化建议与人工确认结果

#### 核心字段

- `id`
- `holding_stock_id`
- `decision_type`
- `decision_status`
- `trigger_summary`
- `reason_summary`
- `suggested_ratio`
- `suggested_quantity`
- `execution_mode`
- `risk_summary`
- `conclusion_summary`
- `confirmed_at`
- `created_at`

### 5.7 `HoldingReviewRecord` 设计

#### 角色定位

- 承接持仓复盘记录
- 通过 `review_type` 区分通用 / 周 / 月 / 季度 / 最终复盘

#### 核心字段

- `id`
- `holding_stock_id`
- `review_type`
- `period_key`
- `performance_summary`
- `execution_summary`
- `risk_summary`
- `discipline_summary`
- `next_action_summary`
- `conclusion_tag`
- `created_at`

---

## 6. 状态机与摘要回填设计

### 6.1 HoldingStock 状态机

建议状态：

- `holding`
- `under_review`
- `high_risk`
- `partial_exit`
- `closed_waiting_review`
- `archived`

### 6.2 状态规则

1. 默认持仓状态为 `holding`
2. 发起再评估或复盘时，可进入 `under_review`
3. 卖出风险明确抬升时，可进入 `high_risk`
4. 部分卖出后进入 `partial_exit`
5. 全部清仓后进入 `closed_waiting_review`

### 6.3 摘要回填规则

`HoldingStock` 的摘要字段由最新有效记录回填：

- 最新状态刷新记录回填价格、盈亏、阶段、区间
- 最新二次分析回填风险状态、计划变化摘要
- 最新决策记录回填建议动作
- 最新复盘记录回填结论标签与最近复核时间

---

## 7. 页面与历史记录设计

### 7.1 持仓主页面

页面上半区：

- 统计卡
- 当前持仓列表
- 行内动作按钮

页面下半区：

- Tab 形式历史记录区
- 列表卡片展示
- 统一筛选区

### 7.2 动作页设计原则

- 二次分析页：突出原逻辑是否成立
- 买卖决策页：统一承接补仓 / 减仓 / 卖出，重点展示动作类型、建议比例、触发条件、执行模式与不执行风险
- 复盘页：统一承接通用 / 周 / 月 / 季度视角，重点展示结果、过程、纪律与下阶段动作建议

---

## 8. 与现有关注股票域的关系

建议流转关系：

```text
WatchStock
  -> DecisionCase
  -> TradePlanAnalysisRecord
  -> HoldingStock
  -> HoldingReanalysisRecord
  -> PositionDecisionRecord
  -> HoldingReviewRecord
```

关键流转规则：

1. 关注股票完成进场决策并实际买入后，才进入持仓股票列表
2. 进场决策和买前分析记录仍然是持仓期动作的重要输入
3. 持仓期内所有新动作都保留独立记录，不覆盖买前记录
4. 后续组合层复盘可汇总持仓侧记录

---

## 9. 实施建议

### 9.1 本轮实施范围

本轮优先完成：

1. `doc/tec_position.md` 业务与技术方案
2. `doc/api_position.md` API 与 AI 交互方案
3. `doc/ui/` 下持仓原型页统一更新
4. `static/ui/nav.js` 持仓栏目导航更新
5. `src/stock_analyse/interfaces/web/routes/misc.py` 持仓入口说明整理

### 9.2 后续真实化阶段

#### Phase 1：持仓主列表真实化

- 落地 `HoldingStock`、`HoldingLot`、`HoldingTrade`
- 提供真实持仓摘要与买卖明细

#### Phase 2：动作页真实化

- 落地二次分析与买卖决策记录
- 打通当前摘要回填

#### Phase 3：复盘与历史中心

- 落地周 / 月 / 季度复盘
- 提供统一历史记录查询

---

## 10. 验证建议

执行完成后应验证：

1. 业务文档是否完整覆盖持仓列表、二次分析、买卖决策、复盘、历史记录
2. 术语是否与 `doc/trading_decision_online_plan.md` 和 `/mnt/github/stock/股票买卖模版.md` 保持一致
3. 页面命名、按钮命名、Tab 命名是否和导航一致
4. 当前服务入口是否仍能打开所有原型页
5. 文档中的 API、实体、页面是否可以一一映射
