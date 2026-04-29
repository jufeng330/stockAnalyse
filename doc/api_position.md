# 持仓股票列表 API 与 AI 交互设计

## 1. 文档目标

本文档用于定义持仓 5 个子页面对应的：

- 页面 API 规划
- 请求 / 响应结构
- 历史记录查询方式
- AI 角色分工
- AI Prompt 模板设计

本方案对齐：

- `doc/tec_position.md`
- `doc/trading_decision_online_plan.md`
- `doc/tec_trading_decision.md`

---

## 2. API 设计原则

### 2.1 命名空间

统一使用：

- `/api/trading-decision/*`

### 2.2 响应结构

#### 成功

```json
{
  "success": true,
  "data": {},
  "message": "ok"
}
```

#### 失败

```json
{
  "success": false,
  "message": "invalid state",
  "error": {
    "code": "INVALID_STATE",
    "message": "invalid state"
  }
}
```

### 2.3 设计原则

1. 主列表 API 返回“当前摘要”
2. 明细 API 返回“批次、成交、历史记录”
3. 长耗时 AI 动作优先走异步 + SSE
4. 关键动作均支持“生成草案”和“保存记录”分离
5. 补仓 / 减仓 / 卖出尽量统一为一类资源，只用 `decision_type` 区分

---

## 3. 持仓主页面 API

## 3.1 `GET /api/trading-decision/holding-stocks`

用途：查询持仓主列表。

### 查询参数

- `keyword`
- `risk_status`
- `suggested_action`
- `stage`
- `price_zone`
- `review_type`
- `page`
- `page_size`

### 响应示例

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "id": "HS-20260425-0001",
        "stock_code": "300750",
        "stock_name": "宁德时代",
        "avg_cost_price": 176.8,
        "current_price": 182.4,
        "total_buy_quantity": 2000,
        "position_market_value": 364800,
        "unrealized_pnl_ratio": 0.0317,
        "position_ratio": 0.096,
        "current_stage": "B",
        "current_price_zone": "合理偏低区",
        "risk_status": "normal",
        "suggested_action": "观察第二笔补仓条件",
        "last_reviewed_at": "2026-04-25 09:30"
      }
    ],
    "summary": {
      "holding_count": 12,
      "high_risk_count": 1,
      "review_due_count": 3,
      "decision_due_count": 4
    },
    "pagination": {
      "page": 1,
      "page_size": 20,
      "total": 12
    }
  },
  "message": "ok"
}
```

## 3.2 `GET /api/trading-decision/holding-stocks/<id>`

用途：查询单只持仓详情。

### 返回内容

- 持仓摘要
- 最近一次二次分析摘要
- 最近一次买卖决策摘要
- 最近一次复盘摘要

## 3.3 `GET /api/trading-decision/holding-stocks/<id>/lots`

用途：查询买入批次明细。

### 返回重点

- 批次编号
- 买入日期
- 买入价格
- 买入数量
- 买入金额
- 费用
- 来源决策

## 3.4 `GET /api/trading-decision/holding-stocks/<id>/trades`

用途：查询买卖成交明细。

### 返回重点

- 买入 / 卖出
- 成交日期
- 成交价格
- 成交数量
- 费用
- 对应决策记录

---

## 4. 二次分析 API

### 当前实现说明

当前仓库已经落地的真实页面与记录接口为：

- `GET /holding-reanalysis?holding_stock_id=...&record_id?=...`
- `POST /api/trading-decision/stock-analysis-records`
- `GET /api/trading-decision/stock-analysis-records?holding_stock_id=...`

其中持仓再评估复用 `stock_analysis_record.html` 页面与 `stock_analysis_records` 存储表，通过 `analysis_scene='holding_reanalysis'` 和 `holding_stock_id` 区分场景。

## 4.1 当前“开始二次分析”按钮的真实运行链路

> 重要：当前仓库里“开始二次分析”**还没有**落地一个独立的
> `POST /api/trading-decision/holding-stocks/<id>/reanalysis/run`。
>
> 当前真实实现是：**持仓再评估页面复用单股 AI 分析入口 `/api/analyze_stock_ai`**，
> 然后在分析完成后按 `holding_reanalysis` 场景写入 `stock_analysis_records`。

### 4.1.1 前端按钮行为

持仓再评估页按钮位于 `stock_analysis_record.html`，点击“开始二次分析”时会调用：

- `POST /api/analyze_stock_ai`

请求体当前实际包含：

```json
{
  "stock_code": "300750",
  "market": "A股",
  "start_date": "2026-01-20",
  "end_date": "2026-04-29",
  "trade_date": "2026-04-29",
  "analysis_depth": "standard",
  "watch_stock_id": "WS-XXXX",
  "holding_stock_id": "HS-XXXX",
  "analysis_scene": "holding_reanalysis",
  "client_id": "stock_analysis_record_xxx"
}
```

### 4.1.2 后端数据访问逻辑

按钮请求进入 `/api/analyze_stock_ai` 后，当前链路如下：

1. `analysis.py` 解析请求参数，识别：
   - `stock_code`
   - `market`
   - `trade_date`
   - `analysis_depth`
   - `watch_stock_id`
   - `holding_stock_id`
   - `analysis_scene`
2. 如果命中当天缓存，则直接返回缓存结果，并走 SSE 推送结果。
3. 如果未命中缓存，则调用：
   - `StockAnalyzerService.stock_ai_analysis_process(...)`
4. `stock_ai_analysis_process(...)` 当前实际调用的底层 AI 用例仍是：
   - `analyze_single_stock_ai_use_case.execute(...)`
5. AI 返回结果后：
   - 若 `analysis_scene == 'holding_reanalysis'` 或存在 `holding_stock_id`
   - 则调用 `TradingDecisionService.save_stock_analysis_record(...)`
   - 最终按持仓场景写入 `stock_analysis_records`
6. 写入成功后，后端会额外：
   - 自动补 `analysis_scene = holding_reanalysis`
   - 自动补 `holding_stock_id`
   - 若持仓有关联关注股票，则自动补 `watch_stock_id`
   - 生成固定五个 tabs
   - 回填持仓摘要：
     - `suggested_action = conclusion_summary`
     - `last_review_at = trade_date`

### 4.1.3 当前真实 AI 访问逻辑

当前“开始二次分析”**并不是独立的持仓再评估 AI orchestrator**，而是：

- 页面：持仓再评估页 `/holding-reanalysis`
- 调用入口：通用股票分析 API `/api/analyze_stock_ai`
- AI 执行器：`StockAnalyzerService.stock_ai_analysis_process(...)`
- 底层用例：`analyze_single_stock_ai_use_case.execute(...)`

这意味着：

1. **页面是持仓二次分析页面**，不是关注股票列表页面。
2. **AI 引擎目前仍复用“单股分析 / 股票分析记录”能力**，不是独立的“持仓股票分析师工作流”。
3. `holding_stock_id` 与 `analysis_scene='holding_reanalysis'` 当前主要影响的是：
   - 历史记录落库位置
   - 页面历史回放模式
   - 左侧导航归属
   - 持仓摘要回填
4. `TradingDecisionService.build_holding_reanalysis_context(...)` 已能组装：
   - 当前持仓
   - 关联关注股票
   - 历史股票分析记录
   - 历史进场决策
   - trade plan 历史
   - snapshot 财务/行情数据
   但这份上下文**当前还没有真正注入到** `analyze_single_stock_ai_use_case.execute(...)` 的提示语中。

### 4.1.4 为什么你会感觉它在调用“股票分析页面”

你的感觉是对的。当前实现里：

- UI 页面入口已经切到 `/holding-reanalysis`
- 历史记录也按持仓再评估存储
- 但运行时的 AI 分析链路仍然复用“股票分析记录 / 单股分析”那套引擎

所以从日志看，会出现类似：

- `开始AI个股分析`
- `AI个股分析完成`
- `/api/analyze_stock_ai`

这说明**当前已完成的是“页面与记录场景切换”**，而不是**“AI 工作流彻底切成独立持仓再评估角色”**。

## 4.2 当前 AI 角色与提示语

### 4.2.1 当前实际角色

当前按钮点击后，实际使用的是：

- **通用单股分析角色 / AI个股分析角色**

它不是一个专门的“持仓股票分析师”角色。

### 4.2.2 当前实际提示语来源

当前底层 AI 调用：

- 使用 `StockAnalyzerService` 的全局 `system_prompt`
- 调用 `analyze_single_stock_ai_use_case.execute(...)`
- 侧重单股行情、技术面、情绪面、新闻、基础数据等通用分析

也就是说，当前真实提示语是：

- **项目全局股票分析 system prompt + 单股分析 use case 内部提示模板**
- 而不是一条单独的“持仓再评估 Prompt”

### 4.2.3 持仓二次分析期望 AI 角色

如果按持仓二次分析的业务目标，当前页面更合理的角色应是：

- **持仓股票分析师**

职责：

1. 判断原买入逻辑是否仍成立
2. 比较当前持仓阶段与当初买入假设是否发生偏移
3. 结合财务、行情、情绪、新闻与历史决策，输出对当前持仓计划的影响
4. 输出“继续持有 / 观察 / 等待验证 / 警惕风险”等持仓语境结论
5. 不直接替代用户做最终交易执行命令

### 4.2.4 持仓二次分析推荐提示语

下面这段是更符合当前业务预期的持仓再评估 Prompt，可作为后续独立 orchestrator / agent 的提示模板：

```text
你现在的角色是“持仓股票分析师”，不是普通的单股推荐助手。

你的任务是：围绕“当前已经持有的股票”，判断原始买入逻辑是否仍成立，并评估是否需要调整后续持仓计划。

你会收到以下上下文：
1. 当前持仓摘要（股票代码、名称、成本、仓位、盈亏、最近买入时间、当前建议动作）
2. 关联关注股票信息
3. 历史股票分析记录摘要
4. 历史进场决策摘要
5. 历史持仓计划摘要
6. 最新行情 / 技术面 / 财务 / 新闻 / 情绪 snapshot

请严格围绕“持仓语境”输出，不要把它写成普通荐股报告。

你的输出必须覆盖以下五个主题：
1. 基本面变化
2. 估值与交易拥挤度
3. 风险与催化
4. 市场情绪
5. 调整建议

额外约束：
- 不要直接输出“立即买入”或“立即卖出”的命令式结论
- 必须回到“原始持仓逻辑是否仍成立”这个核心问题
- 需要明确指出：哪些变化支持继续持有，哪些变化会削弱原逻辑
- 如果证据不足，要说明不确定性来源
- 最终请给出一句话结论，适合写回持仓摘要 suggested_action
```

### 4.2.5 当前实现与目标实现的差异

当前实现：

- 页面：已是持仓二次分析页
- 记录：已按持仓再评估场景落库
- 历史：已支持按持仓回放
- AI：**仍复用通用单股分析能力**

目标实现：

- 页面：持仓二次分析页
- 记录：持仓再评估场景落库
- 历史：按持仓回放
- AI：**切换为真正的“持仓股票分析师”角色和独立 Prompt / orchestrator**

## 4.3 规划中的独立运行 API（尚未落地）

下面这个接口仍属于规划稿，当前按钮点击**并不会**直接调用它：

### `POST /api/trading-decision/holding-stocks/<id>/reanalysis/run`

用途：发起二次分析 / 再评估异步任务。

### 请求体

```json
{
  "analysis_type": "comprehensive",
  "window": "90d",
  "focus": "fundamental_first",
  "client_id": "holding_reanalysis_123"
}
```

### 返回示例

```json
{
  "success": true,
  "data": {
    "task_mode": "async",
    "client_id": "holding_reanalysis_123",
    "holding_stock_id": "HS-20260425-0001",
    "target_type": "holding_reanalysis"
  },
  "message": "holding reanalysis started"
}
```

## 4.4 `POST /api/trading-decision/stock-analysis-records`

用途：保存结构化二次分析记录；当请求带 `holding_stock_id` 或 `analysis_scene='holding_reanalysis'` 时，按持仓再评估场景写入。

### 当前已支持字段

- `holding_stock_id`
- `watch_stock_id`（若持仓有关联关注股票则自动回填）
- `analysis_scene = holding_reanalysis`
- `trade_date`
- `raw_result`

### 当前写入规则

- 记录落入 `stock_analysis_records`
- `holding_stock_id` 与 `watch_stock_id` 可同时存在
- 后端会生成固定五个 tabs：
  - `基本面变化`
  - `估值与交易拥挤度`
  - `风险与催化`
  - `市场情绪`
  - `调整建议`
- 保存成功后回填持仓摘要：
  - `suggested_action = conclusion_summary`
  - `last_review_at = trade_date`

## 4.5 `GET /api/trading-decision/stock-analysis-records?holding_stock_id=...`

用途：查询单只持仓的二次分析历史。

### 查询条件

- `holding_stock_id`
- `limit`

---

## 5. 买卖决策 API

买卖决策是持仓栏目的第 3 个真实页面，统一资源为：

- 页面：`/position-decision?holding_stock_id=...`
- 历史资源：`PositionDecisionRecord`
- AI 角色：`股票分析师`
- 输入主源：财报数据、历史成交数据、持仓计划数据
- 输出动作：`decision_type = buy / reduce / sell / watch`

重要变化：`decision_type` 不再是请求参数，而是 AI 分析后的输出结果。

## 5.1 `GET /position-decision?holding_stock_id=...&record_id=...`

用途：打开真实买卖决策页面，支持加载某条历史记录回放。

### 查询参数

- `holding_stock_id`：必填
- `record_id`：可选，用于回放历史记录

### 页面行为

- 渲染 `templates/position_decision.html`
- 展示持仓摘要、最近计划/再评估摘要、运行日志、历史记录列表
- 结果区固定展示五个 Tabs：
  1. `触发条件`
  2. `核心理由`
  3. `执行注意事项`
  4. `风险分析`
  5. `结论`

## 5.2 `POST /api/trading-decision/holding-stocks/<id>/position-decisions/run`

用途：发起真实买卖决策生成任务。

### 请求体

```json
{
  "trade_date": "2026-04-29",
  "analysis_depth": "standard",
  "client_id": "position_decision_123"
}
```

### 说明

- 不再接收 `decision_type`
- 后端会自动装配：
  - `financial_context`
  - `trade_history_context`
  - `holding_plan_context`
- AI 角色固定为 `股票分析师`
- 通过 `/api/sse` 推送日志、进度和最终结果
- 任务完成后会自动保存一条 `position_decision_records`

### 返回示例

```json
{
  "success": true,
  "data": {
    "status": "running",
    "task_mode": "async",
    "client_id": "position_decision_123",
    "position_decision_context": {
      "holding_stock_id": "HS-20260425-0001",
      "watch_stock_id": "WS-20260420-0001",
      "stock_code": "600519",
      "stock_name": "贵州茅台",
      "market": "A股",
      "trade_date": "2026-04-29",
      "analysis_depth": "standard",
      "role": "股票分析师",
      "data_sources": [
        "financial_context",
        "trade_history_context",
        "holding_plan_context"
      ]
    }
  },
  "message": "买卖决策任务已启动"
}
```

## 5.3 AI 输出协议

最终结果必须包含：

```json
{
  "success": true,
  "data": {
    "holding_stock_id": "HS-...",
    "watch_stock_id": "WS-...",
    "stock_code": "600519",
    "stock_name": "贵州茅台",
    "market": "A股",
    "trade_date": "2026-04-29",
    "analysis_depth": "standard",
    "decision": {
      "action": "buy|reduce|sell|watch",
      "status": "buy_candidate|reduce_candidate|sell_candidate|observe",
      "confidence": "high|medium|low",
      "summary": "一句话结论"
    },
    "tabs": [
      {"id": "trigger", "title": "触发条件", "summary": "...", "evidence": ["...", "..."]},
      {"id": "reason", "title": "核心理由", "summary": "...", "evidence": ["...", "..."]},
      {"id": "execution", "title": "执行注意事项", "summary": "...", "evidence": ["...", "..."]},
      {"id": "risk", "title": "风险分析", "summary": "...", "evidence": ["...", "..."]},
      {"id": "conclusion", "title": "结论", "summary": "...", "evidence": ["...", "..."]}
    ],
    "evidence": [
      {"tab": "核心理由", "detail": "..."}
    ],
    "meta": {
      "role": "股票分析师",
      "data_source": "holding_snapshot",
      "duration_ms": 1234
    }
  }
}
```

规则：

- 五个 Tabs 必须固定存在，顺序固定
- 每个 Tab 先给顶部结论 `summary`，再给底部证据 `evidence`
- 最后一个 `结论` Tab 必须综合前四项给出最终动作

## 5.4 `POST /api/trading-decision/position-decision-records`

用途：手动保存买卖决策记录。

### 请求体

```json
{
  "holding_stock_id": "HS-20260425-0001",
  "trade_date": "2026-04-29",
  "analysis_depth": "deep",
  "raw_result": {"success": true, "data": {}}
}
```

### 落库字段

- `holding_stock_id`
- `watch_stock_id`
- `stock_code`
- `stock_name`
- `market`
- `trade_date`
- `analysis_depth`
- `decision_type`（AI 输出动作）
- `decision_status`
- `trigger_summary`
- `reason_summary`
- `execution_summary`
- `risk_summary`
- `conclusion_summary`
- `confidence`
- `tabs_json`
- `evidence_json`
- `raw_result_json`

### 保存后的摘要回填

- `holding_stocks.suggested_action = 适合买入 / 适合减仓 / 适合卖出 / 继续观察`
- `holding_stocks.last_review_at = trade_date`

## 5.5 `GET /api/trading-decision/position-decision-records?holding_stock_id=...`

用途：查询单只持仓的买卖决策历史。

### 查询条件

- `holding_stock_id`
- `limit`

## 5.6 `GET /api/trading-decision/position-decision-records/<record_id>`

用途：获取某条买卖决策详情，用于页面回放与历史详情展示。

---

## 6. 复盘 API

复盘已经从静态原型页切换为真实业务页：

- 页面入口：`GET /holding-review?holding_stock_id=...&record_id=...`
- 模板：`templates/holding_review.html`
- 路由：`src/stock_analyse/interfaces/web/routes/trading_decision.py`
- 服务装配：`TradingDecisionService.build_holding_review_context()`
- AI 编排：`HoldingReviewOrchestrator`
- 存储：`holding_review_records`
- 页面形态：SSE 运行日志 + 固定 4 Tabs + 历史回放

AI 角色固定为：`交易专家`

真实输入不是一个单独的“复盘说明文本”，而是当前系统已存在的持仓上下文：

- `holding_stock`
- `watch_stock`
- `trade_history_context.trades`
- `trade_history_context.lots`
- `trade_history_context.recent_trade_steps`
- `entry_context.latest_entry_decision / entry_decision_history`
- `reanalysis_context.latest_reanalysis / reanalysis_history`
- `position_decision_context.latest_position_decision / position_decision_history`
- `financial_context.company_profile / financial_indicators / reports`
- `market_context.technical / sentiment / market_context / news`
- `review_focus_context`

## 6.1 `GET /holding-review?holding_stock_id=...&record_id=...`

用途：打开真实持仓复盘页面，并在同一页面里完成生成、保存、历史回放。

### 查询参数

- `holding_stock_id`：必填
- `record_id`：选填，用于历史回放

### 页面能力

- 显示当前持仓摘要、最近二次分析、最近买卖决策
- 通过 SSE 展示运行状态、进度、日志
- 固定展示 4 个 Tabs：
  1. `执行与卖出复盘`
  2. `结果复盘`
  3. `方法与纪律`
  4. `后续动作`
- 历史记录与实时生成共用同一套渲染结构

## 6.2 `POST /api/trading-decision/holding-stocks/<id>/reviews/run`

用途：发起持仓复盘草案生成。

### 请求体

```json
{
  "trade_date": "2026-04-29",
  "review_type": "weekly",
  "period_key": "2026-W18",
  "analysis_depth": "deep",
  "client_id": "holding_review_123"
}
```

### 返回摘要

```json
{
  "success": true,
  "data": {
    "status": "running",
    "task_mode": "async",
    "client_id": "holding_review_123",
    "holding_review_context": {
      "holding_stock_id": "HS-20260429-0001",
      "watch_stock_id": "WS-20260425-0001",
      "stock_code": "600519",
      "stock_name": "贵州茅台",
      "market": "A股",
      "trade_date": "2026-04-29",
      "review_type": "weekly",
      "period_key": "2026-W18",
      "analysis_depth": "deep",
      "role": "交易专家",
      "data_sources": [
        "trade_history_context",
        "entry_context",
        "reanalysis_context",
        "position_decision_context",
        "financial_context",
        "market_context"
      ]
    }
  },
  "message": "持仓复盘任务已启动"
}
```

### 结构化输出协议

AI 必须通过 Tool Calling 输出对象，固定字段包括：

- `performance_summary`
- `execution_summary`
- `risk_summary`
- `discipline_summary`
- `next_action_summary`
- `conclusion_tag`
- `tabs`

`conclusion_tag` 允许值：

- `logic_ok`
- `need_recheck`
- `execution_issue`
- `risk_rising`
- `prepare_reduce`
- `prepare_sell`

`tabs` 必须固定 4 个，顺序固定：

1. `execution_review / 执行与卖出复盘`
2. `result_review / 结果复盘`
3. `discipline_review / 方法与纪律`
4. `next_action / 后续动作`

规则：

- 四个 Tabs 必须固定存在，顺序固定
- 每个 Tab 先给顶部结论 `summary`，再给底部证据 `evidence`
- 最后一个 `后续动作` Tab 必须综合前三项与 `conclusion_tag`

## 6.3 `POST /api/trading-decision/holding-review-records`

用途：手动保存持仓复盘记录。

### 请求体

```json
{
  "holding_stock_id": "HS-20260425-0001",
  "trade_date": "2026-04-29",
  "review_type": "weekly",
  "period_key": "2026-W18",
  "analysis_depth": "deep",
  "raw_result": {"success": true, "data": {}}
}
```

### 落库字段

- `holding_stock_id`
- `watch_stock_id`
- `stock_code`
- `stock_name`
- `market`
- `trade_date`
- `review_type`
- `period_key`
- `analysis_depth`
- `performance_summary`
- `execution_summary`
- `risk_summary`
- `discipline_summary`
- `next_action_summary`
- `conclusion_tag`
- `tabs_json`
- `evidence_json`
- `context_snapshot_json`
- `raw_result_json`

### 保存后的摘要回填

- `holding_stocks.suggested_action = next_action_summary`
- `holding_stocks.last_review_at = trade_date`

## 6.4 `GET /api/trading-decision/holding-review-records?holding_stock_id=...`

用途：查询单只持仓的复盘历史。

### 查询条件

- `holding_stock_id`
- `limit`

## 6.5 `GET /api/trading-decision/holding-review-records/<record_id>`

用途：获取某条持仓复盘详情，用于页面回放与历史详情展示。

---

## 7. 持仓历史记录统一查询 API

## 7.1 `GET /api/trading-decision/holding-records`

用途：为持仓主页面下半区的 Tab + 列表提供统一数据源。

### 查询参数

- `record_type`
  - `reanalysis`
  - `add_decision`
  - `reduce_decision`
  - `sell_decision`
  - `review`
- `stock_code`
- `keyword`
- `risk_status`
- `conclusion_tag`
- `review_type`
- `date_from`
- `date_to`
- `page`
- `page_size`

### 返回示例

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "record_type": "review",
        "record_id": "HR-20260425-015",
        "stock_code": "300750",
        "stock_name": "宁德时代",
        "title": "宁德时代 - 周复盘",
        "summary": "逻辑仍成立，继续持有并跟踪第二笔条件。",
        "conclusion_tag": "logic_ok",
        "created_at": "2026-04-25 10:18"
      }
    ],
    "pagination": {
      "page": 1,
      "page_size": 10,
      "total": 32
    }
  },
  "message": "ok"
}
```

---

## 8. AI 角色设计

## 8.1 持仓分析师

### 职责

- 判断原买入逻辑是否仍成立
- 提取基本面、行业、财务、新闻、技术、估值的关键变化
- 输出风险等级与计划影响

### 不负责

- 直接替用户做最终交易决定

## 8.2 仓位决策师

### 职责

- 面向补仓 / 减仓 / 卖出输出结构化建议
- 明确触发条件、建议比例、执行模式与不执行风险

### 不负责

- 代替人工最终确认执行

## 8.3 交易专家

### 职责

- 在统一复盘页中生成通用 / 周 / 月 / 季度复盘草稿
- 同时评价结果、过程、风险变化、纪律表现与下阶段动作
- 输出固定 4 Tabs 与结构化结论标签，供页面直接回放与历史落库

### 不负责

- 夸大收益或弱化错误
- 跳过执行过程只复述盈亏结果

---

## 9. AI Prompt 模板设计

## 9.1 二次分析 Prompt 模板

### 输入上下文

- 持仓摘要
- 持仓均价与当前价格
- 买入批次与成交明细
- 原进场决策摘要
- 历史分析记录摘要
- 最近报表 / 新闻 / 技术结论

### 输出要求

必须输出：

1. 原逻辑是否仍成立
2. 关键变化项
3. 风险等级变化
4. 对原计划的影响
5. 后续动作建议
6. 一句话结论

### 约束

- 不直接说“立即买卖”
- 结论必须回到持仓上下文
- 需要指出不确定性来源

## 9.2 买卖决策 Prompt 模板

### 输入上下文

- 持仓摘要
- 成本结构
- 买卖历史
- 原进场决策
- 历史再评估记录
- 当前阶段 / 区间 / 风险状态
- 当前市场与基本面变化
- `decision_type`

### 输出要求

必须输出：

1. 触发条件
2. 核心理由
3. 建议动作
4. 建议比例 / 数量
5. 不执行风险
6. 执行注意事项
7. 一句话结论

### 约束

- 不输出带强制口吻的下单命令
- 补仓要解释“为什么是这笔、为什么是这个比例”
- 减仓 / 卖出要解释“为什么现在，而不是继续等待”

## 9.3 复盘 Prompt 模板

### 输入上下文

- 当前持仓摘要
- 历史成交与最近三笔关键动作
- 原始进场决策
- 最近二次分析记录
- 最近买卖决策记录
- 财报与公司画像
- 市场技术、情绪、新闻与市场背景
- 周期类型 `general/weekly/monthly/quarterly`

### 输出要求

必须输出：

1. `performance_summary`
2. `execution_summary`
3. `risk_summary`
4. `discipline_summary`
5. `next_action_summary`
6. `conclusion_tag`
7. 固定 4 个 Tabs：
   - `执行与卖出复盘`
   - `结果复盘`
   - `方法与纪律`
   - `后续动作`

### 约束

- 必须同时看到“结果”和“过程”
- 不能只复述盈亏
- 每个 Tab 都要输出顶部结论和底部证据列表
- `后续动作` 必须综合前三个 Tabs，并体现 `conclusion_tag`
- 必须通过 Tool Calling 返回结构化对象，不能只输出自然语言 JSON 片段

---

## 10. AI 使用原则

1. AI 负责生成结构化草案，不替代最终拍板
2. 所有关键动作都允许人工修改、驳回或补充
3. AI 输出应同时保留结构化字段和原始结果快照
4. AI 结论必须可追溯到输入上下文
5. 同一类动作的 Prompt 模板应固定，避免输出格式漂移

---

## 11. 后续实现建议

1. 先实现统一历史记录查询 API
2. 再实现持仓列表摘要 API 与批次明细 API
3. 然后实现二次分析和买卖决策异步生成 API
4. 最后实现统一复盘生成 API（由 `review_type` 区分通用 / 周 / 月 / 季度）

这样可以优先支撑当前“文档 + 导航 + 原型页”方案向真实页面过渡。

## 4.2 `POST /api/trading-decision/stock-analysis-records`

用途：保存结构化二次分析记录；当请求带 `holding_stock_id` 或 `analysis_scene='holding_reanalysis'` 时，按持仓再评估场景写入。

### 当前已支持字段

- `holding_stock_id`
- `watch_stock_id`（若持仓有关联关注股票则自动回填）
- `analysis_scene = holding_reanalysis`
- `trade_date`
- `raw_result`

### 当前写入规则

- 记录落入 `stock_analysis_records`
- `holding_stock_id` 与 `watch_stock_id` 可同时存在
- 后端会补充 `holding_reanalysis_context`
- 后端会生成固定五个 tabs：
  - `基本面变化`
  - `估值与交易拥挤度`
  - `风险与催化`
  - `市场情绪`
  - `调整建议`
- 保存成功后回填持仓摘要：
  - `suggested_action = conclusion_summary`
  - `last_review_at = trade_date`

## 4.3 `GET /api/trading-decision/stock-analysis-records?holding_stock_id=...`

用途：查询单只持仓的二次分析历史。

### 查询条件

- `holding_stock_id`
- `limit`

---


## 6. 复盘 API

复盘已经从静态原型页切换为真实业务页：

- 页面入口：`GET /holding-review?holding_stock_id=...&record_id=...`
- 模板：`templates/holding_review.html`
- 路由：`src/stock_analyse/interfaces/web/routes/trading_decision.py`
- 服务装配：`TradingDecisionService.build_holding_review_context()`
- AI 编排：`HoldingReviewOrchestrator`
- 存储：`holding_review_records`
- 页面形态：SSE 运行日志 + 固定 4 Tabs + 历史回放

AI 角色固定为：`交易专家`

真实输入不是一个单独的“复盘说明文本”，而是当前系统已存在的持仓上下文：

- `holding_stock`
- `watch_stock`
- `trade_history_context.trades`
- `trade_history_context.lots`
- `trade_history_context.recent_trade_steps`
- `entry_context.latest_entry_decision / entry_decision_history`
- `reanalysis_context.latest_reanalysis / reanalysis_history`
- `position_decision_context.latest_position_decision / position_decision_history`
- `financial_context.company_profile / financial_indicators / reports`
- `market_context.technical / sentiment / market_context / news`
- `review_focus_context`

## 6.1 `GET /holding-review?holding_stock_id=...&record_id=...`

用途：打开真实持仓复盘页面，并在同一页面里完成生成、保存、历史回放。

### 查询参数

- `holding_stock_id`：必填
- `record_id`：选填，用于历史回放

### 页面能力

- 显示当前持仓摘要、最近二次分析、最近买卖决策
- 通过 SSE 展示运行状态、进度、日志
- 固定展示 4 个 Tabs：
  1. `执行与卖出复盘`
  2. `结果复盘`
  3. `方法与纪律`
  4. `后续动作`
- 历史记录与实时生成共用同一套渲染结构

## 6.2 `POST /api/trading-decision/holding-stocks/<id>/reviews/run`

用途：发起持仓复盘草案生成。

### 请求体

```json
{
  "trade_date": "2026-04-29",
  "review_type": "weekly",
  "period_key": "2026-W18",
  "analysis_depth": "deep",
  "client_id": "holding_review_123"
}
```

### 返回摘要

```json
{
  "success": true,
  "data": {
    "status": "running",
    "task_mode": "async",
    "client_id": "holding_review_123",
    "holding_review_context": {
      "holding_stock_id": "HS-20260429-0001",
      "watch_stock_id": "WS-20260425-0001",
      "stock_code": "600519",
      "stock_name": "贵州茅台",
      "market": "A股",
      "trade_date": "2026-04-29",
      "review_type": "weekly",
      "period_key": "2026-W18",
      "analysis_depth": "deep",
      "role": "交易专家",
      "data_sources": [
        "trade_history_context",
        "entry_context",
        "reanalysis_context",
        "position_decision_context",
        "financial_context",
        "market_context"
      ]
    }
  },
  "message": "持仓复盘任务已启动"
}
```

### 结构化输出协议

AI 必须通过 Tool Calling 输出对象，固定字段包括：

- `performance_summary`
- `execution_summary`
- `risk_summary`
- `discipline_summary`
- `next_action_summary`
- `conclusion_tag`
- `tabs`

`conclusion_tag` 允许值：

- `logic_ok`
- `need_recheck`
- `execution_issue`
- `risk_rising`
- `prepare_reduce`
- `prepare_sell`

`tabs` 必须固定 4 个，顺序固定：

1. `execution_review / 执行与卖出复盘`
2. `result_review / 结果复盘`
3. `discipline_review / 方法与纪律`
4. `next_action / 后续动作`

规则：

- 四个 Tabs 必须固定存在，顺序固定
- 每个 Tab 先给顶部结论 `summary`，再给底部证据 `evidence`
- 最后一个 `后续动作` Tab 必须综合前三项与 `conclusion_tag`

## 6.3 `POST /api/trading-decision/holding-review-records`

用途：手动保存持仓复盘记录。

### 请求体

```json
{
  "holding_stock_id": "HS-20260425-0001",
  "trade_date": "2026-04-29",
  "review_type": "weekly",
  "period_key": "2026-W18",
  "analysis_depth": "deep",
  "raw_result": {"success": true, "data": {}}
}
```

### 落库字段

- `holding_stock_id`
- `watch_stock_id`
- `stock_code`
- `stock_name`
- `market`
- `trade_date`
- `review_type`
- `period_key`
- `analysis_depth`
- `performance_summary`
- `execution_summary`
- `risk_summary`
- `discipline_summary`
- `next_action_summary`
- `conclusion_tag`
- `tabs_json`
- `evidence_json`
- `context_snapshot_json`
- `raw_result_json`

### 保存后的摘要回填

- `holding_stocks.suggested_action = next_action_summary`
- `holding_stocks.last_review_at = trade_date`

## 6.4 `GET /api/trading-decision/holding-review-records?holding_stock_id=...`

用途：查询单只持仓的复盘历史。

### 查询条件

- `holding_stock_id`
- `limit`

## 6.5 `GET /api/trading-decision/holding-review-records/<record_id>`

用途：获取某条持仓复盘详情，用于页面回放与历史详情展示。

---

## 7. 持仓历史记录统一查询 API

## 7.1 `GET /api/trading-decision/holding-records`

用途：为持仓主页面下半区的 Tab + 列表提供统一数据源。

### 查询参数

- `record_type`
  - `reanalysis`
  - `add_decision`
  - `reduce_decision`
  - `sell_decision`
  - `review`
- `stock_code`
- `keyword`
- `risk_status`
- `conclusion_tag`
- `review_type`
- `date_from`
- `date_to`
- `page`
- `page_size`

### 返回示例

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "record_type": "review",
        "record_id": "HR-20260425-015",
        "stock_code": "300750",
        "stock_name": "宁德时代",
        "title": "宁德时代 - 周复盘",
        "summary": "逻辑仍成立，继续持有并跟踪第二笔条件。",
        "conclusion_tag": "logic_ok",
        "created_at": "2026-04-25 10:18"
      }
    ],
    "pagination": {
      "page": 1,
      "page_size": 10,
      "total": 32
    }
  },
  "message": "ok"
}
```

---

## 8. AI 角色设计

## 8.1 持仓分析师

### 职责

- 判断原买入逻辑是否仍成立
- 提取基本面、行业、财务、新闻、技术、估值的关键变化
- 输出风险等级与计划影响

### 不负责

- 直接替用户做最终交易决定

## 8.2 仓位决策师

### 职责

- 面向补仓 / 减仓 / 卖出输出结构化建议
- 明确触发条件、建议比例、执行模式与不执行风险

### 不负责

- 代替人工最终确认执行

## 8.3 交易专家

### 职责

- 在统一复盘页中生成通用 / 周 / 月 / 季度复盘草稿
- 同时评价结果、过程、风险变化、纪律表现与下阶段动作
- 输出固定 4 Tabs 与结构化结论标签，供页面直接回放与历史落库

### 不负责

- 夸大收益或弱化错误
- 跳过执行过程只复述盈亏结果

---

## 9. AI Prompt 模板设计

## 9.1 二次分析 Prompt 模板

### 输入上下文

- 持仓摘要
- 持仓均价与当前价格
- 买入批次与成交明细
- 原进场决策摘要
- 历史分析记录摘要
- 最近报表 / 新闻 / 技术结论

### 输出要求

必须输出：

1. 原逻辑是否仍成立
2. 关键变化项
3. 风险等级变化
4. 对原计划的影响
5. 后续动作建议
6. 一句话结论

### 约束

- 不直接说“立即买卖”
- 结论必须回到持仓上下文
- 需要指出不确定性来源

## 9.2 买卖决策 Prompt 模板

### 输入上下文

- 持仓摘要
- 成本结构
- 买卖历史
- 原进场决策
- 历史再评估记录
- 当前阶段 / 区间 / 风险状态
- 当前市场与基本面变化
- `decision_type`

### 输出要求

必须输出：

1. 触发条件
2. 核心理由
3. 建议动作
4. 建议比例 / 数量
5. 不执行风险
6. 执行注意事项
7. 一句话结论

### 约束

- 不输出带强制口吻的下单命令
- 补仓要解释“为什么是这笔、为什么是这个比例”
- 减仓 / 卖出要解释“为什么现在，而不是继续等待”

## 9.3 复盘 Prompt 模板

### 输入上下文

- 当前持仓摘要
- 历史成交与最近三笔关键动作
- 原始进场决策
- 最近二次分析记录
- 最近买卖决策记录
- 财报与公司画像
- 市场技术、情绪、新闻与市场背景
- 周期类型 `general/weekly/monthly/quarterly`

### 输出要求

必须输出：

1. `performance_summary`
2. `execution_summary`
3. `risk_summary`
4. `discipline_summary`
5. `next_action_summary`
6. `conclusion_tag`
7. 固定 4 个 Tabs：
   - `执行与卖出复盘`
   - `结果复盘`
   - `方法与纪律`
   - `后续动作`

### 约束

- 必须同时看到“结果”和“过程”
- 不能只复述盈亏
- 每个 Tab 都要输出顶部结论和底部证据列表
- `后续动作` 必须综合前三个 Tabs，并体现 `conclusion_tag`
- 必须通过 Tool Calling 返回结构化对象，不能只输出自然语言 JSON 片段

---

## 10. AI 使用原则

1. AI 负责生成结构化草案，不替代最终拍板
2. 所有关键动作都允许人工修改、驳回或补充
3. AI 输出应同时保留结构化字段和原始结果快照
4. AI 结论必须可追溯到输入上下文
5. 同一类动作的 Prompt 模板应固定，避免输出格式漂移

---

## 11. 后续实现建议

1. 先实现统一历史记录查询 API
2. 再实现持仓列表摘要 API 与批次明细 API
3. 然后实现二次分析和买卖决策异步生成 API
4. 最后实现统一复盘生成 API（由 `review_type` 区分通用 / 周 / 月 / 季度）

这样可以优先支撑当前“文档 + 导航 + 原型页”方案向真实页面过渡。
