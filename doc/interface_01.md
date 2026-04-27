# 关注股票列表及子页面 API 接口文档

本文档基于当前仓库的**实际运行代码**整理，覆盖以下页面：

- `/watch-stocks` 关注股票列表页
- `/entry-decision` 进场决策页
- `/stock-analysis-record` 股票分析记录页
- `/trade-plan-analysis` 持仓计划分析页

文档重点包含：

1. 每个页面对应的 API 接口
2. 每个接口的协议说明
3. 每个接口内部 AI 交互逻辑步骤
4. 每个接口返回内容与 Web 界面要素的映射关系

---

## 1. 总体说明

### 1.1 统一返回结构

本组接口当前主要有两类返回风格：

#### A. 交易决策域接口
路径前缀主要为：
- `/api/trading-decision/watch-stocks...`
- `/api/trading-decision/trade-plan-analysis-records...`

成功返回：

```json
{
  "success": true,
  "data": {},
  "message": "..."
}
```

失败返回：

```json
{
  "success": false,
  "message": "错误消息",
  "error": {
    "code": "bad_request | not_found | ...",
    "message": "错误消息"
  }
}
```

#### B. AI 分析通道接口
主要为：
- `/api/analyze_stock_ai`
- `/api/sse`

其中：
- `/api/analyze_stock_ai` 负责**启动异步 AI 分析任务**
- `/api/sse` 负责**流式接收日志、进度、最终结果**

---

### 1.2 AI 交互底座

当前多个子页面复用同一套 AI 异步分析底座，核心入口位于：

- `src/stock_analyse/interfaces/web/routes/analysis.py:73`
- `src/stock_analyse/interfaces/web/routes/analysis.py:146`
- `src/stock_analyse/interfaces/web/routes/analysis.py:169`

核心机制如下：

1. 页面先生成 `client_id`
2. 页面先连 `/api/sse?client_id=...`
3. 页面再调用某个 run/analyze API
4. run/analyze API 内部调用 `build_stock_ai_payload(...)`
5. 再调用 `start_stock_ai_analysis(...)`
6. 后台异步线程执行：
   - `context.analyzer.stock_ai_analysis_process(...)`
7. 后台通过 SSE 持续发送：
   - `log`
   - `singleProgress`
   - `final_result`
   - `completion`
   - `error`

因此，凡是“生成决策”“生成计划草案”“开始股票分析”这类按钮，其 AI 交互模式本质一致，只是：
- 启动入口不同
- 页面消费 `final_result` 的方式不同
- 是否把结果落库不同

---

## 2. 页面一：`/watch-stocks` 关注股票列表

页面路由：
- `GET /watch-stocks`
- `GET /index`

后端入口：
- `src/stock_analyse/interfaces/web/routes/trading_decision.py:50`

前端模板：
- `templates/watch_stocks.html`

---

### 2.1 页面加载接口：`GET /api/trading-decision/watch-stocks`

#### 接口协议

- **Method**: `GET`
- **Path**: `/api/trading-decision/watch-stocks`
- **Query 参数**:
  - `keyword`
  - `market`
  - `asset_type`
  - `stage`
  - `price_zone`
  - `status`
  - `page`
  - `page_size`

#### 返回示例

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "id": "WS-XXXX",
        "stock_code": "600519",
        "stock_name": "贵州茅台",
        "market": "A股",
        "industry": "白酒",
        "asset_type": "成长龙头",
        "current_price": 1688.0,
        "pe": 29.6,
        "current_stage": "准备建仓",
        "current_price_zone": "合理区",
        "suggested_action": "适合买入",
        "last_conclusion_summary": "...",
        "last_analysis_at": "2026-04-27"
      }
    ],
    "summary": {
      "watch_count": 10,
      "decision_ready_count": 3,
      "analysis_completed_count": 5,
      "planned_count": 2
    },
    "pagination": {
      "page": 1,
      "page_size": 20,
      "total": 10
    },
    "filters": {
      "keyword": "",
      "market": "",
      "asset_type": "",
      "stage": "",
      "price_zone": "",
      "status": "",
      "page": 1,
      "page_size": 20
    }
  },
  "message": ""
}
```

#### 内部 AI 交互逻辑步骤

该接口**不触发 AI**。

内部步骤：

1. 读取 query 参数
2. `TradingDecisionService.list_watch_stocks(...)` 做参数归一化
3. `WatchStockRepository.list(...)` 查询 SQLite `watch_stocks`
4. `WatchStockRepository.get_summary_counts()` 生成顶部统计卡数据
5. 返回前端列表、统计、分页、筛选条件

#### 返回内容与页面要素映射

| 返回字段 | 页面要素 |
|---|---|
| `data.summary.watch_count` | 顶部统计卡“关注数量” |
| `data.summary.decision_ready_count` | 顶部统计卡“决策就绪数” |
| `data.summary.analysis_completed_count` | 顶部统计卡“分析完成数” |
| `data.summary.planned_count` | 顶部统计卡“计划完成数/计划相关统计” |
| `data.items[].stock_name + stock_code` | 表格“标的”列 |
| `data.items[].market + last_analysis_at` | 表格“标的”列副文案 |
| `data.items[].current_price` | 表格“价格 / 估值”列 |
| `data.items[].pe` | 表格“价格 / 估值”列中的 PE |
| `data.items[].industry` | 表格“行业 / 类型”列 |
| `data.items[].asset_type` | 表格“行业 / 类型”列副文案 |
| `data.items[].current_stage` | 表格“阶段 / 区间”列标签 |
| `data.items[].current_price_zone` | 表格“阶段 / 区间”列标签 |
| `data.items[].suggested_action` | 表格“当前建议”列 |
| `data.items[].status` | 表格“当前建议”列副文案 |
| `data.items[].last_conclusion_summary` | 表格“最新结论”列 |
| `data.items[].source` | 表格“最新结论”列副文案 |
| `data.pagination.*` | 表格下方分页信息 |

---

### 2.2 股票检索接口：`GET /api/trading-decision/watch-stocks/stock-search`

#### 接口协议

- **Method**: `GET`
- **Path**: `/api/trading-decision/watch-stocks/stock-search`
- **Query 参数**:
  - `query`: 股票代码或名称关键字
  - `market`: 市场，如 `A股` / `H` / `usa`
  - `limit`: 返回条数，默认 20

#### 返回示例

```json
{
  "success": true,
  "data": [
    {
      "code": "600519",
      "name": "贵州茅台",
      "market": "A股",
      "display_label": "600519 - 贵州茅台 (A股)",
      "source": "spot",
      "current_price": 1688.0,
      "pe": 29.6
    }
  ],
  "message": ""
}
```

#### 内部 AI 交互逻辑步骤

该接口**不触发 AI**。

内部步骤：

1. 读取 `query/market/limit`
2. `TradingDecisionService.search_stock_candidates(...)`
3. 调用 `stockBorderInfo(...).get_stock_spot()`
4. 只从实时 spot 数据中筛选股票代码和名称
5. 提取适合的价格列与 PE 列
6. 返回用于前端下拉搜索结果的轻量数据结构

#### 返回内容与页面要素映射

| 返回字段 | 页面要素 |
|---|---|
| `data[].display_label` | 新增/编辑弹窗中的搜索结果列表文案 |
| `data[].code` | 选中后回填“股票代码” |
| `data[].name` | 选中后回填“股票名称” |
| `data[].market` | 选中后回填“市场” |
| `data[].current_price` | 选中后回填“当前价格” |
| `data[].pe` | 选中后回填“PE” |
| `data[].source` | 作为来源标记，供表单内部状态或后续展示使用 |

---

### 2.3 新增关注股票接口：`POST /api/trading-decision/watch-stocks`

#### 接口协议

- **Method**: `POST`
- **Path**: `/api/trading-decision/watch-stocks`
- **Body(JSON)**:

```json
{
  "stock_code": "600519",
  "stock_name": "贵州茅台",
  "market": "A股",
  "industry": "白酒",
  "asset_type": "成长龙头",
  "source": "spot",
  "note": "长期观察",
  "current_price": 1688.0,
  "pe": 29.6
}
```

#### 内部 AI 交互逻辑步骤

该接口**不触发 AI**。

内部步骤：

1. 校验必填字段：`stock_code/stock_name/market/asset_type`
2. `TradingDecisionService.create_watch_stock(...)`
3. `WatchStockRepository.create(...)`
4. 写入 SQLite `watch_stocks`
5. 返回新建记录

#### 返回内容与页面要素映射

| 返回字段 | 页面要素 |
|---|---|
| `data.id` | 后续编辑、归档、子页面跳转使用 |
| `data.stock_code ~ data.pe` | 新增成功后刷新主列表行数据 |
| `data.note/source` | 用于编辑回填和列表副文案 |

---

### 2.4 读取单条关注股票：`GET /api/trading-decision/watch-stocks/<watch_stock_id>`

#### 接口协议

- **Method**: `GET`
- **Path**: `/api/trading-decision/watch-stocks/<watch_stock_id>`

#### 内部 AI 交互逻辑步骤

该接口**不触发 AI**。

内部步骤：

1. 根据 ID 读取 `watch_stocks`
2. 找不到返回 `404 not_found`
3. 返回单条记录

#### 返回内容与页面要素映射

| 返回字段 | 页面要素 |
|---|---|
| 全量 `data.*` | 编辑弹窗打开时的表单回填 |

---

### 2.5 更新关注股票：`PUT /api/trading-decision/watch-stocks/<watch_stock_id>`

#### 接口协议

- **Method**: `PUT`
- **Path**: `/api/trading-decision/watch-stocks/<watch_stock_id>`
- **Body(JSON)**: 可传部分字段

#### 内部 AI 交互逻辑步骤

该接口**不触发 AI**。

内部步骤：

1. 读取现有记录
2. 将请求字段与旧值合并
3. `WatchStockRepository.update(...)`
4. 更新 SQLite `watch_stocks`
5. 返回更新后的记录

#### 返回内容与页面要素映射

| 返回字段 | 页面要素 |
|---|---|
| `data.current_stage` | 列表“阶段 / 区间”列；进场决策页保存后的回写字段 |
| `data.current_price_zone` | 列表“阶段 / 区间”列 |
| `data.suggested_action` | 列表“当前建议”列 |
| `data.last_conclusion_summary` | 列表“最新结论”列 |
| `data.last_analysis_at` | 列表“标的”列中的最近分析时间 |
| 其余更新字段 | 编辑弹窗保存结果、列表刷新 |

---

### 2.6 归档关注股票：`POST /api/trading-decision/watch-stocks/<watch_stock_id>/archive`

#### 接口协议

- **Method**: `POST`
- **Path**: `/api/trading-decision/watch-stocks/<watch_stock_id>/archive`

#### 内部 AI 交互逻辑步骤

该接口**不触发 AI**。

内部步骤：

1. 根据 ID 查记录
2. 将 `status` 更新为 `archived`
3. 返回更新后的记录

#### 返回内容与页面要素映射

| 返回字段 | 页面要素 |
|---|---|
| `data.status=archived` | 前端刷新后该条默认从主列表消失 |

---

## 3. 页面二：`/entry-decision` 进场决策

页面路由：
- `GET /entry-decision?watch_stock_id=<id>`

后端入口：
- `src/stock_analyse/interfaces/web/routes/trading_decision.py:57`

前端模板：
- `templates/entry_decision.html`

---

### 3.1 页面加载路由：`GET /entry-decision?watch_stock_id=<id>`

#### 接口协议

- **Method**: `GET`
- **Path**: `/entry-decision`
- **Query 参数**:
  - `watch_stock_id` 必填

#### 内部 AI 交互逻辑步骤

该接口**不触发 AI**。

内部步骤：

1. 校验 `watch_stock_id`
2. `TradingDecisionService.build_entry_decision_page_data(...)`
3. 读取目标 watch stock
4. 构造页面默认值：
   - `trade_date`
   - `analysis_depth`
   - `current_stage`
   - `current_price_zone`
   - `suggested_action`
   - `last_conclusion_summary`
5. 渲染页面模板

#### 返回内容与页面要素映射

| 返回字段 | 页面要素 |
|---|---|
| `watch_stock.stock_name/stock_code` | 页头与标的上下文 |
| `watch_stock.industry/asset_type/current_price/pe` | 标的上下文卡片 |
| `form_defaults.trade_date` | “交易日期”默认值 |
| `form_defaults.analysis_depth` | “分析深度”默认值 |
| `form_defaults.current_stage` | 保存区“当前阶段”默认值 |
| `form_defaults.current_price_zone` | 保存区“价格区间”默认值 |
| `form_defaults.suggested_action` | 保存区“当前建议”默认值 |
| `form_defaults.last_conclusion_summary` | 保存区“最新结论摘要”默认值 |

---

### 3.2 生成决策接口：`POST /api/trading-decision/watch-stocks/<id>/entry-decision/analyze`

#### 接口协议

- **Method**: `POST`
- **Path**: `/api/trading-decision/watch-stocks/<watch_stock_id>/entry-decision/analyze`
- **Body(JSON)**:

```json
{
  "trade_date": "2026-04-27",
  "analysis_depth": "standard",
  "client_id": "entry_client_xxx",
  "current_stage": "观察中",
  "current_price_zone": "合理区",
  "suggested_action": "继续观察",
  "last_conclusion_summary": "手工备注"
}
```

#### 返回示例

```json
{
  "success": true,
  "data": "",
  "message": "股票 600519 AI分析已启动",
  "task_mode": "async",
  "client_id": "entry_client_xxx",
  "entry_decision_context": {
    "watch_stock_id": "WS-XXXX",
    "stock_code": "600519",
    "stock_name": "贵州茅台",
    "market": "A股",
    "trade_date": "2026-04-27",
    "analysis_depth": "standard",
    "pending_save_fields": {
      "current_stage": "观察中",
      "current_price_zone": "合理区",
      "suggested_action": "继续观察",
      "last_conclusion_summary": "手工备注"
    },
    "generated_summary_fields": {
      "suggested_action": "",
      "last_conclusion_summary": "",
      "last_analysis_at": "2026-04-27"
    }
  }
}
```

#### 内部 AI 交互逻辑步骤

这是一个**AI 异步任务启动接口**。

#### A. 启动阶段（HTTP 同步部分）

1. 校验 `watch_stock_id`
2. 从 `watch_stocks` 读取真实 `stock_code/market`
3. 读取页面传入的：
   - `trade_date`
   - `analysis_depth`
   - `client_id`
   - 当前保存草稿字段
4. 调用 `build_stock_ai_payload(...)`
5. 调用 `start_stock_ai_analysis(...)`
6. `start_stock_ai_analysis(...)` 内部：
   1. 建立任务锁，避免同一股票重复分析
   2. 提交异步任务到 `executor`
   3. 创建 `StreamingAnalyzer(client_id, sse_manager)`
   4. 调用 `context.analyzer.stock_ai_analysis_process(...)`
   5. 通过 SSE 发出：
      - `log`
      - `singleProgress`
      - `final_result`
      - `completion`
      - `error`
7. HTTP 接口本身立即返回“任务已启动”

#### B. AI 实际交互次数

正常路径下，这个接口背后会触发 **8 次 LLM/AI 交互**，顺序如下：

1. 市场分析师（MarketAnalyst）
2. 基本面分析师（FundamentalsAnalyst）
3. 新闻分析师（NewsAnalyst）
4. 看多研究员（BullResearcher）
5. 看空研究员（BearResearcher）
6. 研究经理（ResearchManager）
7. 风险经理（RiskManager）
8. 交易员（TraderAgent）

其中：
- 第 1~3 次通常并行执行
- 第 4~5 次基于分析师输出继续执行
- 第 6~7 次基于多空研究结果继续执行
- 第 8 次基于经理层结论输出最终交易建议

#### C. 每次 AI 交互的公共请求提示语模板

当前 8 次 AI 调用都复用同一个系统角色约束与同一个用户提示模板。

**系统提示语（system prompt）**：

```text
你是严谨的中文股票分析师，请基于提供的数据做结构化分析，不要编造未给出的事实。
```

**用户提示语（user prompt）模板**：

```text
角色: {role_name}
任务说明:
{instruction}

请严格输出 JSON 对象，字段为: summary, signals, risks, confidence, evidence。

上下文数据:
{JSON序列化后的context}
```

其中：
- `{role_name}` 由不同 AI 角色替换
- `{instruction}` 由不同 AI 角色的专属任务说明替换
- `{context}` 是当前阶段传给该角色的结构化上下文数据

#### D. 每次 AI 交互的请求提示语、输入上下文、应答内容

##### 第 1 次：市场分析师（MarketAnalyst）

- **角色名**：`市场分析师`
- **任务说明**：

```text
请基于市场快照、技术指标、价格波动和板块信息，判断短期趋势、支撑压力位、潜在催化与交易风险。
```

- **主要输入上下文**：来自股票快照数据，通常包含：
  - `stock_code`
  - `market`
  - `trade_date`
  - `date_range`
  - `market_context`
  - `technical`
  - `fund_flow`
  - `sentiment`
  - `industry`
  - `concepts`
- **应答内容要求**：严格输出 JSON，对象字段为：
  - `summary`
  - `signals`
  - `risks`
  - `confidence`
  - `evidence`
- **应答含义**：输出市场/技术面结论，供后续研究阶段使用

##### 第 2 次：基本面分析师（FundamentalsAnalyst）

- **角色名**：`基本面分析师`
- **任务说明**：

```text
请基于公司资料、主营业务、财务指标和财报摘要，评估盈利质量、成长性、估值合理性与中期基本面风险。
```

- **主要输入上下文**：来自股票快照数据，通常包含：
  - `stock_code`
  - `market`
  - `trade_date`
  - `company_profile`
  - `company_name`
  - `business_intro`
  - `financial_indicators`
  - `reports`
  - `industry`
- **应答内容要求**：严格输出 JSON，对象字段为：
  - `summary`
  - `signals`
  - `risks`
  - `confidence`
  - `evidence`
- **应答含义**：输出盈利质量、成长性、估值判断与基本面风险

##### 第 3 次：新闻分析师（NewsAnalyst）

- **角色名**：`新闻分析师`
- **任务说明**：

```text
请基于近期香港/美股/A股相关新闻与情绪数据，判断短期催化、利空、舆情方向和消息可信度。
```

- **主要输入上下文**：来自股票快照数据，通常包含：
  - `stock_code`
  - `market`
  - `trade_date`
  - `news`
  - `sentiment`
  - `market_context`
- **应答内容要求**：严格输出 JSON，对象字段为：
  - `summary`
  - `signals`
  - `risks`
  - `confidence`
  - `evidence`
- **应答含义**：输出消息面催化、负面新闻、情绪方向与可信度判断

##### 第 4 次：看多研究员（BullResearcher）

- **角色名**：`看多研究员`
- **任务说明**：

```text
请仅基于 analyst_outputs 中的证据，为看多立场构建最强论据，强调上涨驱动、估值修复或短期催化。
```

- **主要输入上下文**：
  - `analyst_outputs.market`
  - `analyst_outputs.fundamentals`
  - `analyst_outputs.news`
  - 原始请求中的股票基础信息
- **应答内容要求**：严格输出 JSON，对象字段为：
  - `summary`
  - `signals`
  - `risks`
  - `confidence`
  - `evidence`
- **应答含义**：从已有证据中提炼最强做多论据，不新增原始事实

##### 第 5 次：看空研究员（BearResearcher）

- **角色名**：`看空研究员`
- **任务说明**：

```text
请仅基于 analyst_outputs 中的证据，为看空立场构建最强论据，强调回撤风险、估值压力、兑现压力或消息不确定性。
```

- **主要输入上下文**：
  - `analyst_outputs.market`
  - `analyst_outputs.fundamentals`
  - `analyst_outputs.news`
  - 原始请求中的股票基础信息
- **应答内容要求**：严格输出 JSON，对象字段为：
  - `summary`
  - `signals`
  - `risks`
  - `confidence`
  - `evidence`
- **应答含义**：从已有证据中提炼最强看空论据，不新增原始事实

##### 第 6 次：研究经理（ResearchManager）

- **角色名**：`研究经理`
- **任务说明**：

```text
请综合多空研究观点，判断哪一方证据更强、分歧点在哪里，并给出中立研究结论。
```

- **主要输入上下文**：
  - `research_outputs.bull`
  - `research_outputs.bear`
  - `analyst_outputs.*`
  - 股票基础信息
- **应答内容要求**：严格输出 JSON，对象字段为：
  - `summary`
  - `signals`
  - `risks`
  - `confidence`
  - `evidence`
- **应答含义**：输出中立研究结论，指出证据强弱与主要分歧点

##### 第 7 次：风险经理（RiskManager）

- **角色名**：`风险经理`
- **任务说明**：

```text
请评估仓位风险、波动风险、消息不确定性和基本面证伪风险，并给出风险等级与仓位约束。
```

- **主要输入上下文**：
  - `research_outputs.bull`
  - `research_outputs.bear`
  - `analyst_outputs.*`
  - 股票基础信息
- **应答内容要求**：严格输出 JSON，对象字段为：
  - `summary`
  - `signals`
  - `risks`
  - `confidence`
  - `evidence`
- **应答含义**：输出风险等级、仓位边界与主要风控提示

##### 第 8 次：交易员（TraderAgent）

- **角色名**：`交易员`
- **任务说明**：

```text
请基于研究经理结论和风险经理约束，输出最终交易建议，包括 action、summary、仓位建议、主要证据和风险提醒。
```

- **主要输入上下文**：
  - `manager_outputs.research_manager`
  - `manager_outputs.risk_manager`
  - `research_outputs.*`
  - `analyst_outputs.*`
  - 股票基础信息
- **应答内容要求**：代码里仍先按统一解析框架接收，核心会沉淀为最终交易建议，最终页面主要消费：
  - `action`
  - `summary`
  - `risk_level`
  - `position_suggestion`
  - `logic`
  - `evidence`
  - `risks`
- **应答含义**：输出最终给用户展示和保存的交易建议

#### E. 每次 AI 应答的标准化结构

在运行时，每次 AI 返回内容会先被解析并标准化，常见标准化结果为：

```json
{
  "summary": "...",
  "signals": [],
  "risks": [],
  "confidence": 0.0,
  "evidence": [],
  "raw_text": "...",
  "role": "..."
}
```

说明：
- `summary`：该角色的核心结论摘要
- `signals`：该角色认为关键的正负信号
- `risks`：该角色识别的风险项
- `confidence`：该角色结论置信度
- `evidence`：支撑该角色结论的证据点
- `raw_text`：原始模型文本，便于排障
- `role`：角色名，由系统补充

#### F. AI 结果如何逐层传递

1. **分析师层**：3 个分析师分别从市场、基本面、新闻三个维度产出 `analyst_outputs`
2. **研究层**：看多/看空研究员只消费 `analyst_outputs`，形成 `research_outputs`
3. **经理层**：研究经理与风险经理消费 `research_outputs + analyst_outputs`，形成 `manager_outputs`
4. **交易层**：交易员消费 `manager_outputs + research_outputs + analyst_outputs`，形成最终 `decision`
5. **页面展示层**：前端收到 SSE `final_result` 后，从 `decision/final_state/scores/signals/risks/evidence` 中抽取结果渲染页面

#### G. 当前页面展示与 AI 原始输出的关系

当前页面并不是把 8 次 AI 的原始返回逐条完整展示出来，而是做了两层收敛：

1. **主结果区**：优先展示最终交易员结论 `decision.*`
2. **扩展分析区**：展示 `final_state.analyst_outputs.*`、`final_state.research_outputs.*`、`final_state.manager_outputs.*`、`final_state.trader_output`

因此：
- AI 实际输出信息量大于页面主结果区展示量
- 没有完全丢失，较多内容被收纳在 `final_state`、`evidence`、`signals`、`risks` 等结构中
- 页面主区是“决策摘要视图”，不是“8 次原始 AI 响应逐条全文视图”

#### AI 最终结果的数据结构

`final_result` 事件中的 `data` 当前核心包含：

- `stock_code`
- `market`
- `trade_date`
- `analysis_mode`
- `decision`
- `final_state`
- `scores`
- `signals`
- `risks`
- `evidence`
- `stance`
- `logic`
- `position_suggestion`
- `time_horizon`
- `meta`
- `snapshot`

#### 返回内容与页面要素映射

##### A. 启动接口即时返回与页面映射

| 返回字段 | 页面要素 |
|---|---|
| `task_mode=async` | 页面进入“等待异步结果”状态 |
| `client_id` | SSE 关联通道 |
| `entry_decision_context.watch_stock_id` | 页面内部状态绑定当前关注股票 |
| `entry_decision_context.pending_save_fields.*` | 分析完成后回填保存区默认值 |

##### B. `final_result.data` 与页面结果区映射

| 返回字段 | 页面要素 |
|---|---|
| `decision.action` | 决策动作 |
| `decision.summary` | 决策摘要 |
| `decision.confidence` | 置信度 |
| `decision.risk_level` | 风险等级 |
| `decision.logic` / `logic` | 决策逻辑 |
| `position_suggestion` / `decision.position_suggestion` | 仓位与执行建议 |
| `time_horizon` | 时间周期 |
| `scores.technical/fundamental/sentiment/composite` | 评分概览 |
| `signals` | 关键信号 |
| `risks` | 主要风险 |
| `evidence` | 证据链 |
| `snapshot` | 关键信息快照 |
| `meta` | 执行元信息 |
| `final_state.analyst_outputs.*` | 市场/基本面/新闻分析师卡片 |
| `final_state.research_outputs.*` | 多空研究对照 |
| `final_state.manager_outputs.*` | 研究经理/风险经理结论 |
| `final_state.trader_output` | 交易员执行建议 |

---

### 3.3 保存回写接口：`PUT /api/trading-decision/watch-stocks/<id>`

#### 用途

进场决策页当前不单独新建 `decision_case` 表，而是把人工确认后的结果回写到 `watch_stocks`。

#### 接口协议

- **Method**: `PUT`
- **Path**: `/api/trading-decision/watch-stocks/<watch_stock_id>`
- **Body(JSON)**:

```json
{
  "current_stage": "准备建仓",
  "current_price_zone": "合理区",
  "suggested_action": "适合买入",
  "last_conclusion_summary": "技术面和基本面共振，可小仓位试错。",
  "last_analysis_at": "2026-04-27"
}
```

#### 内部 AI 交互逻辑步骤

该接口**不直接触发 AI**。

它消费的是上一步 AI 已经生成出来、并且用户已确认/微调后的结果。

内部步骤：

1. 根据 ID 读取 watch stock
2. 合并请求中的人工确认字段
3. 更新 SQLite `watch_stocks`
4. 返回更新后的记录

#### 返回内容与页面要素映射

| 返回字段 | 页面要素 |
|---|---|
| `data.current_stage` | 页面保存区“当前阶段”保存结果 |
| `data.current_price_zone` | 页面保存区“价格区间”保存结果 |
| `data.suggested_action` | 页面保存区“建议动作”保存结果 |
| `data.last_conclusion_summary` | 页面保存区“结论摘要”保存结果 |
| `data.last_analysis_at` | 页面保存区“分析时间”保存结果 |
| 同时返回到主列表后 | watch-stocks 主表格对应行同步更新 |

---

### 3.4 SSE 流式接口：`GET /api/sse?client_id=...`

#### 接口协议

- **Method**: `GET`
- **Path**: `/api/sse`
- **Query 参数**:
  - `client_id`

#### 内部 AI 交互逻辑步骤

1. 页面先建立 SSE 长连接
2. 后台异步分析过程中不断向该 `client_id` 发消息
3. 页面监听不同事件名并更新 UI

#### 事件与页面要素映射

| SSE 事件 | 页面要素 |
|---|---|
| `log` | 日志面板 |
| `singleProgress` | 进度条与状态文案 |
| `final_result` | 结果区整块渲染 |
| `completion` | 页面状态切换为完成 |
| `error` | 错误提示与日志 |

---

## 4. 页面三：`/stock-analysis-record` 股票分析记录

页面路由：
- `GET /stock-analysis-record`
- 可带 `watch_stock_id/code/market` 参数

前端模板：
- `templates/stock_analysis_record.html`

当前实现说明：
- 该页面**直接复用** `/api/analyze_stock_ai` 与 `/api/sse`
- 当前并**没有独立的** `/api/trading-decision/watch-stocks/<id>/stock-analysis/run` 落地接口

---

### 4.1 页面加载路由：`GET /stock-analysis-record`

#### 接口协议

- **Method**: `GET`
- **Path**: `/stock-analysis-record`
- **Query 参数**（前端会读取）：
  - `watch_stock_id`
  - `stock_code` 或 `code`
  - `market`

#### 内部 AI 交互逻辑步骤

该接口**不触发 AI**。

它只负责渲染页面模板，真正的分析由前端后续调用 `/api/analyze_stock_ai` 启动。

#### 返回内容与页面要素映射

| 来源 | 页面要素 |
|---|---|
| URL 参数中的 `watch_stock_id` | 页面头部 watch stock 上下文提示 |
| URL 参数中的 `stock_code/code` | 默认股票代码 |
| URL 参数中的 `market` | 默认市场 |

---

### 4.2 启动分析接口：`POST /api/analyze_stock_ai`

#### 接口协议

- **Method**: `POST`
- **Path**: `/api/analyze_stock_ai`
- **Body(JSON)**:

```json
{
  "stock_code": "600519",
  "market": "SH",
  "client_id": "analysis_client_xxx",
  "trade_date": "2026-04-27",
  "analysis_depth": "standard"
}
```

#### 返回示例

```json
{
  "success": true,
  "data": "",
  "message": "股票 600519 AI分析已启动",
  "task_mode": "async",
  "client_id": "analysis_client_xxx"
}
```

#### 内部 AI 交互逻辑步骤

这也是一个**AI 异步任务启动接口**。

内部步骤：

1. 解析并标准化请求体
2. 标准化 `market`：
   - `A股/CN/SH/SZ -> SH`
   - `港股/H/HK -> H`
   - `美股/US/usa -> usa`
3. 校验 `stock_code`
4. 调用 `_start_stock_ai_analysis_task(...)`
5. 创建异步分析任务
6. 调用 `context.analyzer.stock_ai_analysis_process(...)`
7. 通过 `/api/sse` 输出进度和结果

#### 返回内容与页面要素映射

##### A. 启动接口即时返回

| 返回字段 | 页面要素 |
|---|---|
| `task_mode=async` | 页面切换到运行态 |
| `client_id` | 供 SSE 订阅使用 |
| `message` | 日志面板启动消息 |

##### B. `final_result.data` 与页面元素映射

该页面会根据 `final_result` 的实际数据块**动态生成 Tab**，因此映射是“数据块到 Tab”。

| 返回字段 | 页面要素 |
|---|---|
| `decision` | 决策类 Tab |
| `scores` | 评分 Tab |
| `snapshot` | 快照 Tab |
| `meta` | 元信息 Tab |
| `final_state` | 深度结果 Tab / 调试信息 |
| 其他任意新增块 | 自动生成为新的结果 Tab |

---

### 4.3 SSE 接口：`GET /api/sse?client_id=...`

与进场决策页一致，主要映射为：

| SSE 事件 | 页面要素 |
|---|---|
| `log` | 运行日志 |
| `singleProgress` | 进度条 |
| `final_result` | 动态结果 Tab 区 |
| `completion` | 完成状态 |
| `error` | 错误提示 |

---

## 5. 页面四：`/trade-plan-analysis` 持仓计划分析

页面路由：
- `GET /trade-plan-analysis?watch_stock_id=<id>`
- 可选：`record_id=<id>`

后端入口：
- `src/stock_analyse/interfaces/web/routes/trading_decision.py:68`

前端模板：
- `templates/trade_plan_analysis.html`

---

### 5.1 页面加载路由：`GET /trade-plan-analysis?watch_stock_id=<id>&record_id=<id>`

#### 接口协议

- **Method**: `GET`
- **Path**: `/trade-plan-analysis`
- **Query 参数**:
  - `watch_stock_id` 必填
  - `record_id` 可选

#### 内部 AI 交互逻辑步骤

该接口**不触发 AI**。

内部步骤：

1. 校验 `watch_stock_id`
2. 读取对应 `watch_stocks`
3. 查询该股票的计划分析历史记录
4. 若带 `record_id`，则加载指定记录详情
5. 若不带 `record_id`，默认选中最近一条记录
6. 构造页面默认值：
   - `trade_date`
   - `plan_type`
   - `risk_preference`
   - `analysis_depth`
   - 最近计划摘要字段
7. 渲染页面模板

#### 返回内容与页面要素映射

| 返回字段 | 页面要素 |
|---|---|
| `watch_stock.stock_name/stock_code` | 页头与标的上下文 |
| `watch_stock.industry/asset_type/current_price/pe` | 标的上下文区 |
| `form_defaults.trade_date` | “交易日期”默认值 |
| `form_defaults.plan_type` | “计划类型”默认值 |
| `form_defaults.risk_preference` | “风险偏好”默认值 |
| `selected_record.*` | 当前展示的计划草案结果 |
| `history_items[]` | 页面下方历史记录列表 |

---

### 5.2 生成计划草案接口：`POST /api/trading-decision/watch-stocks/<id>/trade-plan-analysis/run`

#### 接口协议

- **Method**: `POST`
- **Path**: `/api/trading-decision/watch-stocks/<watch_stock_id>/trade-plan-analysis/run`
- **Body(JSON)**:

```json
{
  "trade_date": "2026-04-27",
  "plan_type": "三笔计划",
  "risk_preference": "中高风险",
  "analysis_depth": "standard",
  "client_id": "trade_plan_client_xxx"
}
```

#### 返回示例

```json
{
  "success": true,
  "data": "",
  "message": "股票 600519 AI分析已启动",
  "task_mode": "async",
  "client_id": "trade_plan_client_xxx",
  "trade_plan_analysis_context": {
    "watch_stock_id": "WS-XXXX",
    "stock_code": "600519",
    "stock_name": "贵州茅台",
    "market": "A股",
    "trade_date": "2026-04-27",
    "plan_type": "三笔计划",
    "risk_preference": "中高风险",
    "analysis_depth": "standard"
  }
}
```

#### 内部 AI 交互逻辑步骤

这是一个**AI 异步任务启动接口**。

内部步骤：

1. 校验 `watch_stock_id`
2. 从 `watch_stocks` 读取真实 `stock_code/market`
3. 读取页面参数：
   - `trade_date`
   - `plan_type`
   - `risk_preference`
   - `analysis_depth`
   - `client_id`
4. 调用 `build_stock_ai_payload(...)`
5. 调用 `start_stock_ai_analysis(...)`
6. 后台异步线程执行：
   - `context.analyzer.stock_ai_analysis_process(...)`
7. 后台通过 `/api/sse` 返回：
   - `log`
   - `singleProgress`
   - `final_result`
   - `completion`
   - `error`
8. 页面拿到 `final_result` 后，在前端将 AI 决策结果映射为计划草案展示块

#### 返回内容与页面要素映射

##### A. 启动接口即时返回

| 返回字段 | 页面要素 |
|---|---|
| `task_mode=async` | 页面进入运行态 |
| `client_id` | SSE 通道绑定 |
| `trade_plan_analysis_context.watch_stock_id` | 页面状态绑定 |
| `trade_plan_analysis_context.plan_type` | 当前计划类型上下文 |
| `trade_plan_analysis_context.risk_preference` | 当前风险偏好上下文 |

##### B. `final_result.data` 到计划草案结果区的映射

当前页面前端主要消费 `decision.position_suggestion` 与 `decision` 相关字段。

| 返回字段 | 页面要素 |
|---|---|
| `decision.position_suggestion.target_position` | 最大目标仓位、单票仓位上限 |
| `decision.position_suggestion.add_condition` | 补仓与加仓条件 |
| `decision.position_suggestion.reduce_condition` | 减仓条件 |
| `decision.position_suggestion.stop_loss_reference` | 卖出 / 止损规则 |
| `decision.risks[]` 或 `decision.risk_level` | 风险说明 |
| `decision.summary` 或 `decision.logic` | 结论摘要 |
| 整个 `final_result` | 原始 JSON 折叠区 |

---

### 5.3 保存计划分析记录：`POST /api/trading-decision/trade-plan-analysis-records`

#### 接口协议

- **Method**: `POST`
- **Path**: `/api/trading-decision/trade-plan-analysis-records`
- **Body(JSON)**:

```json
{
  "watch_stock_id": "WS-XXXX",
  "trade_date": "2026-04-27",
  "plan_type": "三笔计划",
  "risk_preference": "中高风险",
  "raw_result": {
    "success": true,
    "data": {
      "decision": {
        "action": "buy",
        "summary": "回踩后具备分批建仓条件。",
        "risk_level": "medium",
        "position_suggestion": {
          "target_position": "30%-50%",
          "add_condition": "放量突破关键压力位后分批加仓",
          "reduce_condition": "跌回突破位下方时减仓",
          "stop_loss_reference": "跌破最近关键支撑位时止损"
        }
      },
      "scores": {
        "composite": 75
      }
    }
  }
}
```

#### 内部 AI 交互逻辑步骤

该接口本身**不重新触发 AI**，而是消费上一步 AI 已返回的 `raw_result`。

内部步骤：

1. 校验 `watch_stock_id`
2. 读取 watch stock
3. 校验 `raw_result` 是对象
4. 调用 `TradingDecisionService.build_trade_plan_analysis_payload(...)`
5. 在服务层把 AI 原始结果结构化为记录字段：
   - `suggested_action`
   - `conclusion_summary`
   - `max_target_position`
   - `position_limit`
   - `entry_plan_json`
   - `add_position_rules`
   - `reduce_position_rules`
   - `sell_rules`
   - `risk_notes`
   - `raw_result_json`
6. `TradePlanAnalysisRecordRepository.create(...)` 保存到 SQLite `trade_plan_analysis_records`
7. 回写 `watch_stocks`：
   - `suggested_action`
   - `last_conclusion_summary`
   - `last_analysis_at`
8. 返回新记录

#### 返回内容与页面要素映射

| 返回字段 | 页面要素 |
|---|---|
| `data.id` | 页面保存成功后跳转 `record_id` |
| `data.max_target_position` | 当前结果区“最大目标仓位” |
| `data.position_limit` | 当前结果区“单票仓位上限” |
| `data.add_position_rules` | 当前结果区“补仓与加仓条件” |
| `data.reduce_position_rules` | 当前结果区“减仓条件” |
| `data.sell_rules` | 当前结果区“卖出/止损规则” |
| `data.risk_notes` | 当前结果区“风险说明” |
| `data.conclusion_summary` | 当前结果区“结论摘要” |
| `data.raw_result_json` | 原始 JSON 折叠区 |
| 同步回写的 watch_stocks 摘要字段 | watch-stocks 列表对应行显示 |

---

### 5.4 计划分析记录列表：`GET /api/trading-decision/trade-plan-analysis-records?watch_stock_id=...`

#### 接口协议

- **Method**: `GET`
- **Path**: `/api/trading-decision/trade-plan-analysis-records`
- **Query 参数**:
  - `watch_stock_id` 必填
  - `limit` 可选，默认 10

#### 内部 AI 交互逻辑步骤

该接口**不触发 AI**。

内部步骤：

1. 校验 `watch_stock_id`
2. 查询 `trade_plan_analysis_records`
3. 按 `created_at DESC` 返回列表
4. 将 JSON 字段反序列化后返回

#### 返回内容与页面要素映射

| 返回字段 | 页面要素 |
|---|---|
| `data[].id` | 历史记录卡片主键、详情跳转参数 |
| `data[].trade_date / created_at` | 历史记录时间 |
| `data[].risk_level` | 历史记录风险等级 |
| `data[].suggested_action` | 历史记录建议动作 |
| `data[].conclusion_summary` | 历史记录摘要 |

---

### 5.5 计划分析记录详情：`GET /api/trading-decision/trade-plan-analysis-records/<record_id>`

#### 接口协议

- **Method**: `GET`
- **Path**: `/api/trading-decision/trade-plan-analysis-records/<record_id>`

#### 内部 AI 交互逻辑步骤

该接口**不触发 AI**。

内部步骤：

1. 根据 `record_id` 查询记录
2. 若找不到则返回 `404 not_found`
3. 反序列化 `entry_plan_json/raw_result_json`
4. 返回单条详情

#### 返回内容与页面要素映射

| 返回字段 | 页面要素 |
|---|---|
| 全量 `data.*` | `/trade-plan-analysis?record_id=...` 详情回看页 |
| `data.entry_plan_json` | 计划步骤/评分扩展块 |
| `data.raw_result_json` | 原始 JSON 展示 |

---

### 5.6 SSE 流式接口：`GET /api/sse?client_id=...`

与进场决策、股票分析共用。

| SSE 事件 | 页面要素 |
|---|---|
| `log` | 运行日志区 |
| `singleProgress` | 进度条 |
| `final_result` | 计划草案结果区 |
| `completion` | 分析完成状态 |
| `error` | 失败提示 |

---

## 6. 页面与接口对应关系总表

| 页面 | 页面路由 | 主接口 | 是否触发 AI | 是否依赖 SSE |
|---|---|---|---|---|
| 关注股票列表 | `/watch-stocks` | `/api/trading-decision/watch-stocks` | 否 | 否 |
| 关注股票新增/编辑 | 同页弹窗 | `/api/trading-decision/watch-stocks`、`/api/trading-decision/watch-stocks/<id>`、`/archive`、`/stock-search` | 否 | 否 |
| 进场决策 | `/entry-decision?watch_stock_id=...` | `/api/trading-decision/watch-stocks/<id>/entry-decision/analyze`、`PUT /api/trading-decision/watch-stocks/<id>`、`/api/sse` | 是 | 是 |
| 股票分析记录 | `/stock-analysis-record` | `/api/analyze_stock_ai`、`/api/sse` | 是 | 是 |
| 持仓计划分析 | `/trade-plan-analysis?watch_stock_id=...` | `/api/trading-decision/watch-stocks/<id>/trade-plan-analysis/run`、`POST/GET /api/trading-decision/trade-plan-analysis-records...`、`/api/sse` | 是（run） | 是 |

---

## 7. 当前实现与技术方案文档差异说明

当前运行代码与 `doc/tec_trading_decision.md` 的目标方案相比，有以下差异：

1. **进场决策页**
   - 技术方案文档中规划的是 `ai-prefill + decision_cases` 体系
   - 当前实际实现是：
     - 直接调用 `/api/trading-decision/watch-stocks/<id>/entry-decision/analyze`
     - AI 结果直接在前端展示
     - 人工确认结果直接回写 `watch_stocks`
   - 即：当前是 **MVP 版轻量闭环**，未引入 `decision_cases` 表

2. **股票分析记录页**
   - 技术方案文档中规划了独立 `/api/trading-decision/watch-stocks/<id>/stock-analysis/run`
   - 当前实际实现仍直接复用 `/api/analyze_stock_ai`
   - 即：当前是 **页面已迁移，接口仍复用旧 AI 分析入口**

3. **持仓计划分析页**
   - 技术方案文档中规划 run 完成后由后台直接保存记录
   - 当前实际实现是：
     - 先 `run`
     - 前端收到 `final_result`
     - 用户再点击“保存计划分析记录”
     - 调用 `POST /api/trading-decision/trade-plan-analysis-records`
   - 即：当前是 **前端二阶段保存模型**，更适合用户确认后再落库

---

## 8. 建议的后续补充方向

1. 为 `stock-analysis-record` 补独立记录持久化接口文档
2. 为 `/api/sse` 单独补事件协议文档
3. 为 `entry-decision` 后续若引入 `decision_case`，再补完整状态流转文档
4. 为 `trade-plan-analysis` 增加“生成即自动保存”与“用户确认后保存”两种模式的差异说明
