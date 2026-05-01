# AI 层实现说明

本文整理当前仓库里已经落地的 AI 实现、AI 流程、提示语设计与应答内容结构。内容按业务功能划分，重点描述真实代码路径，而不是规划稿。

## 1. 总体架构

当前 AI 能力主要分布在以下层次：

- **路由层**：接收页面请求与运行请求，启动异步任务，向前端返回 `client_id`，通过 SSE 推送进度、日志、阶段结果与最终结果。
- **Service 层**：组装每个业务场景的真实输入上下文，包括股票基础信息、财报、技术面、新闻、历史成交、历史分析记录等。
- **Orchestrator 层**：为具体业务角色拼接 prompt，调用统一 AI 客户端，解析 JSON，做本地结构校验，并生成统一结果。
- **LLM 适配层**：`StockAiAnalyzer` 负责与 OpenAI 兼容接口通讯，并执行“首轮生成 + 本地 JSON 解析 + 二次 JSON 重整”流程。
- **持久化层**：将 AI 结果按业务类型保存为历史记录，供页面回放与后续分析复用。

核心代码位置：

- 路由：`src/stock_analyse/interfaces/web/routes/trading_decision.py`
- 服务：`src/stock_analyse/interfaces/web/services/trading_decision_service.py`
- AI 调用层：`src/stock_analyse/infrastructure/llm/stock_ai_analyzer.py`
- 业务编排：
  - `src/stock_analyse/application/orchestrators/entry_decision_orchestrator.py`
  - `src/stock_analyse/application/orchestrators/trade_plan_analysis_orchestrator.py`
  - `src/stock_analyse/application/orchestrators/position_decision_orchestrator.py`
  - `src/stock_analyse/application/orchestrators/holding_review_orchestrator.py`

---

## 2. 当前系统分层服务

### 2.1 接口层（Web / Routes / SSE）

这一层直接面向页面、API 与浏览器端事件流，负责接收请求、启动异步任务、返回页面初始数据，并通过 SSE 推送 AI 运行中的日志、进度和最终结果。

- `src/stock_analyse/interfaces/web/app.py`
  - 打包后 Web 应用的统一入口。负责创建 Flask 应用、初始化共享上下文，并注册分析、历史、鉴权和交易决策路由。
- `src/stock_analyse/interfaces/web/routes/trading_decision.py`
  - 交易决策主路由文件。负责进场决策、持仓计划、买卖决策、持仓复盘等页面入口、run API、历史详情与后台任务调度。
- `src/stock_analyse/interfaces/web/routes/analysis.py`
  - 股票分析与 AI 个股分析路由。负责参数归一化、缓存命中短路、异步分析启动与分析页渲染。
- `src/stock_analyse/interfaces/web/routes/history.py`
  - 历史回放路由。负责把分析历史和选股历史还原为页面可直接展示的内容。
- `src/stock_analyse/interfaces/web/services/trading_decision_service.py`
  - 交易决策服务门面。负责装配关注股、持仓、财报、成交、历史决策等上下文，并负责结果归一化、缓存与落库。
- `src/stock_analyse/interfaces/web/services/stock_analyzer_service.py`
  - 通用股票分析 Web 服务。负责串接传统分析、AI 个股分析、历史查询、选股流程和 SSE 推送。
- `src/stock_analyse/interfaces/web/services/stock_indicator_html_service.py`
  - 指标图形 HTML 服务。负责把技术指标可视化结果转换为前端可嵌入的 HTML 片段。
- `src/stock_analyse/interfaces/web/streaming/sse_manager.py`
  - SSE 连接管理器。负责维护客户端队列、广播消息并清洗复杂对象，确保发送给前端的数据可序列化。
- `src/stock_analyse/interfaces/web/streaming/streaming_analyzer.py`
  - SSE 发送包装器。负责把日志、进度、阶段结果、最终结果和错误统一封装成前端约定事件。

### 2.2 应用编排层（Application / Orchestrators）

这一层负责把业务上下文变成一次具体的 AI 任务，定义角色、提示语、输出协议和本地校验规则，并把结果整理为统一业务对象。

- `src/stock_analyse/application/orchestrators/entry_decision_orchestrator.py`
  - 进场决策编排器。负责多角色串行分析、缺失人工输入暂停、角色结果汇总与最终 markdown 决策卡生成。
- `src/stock_analyse/application/orchestrators/trade_plan_analysis_orchestrator.py`
  - 持仓计划编排器。负责根据模板、缓存和回退数据生成结构化持仓计划与决策摘要。
- `src/stock_analyse/application/orchestrators/position_decision_orchestrator.py`
  - 买卖决策编排器。负责约束 5 个固定 tabs 的输出协议，并校验推荐动作、置信度和证据列表。
- `src/stock_analyse/application/orchestrators/holding_review_orchestrator.py`
  - 持仓复盘编排器。负责约束 4 个固定 tabs、总结字段与 conclusion_tag，并把复盘结果标准化。
- `src/stock_analyse/application/orchestrators/stock_ai_analysis_orchestrator.py`
  - 新版 AI 个股分析编排器。负责组织 analyst / researcher / manager / trader 多节点输出，并汇总最终交易立场。
- `src/stock_analyse/application/orchestrators/stock_analysis_orchestrator.py`
  - 传统股票分析编排器。负责技术图形、情绪分析和 AI 报告的组合执行。
- `src/stock_analyse/application/orchestrators/stock_selection_orchestrator.py`
  - 选股编排器。负责全市场扫描、候选筛选、回测和策略化选股流程调度。

### 2.3 AI 基础设施层（Infrastructure / LLM）

这一层负责屏蔽底层模型差异，统一处理请求构造、返回解析、JSON 重整和并发子任务调用。

- `src/stock_analyse/infrastructure/llm/stock_ai_analyzer.py`
  - 当前统一 AI 访问入口。负责发送兼容 OpenAI 的请求，并执行“首轮生成 → 本地 JSON 解析 → 二次 AI 重整”的链路。
- `src/stock_analyse/infrastructure/llm/client.py`
  - LLM 客户端薄封装。负责把摘要、指标分析、财报分析等传统能力映射到统一 AI 入口。
- `src/stock_analyse/infrastructure/llm/adapter.py`
  - LLM 适配器。负责并发组合多类分析请求，向传统股票分析流程返回完整文本结果集合。

### 2.4 持久化层（Infrastructure / Persistence）

这一层负责存储 AI 主链路需要的业务实体、运行会话和分析结果，使页面可以回放、补录和继续后续决策。

- `src/stock_analyse/infrastructure/persistence/trading_decision/sqlite_connection.py`
  - SQLite 连接工厂。负责提供交易决策模块共享的数据库连接。
- `src/stock_analyse/infrastructure/persistence/trading_decision/schema_manager.py`
  - 表结构管理器。负责初始化关注股、持仓、决策会话和分析记录等主表及索引。
- `src/stock_analyse/infrastructure/persistence/trading_decision/watch_stock_repository.py`
  - 关注股票仓储。负责关注池的新增、查询、筛选和状态更新。
- `src/stock_analyse/infrastructure/persistence/trading_decision/holding_stock_repository.py`
  - 持仓仓储。负责持仓主体、均价、仓位、盈亏等核心状态维护。
- `src/stock_analyse/infrastructure/persistence/trading_decision/holding_stock_trade_repository.py`
  - 持仓成交仓储。负责记录买卖明细和成交历史。
- `src/stock_analyse/infrastructure/persistence/trading_decision/holding_stock_lot_repository.py`
  - 持仓批次仓储。负责按 lot 跟踪买入批次和后续复盘基础数据。
- `src/stock_analyse/infrastructure/persistence/trading_decision/entry_decision_session_repository.py`
  - 进场决策会话仓储。负责多角色进场决策的暂停、恢复与状态持久化。
- `src/stock_analyse/infrastructure/persistence/trading_decision/entry_decision_record_repository.py`
  - 进场决策记录仓储。负责保存最终决策卡与历史结论。
- `src/stock_analyse/infrastructure/persistence/trading_decision/trade_plan_analysis_record_repository.py`
  - 持仓计划记录仓储。负责保存计划正文、风险等级和执行约束。
- `src/stock_analyse/infrastructure/persistence/trading_decision/position_decision_record_repository.py`
  - 买卖决策记录仓储。负责保存 5-tab 决策草案、摘要字段和原始结果。
- `src/stock_analyse/infrastructure/persistence/trading_decision/holding_review_record_repository.py`
  - 持仓复盘记录仓储。负责保存复盘总结、tabs 证据与上下文快照。
- `src/stock_analyse/infrastructure/persistence/trading_decision/stock_analysis_record_repository.py`
  - 股票分析记录仓储。负责保存 AI 个股分析结果并支撑关注、持仓和再分析场景复用。

### 2.5 主链路调用时序

当前 AI 主链路可以概括为：

1. Route 接收页面请求或 run API。
2. Service 组装业务上下文、历史记录和模板输入。
3. Orchestrator 定义角色、prompt、输出协议与本地校验。
4. `StockAiAnalyzer` 发送模型请求并处理 JSON 解析/重整。
5. Service 对结果进行归一化、缓存和持久化。
6. `StreamingAnalyzer` / `SSEManager` 按阶段把日志、进度与最终结果推送给前端。

---

## 3. 通用 AI 调用机制

### 2.1 统一入口

当前交易决策相关能力统一走：

- `src/stock_analyse/infrastructure/llm/stock_ai_analyzer.py:244`
  - `StockAiAnalyzer.openai_api_call(...)`

签名包含：

- `symbol`
- `message`
- `instruction`
- `tools`
- `tool_choice`
- `response_format`
- `require_tool_call`

但当前真实实现里，虽然保留了这些参数，**最终不会把 tools / tool_choice / response_format 传给模型**。

### 2.2 当前返回控制策略

当前项目采用的是：

1. 通过 prompt 明确要求模型只返回 JSON 对象。
2. 本地先尝试解析 JSON。
3. 如果首轮返回不是合法 JSON，就把原始文本再次发给 AI，要求它“整理成一个合法 JSON 对象字符串”。
4. 如果二次整理仍然无法解析，则抛出异常。
5. Orchestrator 再对 JSON 字段、枚举值、tabs 顺序、必填项做严格本地校验。

对应实现：

- 去除 tool / function / response_format：`stock_ai_analyzer.py:277-286`
- 首轮解析：`stock_ai_analyzer.py:296-301`
- 二次 AI 重整：`stock_ai_analyzer.py:186-209`
- JSON 截取解析：`stock_ai_analyzer.py:171-184`

### 2.3 JSON 重整提示语

二次整理时使用的系统提示语：

```text
{instruction}
你的唯一任务是把输入内容整理成一个合法 JSON 对象字符串。不要输出 markdown，不要输出解释，不要补充任何说明文字。
```

对应：`src/stock_analyse/infrastructure/llm/stock_ai_analyzer.py:153`

二次整理时使用的用户提示语：

```text
请将下面内容整理为一个合法 JSON 对象字符串。
要求：
1. 只输出 JSON 对象本身。
2. 去掉 markdown 代码块标记。
3. 如果有中文说明文字，只保留能组成 JSON 对象的部分。
4. 如果内容本身无法整理成 JSON 对象，请原样返回。

原始内容:
{raw_text}
```

对应：`src/stock_analyse/infrastructure/llm/stock_ai_analyzer.py:160`

### 2.4 通用运行流程

交易决策类 AI 功能的通用运行链路如下：

1. 页面调用 run API。
2. 路由层构建上下文并分配 `client_id`。
3. 后台线程启动任务。
4. `StreamingAnalyzer` 通过 SSE 向前端推送：
   - 日志
   - 进度
   - 阶段结果（如进场决策多角色输出）
   - 最终结果
   - 错误
5. Orchestrator 构造 prompt。
6. `StockAiAnalyzer.openai_api_call(...)` 发起首轮请求。
7. 本地解析 JSON；如果失败则触发二次 AI 重整。
8. Orchestrator 做本地 schema/字段校验。
9. Service 层做结果归一化与落库。
10. SSE 返回最终结果，页面支持实时展示与历史回放。

---

## 3. 进场决策（Entry Decision）

### 3.1 功能定位

进场决策用于在**关注股票阶段**生成一份完整的“买前决策卡”。它不是一次单角色问答，而是一个**多角色串行 AI 工作流**，最后再汇总成 markdown 实战版报告。

### 3.2 入口与运行方式

页面入口：

- `GET /entry-decision`
- 路由：`src/stock_analyse/interfaces/web/routes/trading_decision.py:479`

运行接口：

- `POST /api/trading-decision/watch-stocks/<watch_stock_id>/entry-decision/analyze`
- 路由：`trading_decision.py:631`

会话恢复接口：

- `POST /api/trading-decision/entry-decisions/<session_id>/resume`
- 路由：`trading_decision.py:709`

执行方式：

- 创建 session
- 异步后台执行
- SSE 推送角色进度与暂停状态
- 缺少手工仓位输入时暂停，待补充后继续

### 3.3 输入上下文

自动上下文构造在：

- `src/stock_analyse/application/orchestrators/entry_decision_orchestrator.py:169`

核心输入包括：

- `watch_stock_context`
  - 股票代码、名称、市场、行业、资产类型、当前价格、市盈率、备注
- `snapshot`
  - 财报
  - 技术面
  - 市场情绪
  - 新闻
- `derived_inputs`
  - 投资周期判断
  - 预期摘要
  - 财务摘要
  - 估值输入
- `manual_inputs`
  - 用户补充仓位与约束条件

其中 `buy_plan_analysis` 角色要求补齐：

- `position_input.current_position`
- `position_input.max_target_position`

若缺失则暂停。

### 3.4 AI 角色与提示语

角色定义在：

- `src/stock_analyse/application/orchestrators/entry_decision_orchestrator.py:24`

固定执行顺序：

1. `macro_analysis` / 宏观AI分析师
2. `asset_classification` / 资产分类AI分析师
3. `value_stage_analysis` / 价值阶段AI分析师
4. `price_zone_analysis` / 价格分区AI分析师
5. `buy_plan_analysis` / 买卖计划AI分析师
6. `risk_control_analysis` / 风险控制AI分析师

各角色 instruction 摘要如下。

#### 1）宏观AI分析师

```text
你是宏观AI分析师。请基于提供的数据，判断当前市场环境、风格偏好、资金风险偏好和该标的所处宏观适配度。必须严格输出 JSON 对象，字段至少包含: macro_view, macro_conclusion, macro_reasoning, market_style, liquidity_signal, risks, opportunities。
```

#### 2）资产分类AI分析师

```text
你是资产分类AI分析师。请判断该标的属于什么资产类型、这类资产主要靠什么上涨、当前适合什么打法。必须严格输出 JSON 对象，字段至少包含: asset_classification, classification_reasoning, upside_logic, risk_logic, recommended_playbook, forbidden_playbook。
```

#### 3）价值阶段AI分析师

```text
你是价值阶段AI分析师。请结合系统自动提取的历史财报摘要、预期变化与公司质量，判断当前价值阶段。必须严格输出 JSON 对象，字段至少包含: current_stage, stage_reasoning, revenue_growth_view, profit_growth_view, cashflow_view, margin_trend_view, expectation_view, stage_risks。
```

#### 4）价格分区AI分析师

```text
你是价格分区AI分析师。请结合系统自动提取的估值、价格、技术位置和安全边际判断当前价格区间。必须严格输出 JSON 对象，字段至少包含: price_zone, zone_reasoning, action_signal, action_reasoning, valuation_comment, technical_comment, cheap_reason, danger_reason。
```

#### 5）买卖计划AI分析师

```text
你是买卖计划AI分析师。请结合系统自动提取的周期偏好、估值与价格位置，并参考用户给定仓位约束，给出分笔建仓与后续应对计划。必须严格输出 JSON 对象，字段至少包含: suggested_action, action_reasoning, suggested_entry_leg, max_target_position, current_position, buy_plan, rise_plan, fall_plan, sell_rules, execution_notes。
```

#### 6）风险控制AI分析师

```text
你是风险控制AI分析师。请输出最终风险约束与决策卡。必须严格输出 JSON 对象，字段至少包含: risk_level, risk_reasoning, key_risks, invalidation_signals, position_constraints, decision_card, conclusion_summary。decision_card 必须包含 current_stage, current_price_zone, suggested_action, suggested_entry_leg, max_target_position, execution_summary。
```

### 3.5 单角色 prompt 结构

每个角色运行时的 prompt 由 `_build_prompt()` 生成：

- `entry_decision_orchestrator.py:209`

格式为：

```text
角色: {角色标题}
任务说明:
{角色 prompt}

请仅输出 JSON 对象，不要输出 markdown 代码块，不要输出额外解释。

上下文数据:
{payload JSON}
```

### 3.6 最终汇总 markdown

六个角色完成后，还会再调用一次 AI，把所有结果整合成 markdown 实战版报告。

模板文件：

- `/mnt/github/stock/进场决策模板_空白实战版.md`
- 代码：`entry_decision_orchestrator.py:22`

汇总 instruction：

- `entry_decision_orchestrator.py:356`

定位是：

```text
你是进场决策总结AI，也是把研究结论压缩成交易执行卡的编辑器。你的职责不是解释过程，而是基于模板产出一份接近“进场决策_600900.md”风格的完整实战版 markdown。必须严格遵守模板结构，必须把多个分析师输出整合成具体、保守、可执行的结论。
```

汇总 prompt 的关键约束：

- 严格保留模板章节标题、顺序、编号
- 输出完整 markdown
- 尽量写成可执行决策卡，而不是空白表单
- 对 Step 3/4/5/6/7/9/10 写出明确动作
- 不允许编造精确财务数字或价格
- analyst 结论冲突时，优先更保守、可执行的方案

### 3.7 输出结构

最终结果由 `_build_final_result()` 生成：

- `entry_decision_orchestrator.py:227`

主要字段包括：

- `basic_info`
- `macro_analysis`
- `asset_classification`
- `value_stage_analysis`
- `price_zone_analysis`
- `buy_plan_analysis`
- `risk_control_analysis`
- `decision_card`
- `entry_decision_summary_markdown`
- `snapshot`
- `manual_inputs`
- `meta`

### 3.8 持久化与前端返回

异步执行代码：

- `trading_decision.py:351`

成功后会：

1. 更新 session 状态。
2. 标记结果来源 `live`。
3. 保存 markdown 缓存。
4. 自动保存历史记录：`save_entry_decision_record(...)`
5. 通过 SSE 推送最终结果。

---

## 4. 持仓计划分析（Trade Plan Analysis）

### 4.1 功能定位

持仓计划分析用于从关注股票阶段，结合模板、缓存结论和回退数据，生成一份**可执行的持仓计划草案**。输出不是 tab，而是 markdown 正文 + 决策对象。

### 4.2 入口与运行方式

页面入口：

- `GET /trade-plan-analysis`
- 路由：`trading_decision.py:494`

运行接口：

- `POST /api/trading-decision/watch-stocks/<watch_stock_id>/trade-plan-analysis/run`
- 路由：`trading_decision.py:737`

执行方式：

- 后台异步任务
- SSE 返回日志、进度、最终结果
- 成功后写入 markdown 缓存

### 4.3 输入上下文

Orchestrator 使用的 context 结构见：

- `src/stock_analyse/application/orchestrators/trade_plan_analysis_orchestrator.py:55`

核心输入：

- `template_markdown`
- `watch_stock`
- `request`
- `cache_context`
- `fallback_context`
- `data_source`

模板默认从：

- `/mnt/github/stock/stockAnalyse/doc/持仓计划.md`
- 代码：`trade_plan_analysis_orchestrator.py:10`

### 4.4 AI 角色与提示语

固定角色 instruction：

```text
你是一名股票交易专家，擅长把研究结论转化为可执行的仓位、价格、下单和失败预案。请输出严格 JSON，不要输出 markdown 代码块，不要输出额外解释。
```

位置：`trade_plan_analysis_orchestrator.py:64`

用户 prompt 的关键要求：

- 严格按模板章节顺序输出 `trade_plan_markdown`
- 不删改章节标题
- 输出必须可执行
- 优先复用缓存中的进场决策或股票分析结论
- 不足信息可用 fallback 数据补足
- 无法确定时写“待确认”
- 必须返回固定 JSON 结构

位置：`trade_plan_analysis_orchestrator.py:68`

### 4.5 输出结构

要求模型返回：

```json
{
  "trade_plan_markdown": "完整 markdown 正文",
  "decision": {
    "action": "buy|hold|watch|sell",
    "summary": "一句话总结",
    "logic": "核心逻辑",
    "risk_level": "low|medium|high",
    "risks": ["风险1", "风险2"],
    "time_horizon": "执行周期",
    "position_suggestion": {
      "target_position": "最大目标仓位",
      "position_limit": "单票仓位上限",
      "add_condition": "加仓条件",
      "reduce_condition": "减仓条件",
      "stop_loss_reference": "止损或退出参考"
    }
  },
  "plan_metadata": {
    "template_name": "持仓计划模板（买前执行版）",
    "data_source": "cache_first|partial_cache_fallback|fallback_only",
    "cache_hits": ["命中文件名"]
  }
}
```

位置：`trade_plan_analysis_orchestrator.py:75`

最终后端返回结构在 `_build_final_result()` 中整理为：

- `watch_stock_id`
- `stock_code`
- `stock_name`
- `market`
- `trade_date`
- `plan_type`
- `risk_preference`
- `trade_plan_markdown`
- `decision`
- `meta`
- `cache_context`
- `fallback_context`

位置：`trade_plan_analysis_orchestrator.py:108`

### 4.6 异常回退

如果模型没有给出 `trade_plan_markdown`，会走 `_fallback_markdown()` 拼出一个最小可展示版本。

位置：`trade_plan_analysis_orchestrator.py:179`

### 4.7 持久化与返回

异步执行代码：

- `trading_decision.py:99`

成功后会：

1. 生成结果。
2. 写入 `trade_plan` markdown 缓存。
3. 通过 SSE 返回最终结果。

---

## 5. 买卖决策（Position Decision）

### 5.1 功能定位

买卖决策用于在**持仓阶段**基于财报、历史成交、持仓计划等输入，生成一个结构化的买卖决策草案。前端以 **5 个固定 tabs** 展示。

### 5.2 入口与运行方式

页面入口：

- `GET /position-decision`
- 路由：`trading_decision.py:524`

运行接口：

- `POST /api/trading-decision/holding-stocks/<holding_stock_id>/position-decisions/run`
- 路由：`trading_decision.py:806`

执行方式：

- 后台异步任务
- SSE 推送日志、进度、最终结果
- 成功后自动保存历史记录

### 5.3 输入上下文

由 `TradingDecisionService.build_position_decision_context(...)` 组装：

- `src/stock_analyse/interfaces/web/services/trading_decision_service.py:3036`

输入包含：

- `holding_stock`
- `watch_stock`
- `request`
- `financial_context`
  - 公司画像
  - 财务指标
  - 报表
- `trade_history_context`
  - 历史成交
  - lot 记录
  - 技术面 / 情绪 / 市场环境 / 新闻快照
- `holding_plan_context`
  - 最新持仓计划
  - 持仓计划历史
- `supporting_context`
  - 股票分析历史
  - 进场决策历史
- `data_source = holding_snapshot`
- `role_instruction = 股票分析师`

### 5.4 AI 角色与提示语

固定角色 instruction：

```text
你是股票分析师。请基于财报数据、历史成交数据、持仓计划数据做买卖决策分析。必须返回结构化 JSON 对象，不要输出 markdown 代码块，不要输出额外解释。
```

位置：`src/stock_analyse/application/orchestrators/position_decision_orchestrator.py:67`

核心用户 prompt：

```text
请根据输入上下文生成一份结构化买卖决策草案。

硬性要求：
1. 不要预设动作，必须先分析后给出推荐动作。
2. 推荐动作 recommended_action 只能是：buy、reduce、sell、watch。
3. 必须输出 5 个固定 tabs，顺序必须是：触发条件、核心理由、执行注意事项、风险分析、结论。
4. 每个 tab 必须包含：id、title、summary、evidence。
5. 每个 tab 的 summary 是顶部结论，evidence 是底部理由列表。
6. 最后一个“结论”tab 必须综合前四个 tabs，给出最终推荐动作与置信度。
```

位置：`position_decision_orchestrator.py:71`

### 5.5 输出协议

后端把 schema 文本直接嵌进 prompt 中，要求模型返回一个 JSON 对象。

schema 位置：

- `position_decision_orchestrator.py:100`

关键字段：

- `recommended_action`: `buy | reduce | sell | watch`
- `decision_status`: `buy_candidate | reduce_candidate | sell_candidate | observe`
- `confidence`: `high | medium | low`
- `conclusion_summary`: string
- `tabs`: 固定 5 个

固定 tabs：

1. `trigger / 触发条件`
2. `reason / 核心理由`
3. `execution / 执行注意事项`
4. `risk / 风险分析`
5. `conclusion / 结论`

### 5.6 本地校验逻辑

Orchestrator 在 `_validate_position_decision_payload()` 里做严格校验：

- 必填字段不能缺失
- 枚举值必须合法
- `tabs` 数量必须等于 5
- tab 顺序必须固定
- 每个 tab 都必须有：
  - `id`
  - `title`
  - `summary`
  - 非空 `evidence`
- `conclusion_summary` 不能为空

位置：`position_decision_orchestrator.py:154`

### 5.7 最终返回结构

最终结果结构见：

- `position_decision_orchestrator.py:198`

主要字段：

- `decision`
  - `action`
  - `status`
  - `confidence`
  - `summary`
- `tabs`
- `evidence`
- `meta`
  - `role = 股票分析师`
  - `data_source`
  - `duration_ms`
- `context_snapshot`
  - `financial_context`
  - `trade_history_context`
  - `holding_plan_context`

### 5.8 持久化

异步执行代码：

- `trading_decision.py:135`

成功后会自动调用：

- `save_position_decision_record(...)`

保存时会从 tabs 里拆出摘要字段：

- `trigger_summary`
- `reason_summary`
- `execution_summary`
- `risk_summary`
- `conclusion_summary`
- `confidence`
- `tabs_json`
- `evidence_json`
- `raw_result_json`

对应实现：

- `trading_decision_service.py:3141`

---

## 6. 持仓复盘（Holding Review）

### 6.1 功能定位

持仓复盘用于对**当前持仓的交易过程与结果**进行复盘，重点不是普通荐股，而是复盘：

- 结果是否符合预期
- 执行是否到位
- 风险是否抬升
- 方法与纪律是否偏离
- 下一步动作是什么

前端以 **4 个固定 tabs** 展示。

### 6.2 入口与运行方式

页面入口：

- `GET /holding-review`
- 路由：`trading_decision.py:509`

运行接口：

- `POST /api/trading-decision/holding-stocks/<holding_stock_id>/reviews/run`
- 路由：`trading_decision.py:764`

执行方式：

- 后台异步任务
- SSE 推送日志、进度、最终结果
- 成功后自动保存历史记录

### 6.3 输入上下文

由 `TradingDecisionService.build_holding_review_context(...)` 组装：

- `src/stock_analyse/interfaces/web/services/trading_decision_service.py:2963`

输入包含：

- `holding_stock`
- `watch_stock`
- `request`
- `trade_history_context`
  - 历史成交
  - lots
  - 最近三笔关键动作 `recent_trade_steps`
- `entry_context`
  - 最新进场决策
  - 进场决策历史
- `reanalysis_context`
  - 最新再评估
  - 再评估历史
- `position_decision_context`
  - 最新买卖决策
  - 买卖决策历史
- `financial_context`
  - 公司画像
  - 财务指标
  - 报表
- `market_context`
  - 技术面
  - 情绪
  - 市场环境
  - 新闻
- `review_focus_context`
  - 当前建议动作
  - 未实现盈亏
  - 未实现收益率
  - 持仓数量
  - 上次复盘时间
- `data_source = holding_snapshot`
- `role_instruction = 交易专家`

### 6.4 AI 角色与提示语

固定角色 instruction：

```text
你是交易专家。请基于持仓成交、原始决策、复盘相关记录、财报与市场数据做持仓复盘。必须返回结构化 JSON 对象，不要输出 markdown 代码块，不要输出额外解释。
```

位置：`src/stock_analyse/application/orchestrators/holding_review_orchestrator.py:70`

核心用户 prompt：

```text
请根据输入上下文生成一份结构化持仓复盘草案。

硬性要求：
1. 这是持仓复盘，不是普通荐股报告，必须同时看结果与过程。
2. 必须输出以下字段：performance_summary、execution_summary、risk_summary、discipline_summary、next_action_summary、conclusion_tag、tabs。
3. conclusion_tag 只能是：logic_ok、need_recheck、execution_issue、risk_rising、prepare_reduce、prepare_sell。
4. 必须输出 4 个固定 tabs，顺序必须是：执行与卖出复盘、结果复盘、方法与纪律、后续动作。
5. 每个 tab 必须包含：id、title、summary、evidence。
6. 每个 tab 的 summary 是顶部结论，evidence 是底部理由列表。
7. 后续动作 tab 必须综合前 3 个 tabs，明确下一步动作建议，并体现 conclusion_tag。
```

位置：`holding_review_orchestrator.py:74`

### 6.5 输出协议

schema 以 JSON Schema 文本形式写进 prompt：

- `holding_review_orchestrator.py:104`

关键字段：

- `performance_summary`
- `execution_summary`
- `risk_summary`
- `discipline_summary`
- `next_action_summary`
- `conclusion_tag`
- `tabs`

`conclusion_tag` 枚举：

- `logic_ok`
- `need_recheck`
- `execution_issue`
- `risk_rising`
- `prepare_reduce`
- `prepare_sell`

固定 tabs：

1. `execution_review / 执行与卖出复盘`
2. `result_review / 结果复盘`
3. `discipline_review / 方法与纪律`
4. `next_action / 后续动作`

### 6.6 本地校验逻辑

Orchestrator 在 `_validate_holding_review_payload()` 中强校验：

- 顶层必须是对象
- 必填字段齐全
- `conclusion_tag` 必须是合法枚举
- 5 个 summary 字段不能为空
- `tabs` 数量必须等于 4
- tab 顺序必须固定
- 每个 tab 都必须有：
  - `id`
  - `title`
  - `summary`
  - 非空 `evidence`

位置：`holding_review_orchestrator.py:162`

### 6.7 最终返回结构

最终结果由 `_build_final_result()` 生成：

- `holding_review_orchestrator.py:206`

主要字段：

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
- `tabs`
- `evidence`
- `meta`
  - `role = 交易专家`
  - `data_source`
  - `duration_ms`
- `context_snapshot`

### 6.8 持久化

异步执行代码：

- `trading_decision.py:184`

成功后自动调用：

- `save_holding_review_record(...)`

落库字段映射：

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

对应实现：

- `trading_decision_service.py:3102`
- `trading_decision_service.py:3271`

---

## 7. 持仓再分析 / 二次分析（Holding Reanalysis）

### 7.1 功能定位

持仓再分析属于持仓侧的再次分析能力，用于结合持仓状态、关注股票历史、进场决策历史、持仓计划历史，重新给出分析结果。它属于 AI 能力链路的一部分，但本文档当前重点记录的是交易决策模块中已经明显独立成型的几条主链路。

### 7.2 当前实现位置

相关 Service 入口：

- `src/stock_analyse/interfaces/web/services/trading_decision_service.py:2801`
- `trading_decision_service.py:2856`
- `trading_decision_service.py:2867`

已知上下文组织包括：

- `snapshot`
- `watch_stock_history`
- `entry_decision_history`
- `trade_plan_history`

并会在保存时标记：

- `analysis_scene = holding_reanalysis`

### 7.3 与本文档其他能力的关系

持仓再分析更多承担“再次观察 / 再确认”的作用；买卖决策更强调动作建议，持仓复盘更强调事后复盘沉淀。三者共用同一套路由 / service / SSE 基础设施，但输入重点与输出协议不同。

---

## 8. 传统股票分析与选股能力

### 8.1 传统股票分析

除交易决策模块外，仓库中仍保留较早期的股票分析 AI 能力，入口仍在 `StockAiAnalyzer` 中。

例如：

- `stock_indicator_analyse(...)`
- `stock_report_analyse(...)`

对应位置：

- `src/stock_analyse/infrastructure/llm/stock_ai_analyzer.py:342`
- `stock_ai_analyzer.py:458`

这部分特点是：

- 直接拼接大段市场、财务、技术、新闻数据
- 以研究报告式 prompt 为主
- 输出通常是自然语言或 markdown
- 不像交易决策模块那样有严格 tabs / schema / 历史记录协议

### 8.2 选股能力

根据当前系统实现，选股流程更多是规则 / 评分驱动，不是本文前面几类那种结构化 LLM 工作流。因此本文件不展开作为“AI 输出协议”主场景描述。

---

## 9. SSE 与前端交互模式

当前 AI 场景统一采用异步 + SSE 方式与前端交互。

在 `src/stock_analyse/interfaces/web/routes/trading_decision.py` 中，典型后台任务都会：

1. `streamer.send_log(...)`
2. `streamer.send_progress(...)`
3. 成功时 `streamer.send_final_result(...)`
4. 结束时 `streamer.send_completion(...)`
5. 失败时 `streamer.send_error(...)`

其中进场决策还额外使用：

- `send_role_result`
- `send_pause`

因此前端能区分：

- 初始化阶段
- 每步进度
- 多角色阶段结果
- 缺失输入暂停
- 最终结果
- 失败信息

这也是为什么当前页面可以支持：

- 实时日志
- 进度条
- Tab 实时渲染
- 历史记录回放

---

## 10. 当前各业务能力输出形态对比

### 10.1 进场决策

- 输出类型：多角色 JSON + 最终 markdown
- 角色数：6 个分析师 + 1 个总结 AI
- 是否允许暂停：允许
- 前端呈现：阶段结果 + 决策卡 + markdown

### 10.2 持仓计划分析

- 输出类型：严格 JSON
- 主要结果：`trade_plan_markdown + decision + meta`
- 前端呈现：markdown 正文 + 决策摘要

### 10.3 买卖决策

- 输出类型：严格 JSON
- 主要结果：`decision + 5 tabs + evidence`
- 前端呈现：固定 5 个 tabs

### 10.4 持仓复盘

- 输出类型：严格 JSON
- 主要结果：`5 个 summary 字段 + conclusion_tag + 4 tabs + evidence`
- 前端呈现：固定 4 个 tabs

---

## 11. 当前 AI 层的关键设计结论

### 11.1 不是 Tool Calling 驱动

虽然 orchestrator 中保留了“tool schema”命名，例如：

- `_build_position_decision_tool_schema()`
- `_build_holding_review_tool_schema()`

但当前这些 schema 的真实作用是：

- **把 JSON 结构说明直接嵌入 prompt**
- 并在本地做结构校验

而不是把 schema 真正作为 OpenAI tools/function_call 传给模型执行。

### 11.2 真实稳定性保障来自三层

当前实现的稳定性主要来自三层：

1. **Prompt 约束**：只允许输出 JSON，并写清楚字段、枚举、tab 顺序。
2. **二次 AI 重整**：首轮输出不合法时，要求 AI 把原文整理成合法 JSON。
3. **本地严格校验**：orchestrator 对字段与顺序做硬校验，不接受模糊结构。

### 11.3 页面展示协议已经业务化

当前项目中的 AI 结果不是简单聊天文本，而是已经业务化为：

- 进场决策的多角色研究卡
- 持仓计划的执行模板
- 买卖决策的固定 5-tab 协议
- 持仓复盘的固定 4-tab 协议

因此后续如果继续扩展 AI 功能，建议仍沿用：

- 先定义业务输出协议
- 再定义 prompt
- 再做本地校验
- 最后沉淀为历史记录和页面回放

---

## 12. 关键文件索引

### AI 调用层

- `src/stock_analyse/infrastructure/llm/stock_ai_analyzer.py`

### 路由层

- `src/stock_analyse/interfaces/web/routes/trading_decision.py`

### Service 层

- `src/stock_analyse/interfaces/web/services/trading_decision_service.py`

### Orchestrator 层

- `src/stock_analyse/application/orchestrators/entry_decision_orchestrator.py`
- `src/stock_analyse/application/orchestrators/trade_plan_analysis_orchestrator.py`
- `src/stock_analyse/application/orchestrators/position_decision_orchestrator.py`
- `src/stock_analyse/application/orchestrators/holding_review_orchestrator.py`

---

## 13. 一句话总结

当前仓库的 AI 层已经从“直接生成分析文本”升级为“按业务协议生成结构化结果”：路由负责异步任务与 SSE，Service 负责真实数据装配，Orchestrator 负责 prompt 与本地校验，`StockAiAnalyzer` 负责 prompt-only JSON 生成与二次重整，最终结果再落为可回放的业务历史记录。

---

## 14. 当前包结构的完整分层说明（application / domain / infrastructure / interfaces / shared）

这一章不是只看 AI 主链路，而是按当前 `src/stock_analyse` 的真实目录结构，对五个顶层目录、主要子目录以及每个 Python 文件的职责做一次完整说明。

### 14.1 顶层分层定义

#### 1）`application`

这是**应用编排层**。

职责：
- 面向“用例执行”组织业务流程
- 调度 domain 能力与 infrastructure 能力
- 定义 AI 编排、工作流、DTO、agent 协作方式
- 不直接承担底层存储和外部接口适配细节

可以理解为：**系统如何完成一件事**，主要在这一层定义。

#### 2）`domain`

这是**领域规则层**。

职责：
- 承载估值、技术分析、情绪分析、选股策略等核心业务规则
- 定义与具体框架无关的分析逻辑和策略逻辑
- 尽量保持“业务含义优先”，而不是“外部技术优先”

可以理解为：**系统为什么这样判断**，主要在这一层体现。

#### 3）`infrastructure`

这是**基础设施层**。

职责：
- 对接配置、日志、外部数据源、LLM、数据库、文件缓存等技术实现
- 为 application / domain 提供可调用的技术能力
- 处理持久化、远程 API、底层计算器等适配问题

可以理解为：**系统依赖什么技术手段落地**，主要在这一层实现。

#### 4）`interfaces`

这是**接口适配层**。

职责：
- 对外暴露 Web 页面、HTTP API、SSE 流式事件
- 把用户请求转成 application 层可执行的输入
- 把 application 层结果转换为页面和接口响应

可以理解为：**系统如何和用户/前端交互**，主要在这一层完成。

#### 5）`shared`

这是**共享基础层**。

职责：
- 放置跨层公用的枚举、异常、日期工具、股票工具等
- 避免相同低层工具在多个层里重复实现

可以理解为：**各层都会复用的通用基础件**。

### 14.2 `application` 目录说明

#### `application/`

- `src/stock_analyse/application/__init__.py`
  - 应用层包标记文件，用于声明 `application` 是一个独立分层模块。

#### `application/agents/`

职责：
- 承载多角色 AI 分析流程里的 agent 定义
- 将不同分析角色拆分为独立职责单元

- `src/stock_analyse/application/agents/__init__.py`
  - agent 子包标记文件。
- `src/stock_analyse/application/agents/base_agent.py`
  - AI agent 基类，负责沉淀统一的模型调用入口、提示语拼装或角色共性能力。

##### `application/agents/analysts/`

职责：
- 定义“分析师角色”类型的 AI 节点，偏向事实整理和单维度分析。

- `src/stock_analyse/application/agents/analysts/__init__.py`
  - analyst 子包标记文件。
- `src/stock_analyse/application/agents/analysts/fundamentals_analyst.py`
  - 基本面分析师 agent，负责财报、盈利、成长性、质量等维度分析。
- `src/stock_analyse/application/agents/analysts/market_analyst.py`
  - 市场分析师 agent，负责市场环境、风格、技术和交易背景判断。
- `src/stock_analyse/application/agents/analysts/news_analyst.py`
  - 新闻分析师 agent，负责新闻、舆情和事件驱动信息整理。

##### `application/agents/managers/`

职责：
- 定义“管理者角色”类型的 AI 节点，偏向汇总、审查和风险把关。

- `src/stock_analyse/application/agents/managers/__init__.py`
  - manager 子包标记文件。
- `src/stock_analyse/application/agents/managers/research_manager.py`
  - 研究经理 agent，负责综合多方研究结果形成统一研究结论。
- `src/stock_analyse/application/agents/managers/risk_manager.py`
  - 风险经理 agent，负责从仓位、波动、逻辑破坏等角度输出风控判断。

##### `application/agents/researchers/`

职责：
- 定义“研究员角色”类型的 AI 节点，偏向多空论证。

- `src/stock_analyse/application/agents/researchers/__init__.py`
  - researcher 子包标记文件。
- `src/stock_analyse/application/agents/researchers/bear_researcher.py`
  - 空头研究员 agent，负责构建负面或保守论证。
- `src/stock_analyse/application/agents/researchers/bull_researcher.py`
  - 多头研究员 agent，负责构建正面或进攻性论证。

##### `application/agents/trader/`

职责：
- 定义最终交易决策角色，把研究结果转成动作建议。

- `src/stock_analyse/application/agents/trader/__init__.py`
  - trader 子包标记文件。
- `src/stock_analyse/application/agents/trader/trader_agent.py`
  - 交易员 agent，负责把多角色输出压缩成最终交易立场和执行建议。

#### `application/dto/`

职责：
- 定义应用层流转的数据对象
- 承担 AI 工作流输入、状态、输出的结构化表达

- `src/stock_analyse/application/dto/__init__.py`
  - DTO 子包标记文件。
- `src/stock_analyse/application/dto/entry_decision_state.py`
  - 进场决策状态对象，保存 session、角色进度、暂停信息和最终结果。
- `src/stock_analyse/application/dto/stock_ai_analysis_request.py`
  - AI 个股分析请求对象，承载分析参数与上下文输入。
- `src/stock_analyse/application/dto/stock_ai_analysis_response.py`
  - AI 个股分析响应对象，负责组织成功/失败响应结构。
- `src/stock_analyse/application/dto/stock_ai_analysis_state.py`
  - AI 个股分析全流程状态对象，保存分析阶段、研究输出与最终决策。

#### `application/orchestrators/`

职责：
- 编排复杂业务流程
- 负责 prompt、角色顺序、结构化输出协议、本地校验和最终结果整理
- 是当前 AI 主链路最核心的应用层目录之一

- `src/stock_analyse/application/orchestrators/entry_decision_orchestrator.py`
  - 进场决策编排器，负责多角色串行执行、暂停恢复与最终 markdown 决策卡生成。
- `src/stock_analyse/application/orchestrators/holding_review_orchestrator.py`
  - 持仓复盘编排器，负责生成固定 4-tab 的复盘草案。
- `src/stock_analyse/application/orchestrators/position_decision_orchestrator.py`
  - 买卖决策编排器，负责生成固定 5-tab 的买卖决策草案。
- `src/stock_analyse/application/orchestrators/stock_ai_analysis_orchestrator.py`
  - 新版 AI 个股分析编排器，负责 analyst / researcher / manager / trader 多节点协作。
- `src/stock_analyse/application/orchestrators/stock_analysis_orchestrator.py`
  - 传统股票分析编排器，负责技术分析、情绪分析和 AI 报告组合执行。
- `src/stock_analyse/application/orchestrators/stock_selection_orchestrator.py`
  - 选股编排器，负责市场扫描、候选筛选和回测流程调度。
- `src/stock_analyse/application/orchestrators/trade_plan_analysis_orchestrator.py`
  - 持仓计划分析编排器，负责按模板生成结构化持仓计划草案。

#### `application/services/`

职责：
- 为 orchestrator 或 use case 提供偏应用层的聚合服务
- 强调“为了完成一个业务场景，需要哪些组合数据”

- `src/stock_analyse/application/services/ai_stock_data_facade.py`
  - AI 数据门面，负责聚合 AI 分析所需的快照数据和多源输入。
- `src/stock_analyse/application/services/quantitative_analysis_service.py`
  - 量化分析服务，负责技术指标和量化分析的应用层封装。

#### `application/use_cases/`

职责：
- 一文件一个动作或一个业务用例
- 是 application 层里最细粒度的执行单元

- `src/stock_analyse/application/use_cases/__init__.py`
  - use case 子包标记文件。
- `src/stock_analyse/application/use_cases/analyze_financials.py`
  - 财务分析用例，负责整理并分析财务报表数据。
- `src/stock_analyse/application/use_cases/analyze_reports.py`
  - 报告分析用例，负责统一分析不同类型报告内容。
- `src/stock_analyse/application/use_cases/analyze_sentiment.py`
  - 情绪分析用例，负责生成情绪分数和情绪结论。
- `src/stock_analyse/application/use_cases/analyze_single_stock.py`
  - 单只股票分析用例，负责运行传统综合分析流程。
- `src/stock_analyse/application/use_cases/analyze_single_stock_ai.py`
  - AI 单股分析用例，负责运行 AI 增强版个股分析流程。
- `src/stock_analyse/application/use_cases/analyze_technical_indicators.py`
  - 技术指标分析用例，负责生成技术指标相关结论。
- `src/stock_analyse/application/use_cases/analyze_wave_trend.py`
  - 波段趋势分析用例，负责判断波段方向和趋势状态。
- `src/stock_analyse/application/use_cases/analyze_waves.py`
  - 波浪分析用例，负责识别价格波段结构。
- `src/stock_analyse/application/use_cases/calculate_dcf.py`
  - DCF 估值计算用例。
- `src/stock_analyse/application/use_cases/compare_valuation.py`
  - 估值对比用例，负责对比估值结果与当前价格位置。
- `src/stock_analyse/application/use_cases/find_history_stock_analysis.py`
  - 历史股票分析查询用例。
- `src/stock_analyse/application/use_cases/find_history_strategy_analysis.py`
  - 历史策略分析查询用例。
- `src/stock_analyse/application/use_cases/get_comprehensive_news_analysis.py`
  - 综合新闻分析用例，负责汇总新闻面结论。
- `src/stock_analyse/application/use_cases/get_dividend.py`
  - 分红数据获取用例。
- `src/stock_analyse/application/use_cases/get_financial_indicator.py`
  - 财务指标获取用例。
- `src/stock_analyse/application/use_cases/get_fund_flow.py`
  - 资金流获取用例。
- `src/stock_analyse/application/use_cases/get_holders.py`
  - 股东/持有人数据获取用例。
- `src/stock_analyse/application/use_cases/get_market_spot.py`
  - 市场概览数据获取用例。
- `src/stock_analyse/application/use_cases/get_price_range.py`
  - 价格区间计算用例。
- `src/stock_analyse/application/use_cases/get_sector_components.py`
  - 板块成分股查询用例。
- `src/stock_analyse/application/use_cases/get_sector_detail.py`
  - 板块明细数据获取用例。
- `src/stock_analyse/application/use_cases/get_stock_financial_report_history.py`
  - 股票财报历史获取用例。
- `src/stock_analyse/application/use_cases/get_stock_history.py`
  - 股票历史行情获取用例。
- `src/stock_analyse/application/use_cases/get_stock_info.py`
  - 股票基础信息获取用例。
- `src/stock_analyse/application/use_cases/get_stock_news.py`
  - 股票新闻获取用例。
- `src/stock_analyse/application/use_cases/get_stock_report.py`
  - 个股研究报告获取用例。
- `src/stock_analyse/application/use_cases/get_stock_sectors.py`
  - 股票关联板块获取用例。
- `src/stock_analyse/application/use_cases/list_concepts.py`
  - 概念板块列表用例。
- `src/stock_analyse/application/use_cases/list_industries.py`
  - 行业列表用例。
- `src/stock_analyse/application/use_cases/prepare_wave_visualization.py`
  - 波浪分析可视化准备用例。
- `src/stock_analyse/application/use_cases/query_analysis_history.py`
  - 分析历史统一查询用例。
- `src/stock_analyse/application/use_cases/query_select_history.py`
  - 选股历史统一查询用例。
- `src/stock_analyse/application/use_cases/run_full_market_scan.py`
  - 全市场扫描执行用例。
- `src/stock_analyse/application/use_cases/run_stock_selection.py`
  - 选股执行用例。
- `src/stock_analyse/application/use_cases/select_stock_strategy.py`
  - 选股策略分派用例。
- `src/stock_analyse/application/use_cases/select_stocks.py`
  - 股票筛选与打分用例。

#### `application/workflows/`

职责：
- 封装比单个 use case 更长链条的执行流程
- 更偏向“过程模板”而不是单点动作

- `src/stock_analyse/application/workflows/__init__.py`
  - workflow 子包标记文件。
- `src/stock_analyse/application/workflows/backtest_stocks_workflow.py`
  - 股票回测工作流。
- `src/stock_analyse/application/workflows/dividend_analysis_workflow.py`
  - 分红分析工作流。
- `src/stock_analyse/application/workflows/full_market_scan_workflow.py`
  - 全市场扫描工作流。
- `src/stock_analyse/application/workflows/technical_analysis_workflow.py`
  - 技术分析工作流。

### 14.3 `domain` 目录说明

#### `domain/`

- `src/stock_analyse/domain/__init__.py`
  - 领域层包标记文件。

#### `domain/services/`

职责：
- 沉淀不依赖 Web 路由和数据库表结构的核心分析逻辑
- 强调业务判断本身

- `src/stock_analyse/domain/services/__init__.py`
  - domain service 子包标记文件。
- `src/stock_analyse/domain/services/dcf_valuation_service.py`
  - DCF 估值领域服务，负责贴现现金流估值计算逻辑。
- `src/stock_analyse/domain/services/sentiment_analysis.py`
  - 情绪分析领域服务，负责情绪评分和情绪解释逻辑。
- `src/stock_analyse/domain/services/stock_strategy_service.py`
  - 股票策略领域服务，负责策略信号和策略判断逻辑。
- `src/stock_analyse/domain/services/stock_wave_analyzer.py`
  - 波浪分析领域服务，负责波段识别和波形判断。
- `src/stock_analyse/domain/services/technical_indicator_service.py`
  - 技术指标领域服务，负责指标算法和指标解释逻辑。
- `src/stock_analyse/domain/services/technical_params.py`
  - 技术分析参数定义文件，负责沉淀指标参数和默认配置。
- `src/stock_analyse/domain/services/valuation_service.py`
  - 估值领域服务，负责高层估值逻辑和估值解释。

#### `domain/strategies/`

职责：
- 定义系统中的策略规则和筛选策略
- 是选股和过滤逻辑的主要领域表达位置

- `src/stock_analyse/domain/strategies/__init__.py`
  - strategy 子包标记文件。
- `src/stock_analyse/domain/strategies/financial_filter_service.py`
  - 财务过滤策略服务，负责按财务指标筛选股票。
- `src/stock_analyse/domain/strategies/financial_report_filter_service.py`
  - 财报过滤策略服务，负责按财报特征做筛选。
- `src/stock_analyse/domain/strategies/selection_strategy_service.py`
  - 选股策略服务，负责管理和分发不同选股策略。
- `src/stock_analyse/domain/strategies/stock_select_strategy.py`
  - 具体选股策略实现文件，负责执行单个策略逻辑。

### 14.4 `infrastructure` 目录说明

#### `infrastructure/`

- `src/stock_analyse/infrastructure/__init__.py`
  - 基础设施层包标记文件。

#### `infrastructure/analysis/`

职责：
- 承载偏底层、可复用的分析计算器实现。

- `src/stock_analyse/infrastructure/analysis/technical_indicator_calculator.py`
  - 技术指标计算器，负责实际指标数值计算。

#### `infrastructure/bootstrap/`

职责：
- 放置系统启动期的数据初始化逻辑。

- `src/stock_analyse/infrastructure/bootstrap/stock_data_initializer.py`
  - 股票数据初始化器，负责准备本地运行所需的数据基础。

#### `infrastructure/config/`

职责：
- 管理应用、Web、AI 等统一配置。

- `src/stock_analyse/infrastructure/config/__init__.py`
  - config 子包标记文件。
- `src/stock_analyse/infrastructure/config/settings.py`
  - 统一配置入口，负责加载和解析项目设置。

#### `infrastructure/data_sources/`

职责：
- 适配不同外部数据来源
- 负责把外部接口返回转换为系统内部可用数据

- `src/stock_analyse/infrastructure/data_sources/__init__.py`
  - data source 子包标记文件。

##### `infrastructure/data_sources/concepts/`
- `src/stock_analyse/infrastructure/data_sources/concepts/ths_concept_client.py`
  - 同花顺概念数据客户端。

##### `infrastructure/data_sources/news/`
- `src/stock_analyse/infrastructure/data_sources/news/eastmoney_news_client.py`
  - 东方财富新闻数据客户端。

##### `infrastructure/data_sources/reports/`
- `src/stock_analyse/infrastructure/data_sources/reports/annual_report_client.py`
  - 年报数据客户端，负责获取年报或报表原始数据。

#### `infrastructure/llm/`

职责：
- 管理与大模型相关的技术适配
- 是当前结构化 AI 输出链路的技术承载层

- `src/stock_analyse/infrastructure/llm/adapter.py`
  - LLM 适配器，负责传统股票分析场景下的多路 AI 请求组合。
- `src/stock_analyse/infrastructure/llm/client.py`
  - LLM 客户端薄封装，负责把特定分析请求转给统一 AI 入口。
- `src/stock_analyse/infrastructure/llm/stock_ai_analyzer.py`
  - 统一 AI 分析器，负责模型调用、原始响应保存、JSON 解析与二次重整。

#### `infrastructure/logging/`

职责：
- 提供统一日志工具。

- `src/stock_analyse/infrastructure/logging/__init__.py`
  - logging 子包标记文件。
- `src/stock_analyse/infrastructure/logging/logger.py`
  - 日志器工具文件，负责统一日志输出行为。

#### `infrastructure/persistence/`

职责：
- 提供文件缓存、数据库缓存、文件落盘等持久化能力。

- `src/stock_analyse/infrastructure/persistence/__init__.py`
  - persistence 子包标记文件。
- `src/stock_analyse/infrastructure/persistence/file_cache.py`
  - 文件缓存实现。
- `src/stock_analyse/infrastructure/persistence/mysql_cache.py`
  - MySQL 缓存实现。
- `src/stock_analyse/infrastructure/persistence/stock_file_utils.py`
  - 股票分析结果相关文件工具。

##### `infrastructure/persistence/trading_decision/`

职责：
- 专门负责交易决策模块的 SQLite 持久化
- 是当前关注股、持仓、进场决策、买卖决策、持仓复盘等功能的核心仓储目录

- `src/stock_analyse/infrastructure/persistence/trading_decision/__init__.py`
  - trading decision persistence 子包标记文件。
- `src/stock_analyse/infrastructure/persistence/trading_decision/entry_decision_record_repository.py`
  - 进场决策记录仓储。
- `src/stock_analyse/infrastructure/persistence/trading_decision/entry_decision_session_repository.py`
  - 进场决策会话仓储，负责暂停/恢复状态保存。
- `src/stock_analyse/infrastructure/persistence/trading_decision/holding_review_record_repository.py`
  - 持仓复盘记录仓储。
- `src/stock_analyse/infrastructure/persistence/trading_decision/holding_stock_lot_repository.py`
  - 持仓 lot 批次仓储。
- `src/stock_analyse/infrastructure/persistence/trading_decision/holding_stock_repository.py`
  - 持仓主体仓储，负责持仓核心状态存取。
- `src/stock_analyse/infrastructure/persistence/trading_decision/holding_stock_trade_repository.py`
  - 持仓成交明细仓储。
- `src/stock_analyse/infrastructure/persistence/trading_decision/position_decision_record_repository.py`
  - 买卖决策记录仓储。
- `src/stock_analyse/infrastructure/persistence/trading_decision/schema_manager.py`
  - 交易决策表结构管理器，负责建表和补齐索引。
- `src/stock_analyse/infrastructure/persistence/trading_decision/sqlite_connection.py`
  - SQLite 连接工厂，负责交易决策模块数据库连接。
- `src/stock_analyse/infrastructure/persistence/trading_decision/stock_analysis_record_repository.py`
  - 股票分析记录仓储。
- `src/stock_analyse/infrastructure/persistence/trading_decision/trade_plan_analysis_record_repository.py`
  - 持仓计划分析记录仓储。
- `src/stock_analyse/infrastructure/persistence/trading_decision/watch_stock_repository.py`
  - 关注股票仓储。

#### `infrastructure/services/`

职责：
- 对外部业务数据访问做进一步封装
- 介于 data source 与 application 之间，提供更接近业务的服务接口

- `src/stock_analyse/infrastructure/services/company_data_service.py`
  - 公司数据服务，负责公司基本面和公司资料相关数据获取。
- `src/stock_analyse/infrastructure/services/concept_service.py`
  - 概念板块服务。
- `src/stock_analyse/infrastructure/services/market_data_service.py`
  - 市场数据服务，负责市场行情和股票市场环境相关数据获取。
- `src/stock_analyse/infrastructure/services/valuation_gateway.py`
  - 估值网关，负责衔接估值所需的外部数据与内部估值逻辑。

### 14.5 `interfaces` 目录说明

#### `interfaces/`

- `src/stock_analyse/interfaces/__init__.py`
  - 接口层包标记文件。

#### `interfaces/web/`

职责：
- 当前系统的主要交付界面
- 负责 Flask 页面、API 和 SSE 流式输出

- `src/stock_analyse/interfaces/web/__init__.py`
  - Web 接口子包标记文件。
- `src/stock_analyse/interfaces/web/app.py`
  - Flask Web 应用入口，负责创建应用和注册所有路由。

##### `interfaces/web/routes/`

职责：
- 承接 HTTP 请求
- 把请求交给 service 或 orchestrator
- 返回页面、JSON 或异步任务启动结果

- `src/stock_analyse/interfaces/web/routes/__init__.py`
  - route 子包导出文件。
- `src/stock_analyse/interfaces/web/routes/analysis.py`
  - 股票分析与 AI 分析路由。
- `src/stock_analyse/interfaces/web/routes/auth.py`
  - 登录鉴权和认证相关路由。
- `src/stock_analyse/interfaces/web/routes/history.py`
  - 历史记录回放与查询路由。
- `src/stock_analyse/interfaces/web/routes/misc.py`
  - 杂项配置、辅助能力和通用页面路由。
- `src/stock_analyse/interfaces/web/routes/trading_decision.py`
  - 交易决策主路由，负责关注股、持仓、进场决策、持仓计划、买卖决策、持仓复盘等入口。

##### `interfaces/web/services/`

职责：
- 作为 Web 层的服务门面
- 负责把 application / infrastructure 的能力组合成页面和 API 所需数据

- `src/stock_analyse/interfaces/web/services/__init__.py`
  - web service 子包标记文件。
- `src/stock_analyse/interfaces/web/services/stock_analyzer_service.py`
  - Web 股票分析服务，负责分析执行、历史查询、选股执行和 SSE 联动。
- `src/stock_analyse/interfaces/web/services/stock_indicator_html_service.py`
  - 技术指标 HTML 服务，负责把图形结果转换为可嵌入前端的 HTML。
- `src/stock_analyse/interfaces/web/services/trading_decision_service.py`
  - 交易决策 Web 服务门面，负责上下文组装、记录保存和展示数据构造。

##### `interfaces/web/streaming/`

职责：
- 专门处理流式推送
- 是前端实时日志、进度条和结果回传的承载层

- `src/stock_analyse/interfaces/web/streaming/__init__.py`
  - streaming 子包标记文件。
- `src/stock_analyse/interfaces/web/streaming/sse_manager.py`
  - SSE 连接管理器，负责客户端生命周期和事件分发。
- `src/stock_analyse/interfaces/web/streaming/streaming_analyzer.py`
  - 流式事件包装器，负责把分析过程映射成统一前端事件。

### 14.6 `shared` 目录说明

#### `shared/`

职责：
- 放置跨层公用的小型基础能力
- 避免 application / domain / infrastructure / interfaces 互相复制通用工具

- `src/stock_analyse/shared/__init__.py`
  - shared 子包标记文件。
- `src/stock_analyse/shared/enums.py`
  - 共享枚举定义文件。
- `src/stock_analyse/shared/errors.py`
  - 共享异常定义文件。
- `src/stock_analyse/shared/report_date_utils.py`
  - 财报日期相关公共工具。
- `src/stock_analyse/shared/stock_utils.py`
  - 股票代码、名称、市场等通用工具函数。

### 14.7 当前分层关系的阅读方式

如果按阅读顺序理解整个系统，建议按下面路径看：

1. 先看 `interfaces/web/routes/`，理解系统对外暴露了哪些页面和 API。
2. 再看 `interfaces/web/services/`，理解页面和 API 的数据是如何被组织的。
3. 再看 `application/orchestrators/`、`application/use_cases/`、`application/workflows/`，理解真正的业务执行流程。
4. 再看 `domain/services/` 与 `domain/strategies/`，理解核心分析逻辑和策略判断规则。
5. 最后看 `infrastructure/`，理解模型、数据库、缓存、外部数据源和底层计算是如何落地的。

这套结构下，五层的关系可以简化为：

- `interfaces` 负责对外暴露
- `application` 负责组织业务执行
- `domain` 负责沉淀业务规则
- `infrastructure` 负责承接技术实现
- `shared` 负责提供跨层复用能力

这也是当前仓库从“脚本式分析工具”逐步演进为“分层业务系统”的核心结构基础。