# 股票数据访问涉及的类与接口

本文梳理当前项目中“股票数据访问”相关的核心类、分层关系以及对外接口，重点覆盖：
- 股票基础资料、行情、新闻、财报、概念板块等外部数据访问
- 关注股票、持仓股票、分析记录等本地持久化访问
- Web 页面/API 如何触发这些数据访问
- AI 分析链路如何组合股票数据

---

## 1. 总体分层

当前项目的数据访问大致分为 4 层：

1. **接口层（Web Routes）**
   - 接收页面请求或 API 请求
   - 调用 `TradingDecisionService`、`StockAnalyzerService`

2. **应用服务层（Application / Web Service）**
   - 拼装业务上下文
   - 协调外部股票数据与本地数据库数据
   - 驱动 AI 分析或历史记录查询

3. **基础设施层（Infrastructure Services / Data Sources）**
   - 从 AkShare、Eastmoney、财报源等读取股票数据
   - 对外部数据做缓存、转换、聚合

4. **持久化层（Repositories）**
   - 负责 watch stock / holding stock / 历史记录等 SQLite 数据访问

---

## 2. 股票数据访问的核心服务类

### 2.1 `TradingDecisionService`
- 文件：`src/stock_analyse/interfaces/web/services/trading_decision_service.py`
- 作用：
  - 持仓股票、关注股票页面数据构建
  - 历史记录页数据构建
  - 进场决策、持仓计划、二次分析、买卖决策、持仓复盘等业务上下文组装
  - 调用各类 Repository 访问本地业务数据
  - 调用 `AIStockDataFacade` 构建 AI 可用的股票快照

它是交易决策域的数据访问总入口之一。

### 2.2 `StockAnalyzerService`
- 文件：`src/stock_analyse/interfaces/web/services/stock_analyzer_service.py`
- 作用：
  - 封装传统股票分析与 AI 股票分析流程
  - 对接分析历史、选股历史查询
  - 调用应用层 orchestrator / use case 获取股票行情、财报、新闻等数据

它是“股票分析”域的数据访问总入口之一。

### 2.3 `AIStockDataFacade`
- 文件：`src/stock_analyse/application/services/ai_stock_data_facade.py`
- 作用：
  - 聚合单只股票分析所需的多源数据
  - `build_snapshot(...)` 是最关键的聚合入口
- 聚合内容包括：
  - 公司资料
  - 股票名称、主营业务
  - 概念/行业
  - 最新行情
  - 新闻
  - 财务指标
  - 资金流
  - 财报
  - 技术面摘要
  - 情绪分析

这是“AI 分析前的数据总装配层”。

---

## 3. 外部股票数据访问类

### 3.1 `stockCompanyInfo`
- 文件：`src/stock_analyse/infrastructure/services/company_data_service.py`
- 作用：
  - 访问股票级别的基础数据
- 典型数据：
  - 公司简介
  - 股票名称
  - 主营业务
  - 行业 / 概念信息
  - 个股新闻
  - 个股资金流
  - 财务指标
  - 分红、股本变化、股东增减持
  - 历史行情相关数据

这是项目里最重要的“个股数据访问类”之一。

### 3.2 `stockBorderInfo`
- 文件：`src/stock_analyse/infrastructure/services/market_data_service.py`
- 作用：
  - 访问市场级别数据
- 典型数据：
  - 股票实时行情 `get_stock_spot`
  - 全市场股票列表
  - 市场维度财报/财务指标
  - 概念、分红、北向持股等市场数据

这是最重要的“市场数据访问类”之一。

### 3.3 `stockAnnualReport`
- 文件：`src/stock_analyse/infrastructure/data_sources/reports/annual_report_client.py`
- 作用：
  - 访问财报原始数据与报表数据
- 典型数据：
  - 资产负债表
  - 利润表
  - 现金流量表
  - 年报/季报相关数据集

这是“财报数据访问类”的核心实现。

### 3.4 `stockNewsData`
- 文件：`src/stock_analyse/infrastructure/data_sources/news/eastmoney_news_client.py`
- 作用：
  - 从东财等来源抓取股票新闻

### 3.5 `stockConceptData`
- 文件：`src/stock_analyse/infrastructure/data_sources/concepts/ths_concept_client.py`
- 作用：
  - 访问概念板块、概念成分股等数据

### 3.6 `ValuationGateway`
- 文件：`src/stock_analyse/infrastructure/services/valuation_gateway.py`
- 作用：
  - 提供相对轻量的估值/行情访问封装
- 关键能力：
  - `get_stock_report`
  - `get_stock_spot`

### 3.7 `StockAiAnalyzer`
- 文件：`src/stock_analyse/infrastructure/llm/stock_ai_analyzer.py`
- 作用：
  - 不直接负责底层股票行情抓取，但负责消费股票数据并发起 AI 分析
  - 属于“股票数据消费型分析类”

---

## 4. 本地持久化访问类（Repository）

### 4.1 关注股票
#### `WatchStockRepository`
- 文件：`src/stock_analyse/infrastructure/persistence/trading_decision/watch_stock_repository.py`
- 作用：
  - 访问 `watch_stocks` 相关 SQLite 数据
- 典型能力：
  - 新增/修改/删除关注股票
  - 列表查询
  - 保存分析摘要字段

### 4.2 持仓股票
#### `HoldingStockRepository`
- 文件：`src/stock_analyse/infrastructure/persistence/trading_decision/holding_stock_repository.py`
- 作用：
  - 访问持仓股票主表
  - 聚合持仓数量、成本、盈亏、市值等数据

#### `HoldingStockLotRepository`
- 文件：`src/stock_analyse/infrastructure/persistence/trading_decision/holding_stock_lot_repository.py`
- 作用：
  - 访问持仓 lot 明细

#### `HoldingStockTradeRepository`
- 文件：`src/stock_analyse/infrastructure/persistence/trading_decision/holding_stock_trade_repository.py`
- 作用：
  - 访问买卖成交记录

### 4.3 股票分析/决策历史记录
#### `StockAnalysisRecordRepository`
- 文件：`src/stock_analyse/infrastructure/persistence/trading_decision/stock_analysis_record_repository.py`
- 作用：
  - 保存与查询股票分析记录
  - 同时覆盖关注股票分析、持仓二次分析

#### `TradePlanAnalysisRecordRepository`
- 文件：`src/stock_analyse/infrastructure/persistence/trading_decision/trade_plan_analysis_record_repository.py`
- 作用：
  - 保存与查询持仓计划分析记录

#### `PositionDecisionRecordRepository`
- 文件：`src/stock_analyse/infrastructure/persistence/trading_decision/position_decision_record_repository.py`
- 作用：
  - 保存与查询买卖决策记录

#### `HoldingReviewRecordRepository`
- 文件：`src/stock_analyse/infrastructure/persistence/trading_decision/holding_review_record_repository.py`
- 作用：
  - 保存与查询持仓复盘记录

#### `EntryDecisionRecordRepository`
- 文件：`src/stock_analyse/infrastructure/persistence/trading_decision/entry_decision_record_repository.py`
- 作用：
  - 保存与查询进场决策记录

#### `EntryDecisionSessionRepository`
- 文件：`src/stock_analyse/infrastructure/persistence/trading_decision/entry_decision_session_repository.py`
- 作用：
  - 保存进场决策会话状态

---

## 5. 应用层 orchestrator / use case 与股票数据访问关系

### 5.1 Orchestrator
以下类会直接或间接触发股票数据访问：

- `FocusStockAIAnalysisOrchestrator`
  - 文件：`src/stock_analyse/application/orchestrators/focus_stock_ai_analysis_orchestrator.py`
  - 作用：关注股票 AI 分析

- `HoldingStockReanalysisOrchestrator`
  - 文件：`src/stock_analyse/application/orchestrators/holding_stock_reanalysis_orchestrator.py`
  - 作用：持仓二次分析

- `StockAIAnalysisOrchestrator`
  - 文件：`src/stock_analyse/application/orchestrators/stock_ai_analysis_orchestrator.py`
  - 作用：统一股票 AI 分析调度

- `FocusEntryDecisionOrchestrator`
  - 文件：`src/stock_analyse/application/orchestrators/entry_decision_orchestrator.py`
  - 作用：进场决策分析

- `FocusTradePlanAnalysisOrchestrator`
  - 文件：`src/stock_analyse/application/orchestrators/trade_plan_analysis_orchestrator.py`
  - 作用：持仓计划分析

- `HoldingPositionDecisionOrchestrator`
  - 文件：`src/stock_analyse/application/orchestrators/position_decision_orchestrator.py`
  - 作用：买卖决策分析

- `HoldingReviewOrchestrator`
  - 文件：`src/stock_analyse/application/orchestrators/holding_review_orchestrator.py`
  - 作用：持仓复盘分析

### 5.2 Use Cases
常见股票数据访问 use case 位于：
- `src/stock_analyse/application/use_cases/get_stock_info.py`
- `src/stock_analyse/application/use_cases/get_stock_report.py`
- `src/stock_analyse/application/use_cases/get_market_spot.py`
- `src/stock_analyse/application/use_cases/get_stock_news.py`
- `src/stock_analyse/application/use_cases/get_financial_indicator.py`
- `src/stock_analyse/application/use_cases/get_fund_flow.py`
- `src/stock_analyse/application/use_cases/get_sector_components.py`
- `src/stock_analyse/application/use_cases/get_stock_history.py`

这些 use case 通常会调用：
- `stockCompanyInfo`
- `stockBorderInfo`
- `stockAnnualReport`
- `ValuationGateway`
- 相关分析服务或情绪分析组件

---

## 6. 股票数据访问相关的 Web 接口

## 6.1 股票分析接口
位于：`src/stock_analyse/interfaces/web/routes/analysis.py`

### 页面/流式相关
- `/api/sse`
  - 用于 AI 分析结果的流式输出

### API
- `/api/select_stock`
  - 选股分析
- `/api/analyze_stock`
  - 传统股票分析
- `/api/analyze_stock_ai`
  - AI 股票分析
- `/api/query_select_history`
  - 查询选股历史
- `/api/query_analysis_history`
  - 查询股票分析历史

## 6.2 关注/持仓/决策相关页面接口
位于：`src/stock_analyse/interfaces/web/routes/trading_decision.py`

### 页面
- `/index`
- `/watch-stocks`
- `/holding-stocks`
- `/entry-decision`
- `/trade-plan-analysis`
- `/holding-review`
- `/position-decision`

这些页面最终都会通过 `TradingDecisionService` 触发 Repository 查询，部分页面还会进一步拼装股票外部数据。

## 6.3 股票分析详情/二次分析页
位于：`src/stock_analyse/interfaces/web/routes/misc.py`

- `/stock-analysis-record`
  - 查看股票分析详情
- `/holding-reanalysis`
  - 查看/触发持仓二次分析

## 6.4 历史记录接口
位于：`src/stock_analyse/interfaces/web/routes/history.py`

- `/api/history/analyse`
- `/api/history/select`

---

## 7. 典型数据访问链路

### 7.1 关注股票分析链路
1. 请求进入 `/api/analyze_stock_ai`
2. `StockAnalyzerService` 接管流程
3. Orchestrator/use case 调用 `AIStockDataFacade.build_snapshot(...)`
4. `AIStockDataFacade` 聚合：
   - `stockCompanyInfo`
   - `stockBorderInfo`
   - `stockAnnualReport`
   - `stockNewsData`
   - 概念/技术/情绪分析组件
5. 结果交给 `StockAiAnalyzer` / AI orchestrator
6. 分析结果写入 `StockAnalysisRecordRepository`
7. 页面通过详情接口或历史接口读取结果

### 7.2 持仓复盘/买卖决策链路
1. 页面进入 `/holding-review` 或 `/position-decision`
2. `TradingDecisionService` 读取：
   - `HoldingStockRepository`
   - `HoldingStockTradeRepository`
   - `HoldingStockLotRepository`
   - 各类历史记录 Repository
3. 同时通过 `AIStockDataFacade` 获取外部股票数据快照
4. Orchestrator 组织 AI 请求
5. 分析结果保存到：
   - `HoldingReviewRecordRepository`
   - `PositionDecisionRecordRepository`
6. 页面再次通过记录详情页读取结果

---

## 8. 结论

当前项目的股票数据访问模式具有以下特征：

1. **外部股票数据访问** 主要由以下类承担：
   - `stockCompanyInfo`
   - `stockBorderInfo`
   - `stockAnnualReport`
   - `stockNewsData`
   - `stockConceptData`
   - `ValuationGateway`

2. **本地业务数据访问** 主要由各类 Repository 承担：
   - `WatchStockRepository`
   - `HoldingStockRepository`
   - `HoldingStockLotRepository`
   - `HoldingStockTradeRepository`
   - 各类 `*RecordRepository`

3. **应用层聚合入口** 主要是：
   - `TradingDecisionService`
   - `StockAnalyzerService`
   - `AIStockDataFacade`

4. **对外 Web 接口** 主要集中在：
   - `interfaces/web/routes/analysis.py`
   - `interfaces/web/routes/trading_decision.py`
   - `interfaces/web/routes/misc.py`
   - `interfaces/web/routes/history.py`

整体上，项目采用了“接口层 -> 应用服务层 -> 基础设施层/仓储层”的结构，股票数据访问路径相对清晰，其中 `AIStockDataFacade.build_snapshot(...)` 是外部股票数据聚合的关键节点。