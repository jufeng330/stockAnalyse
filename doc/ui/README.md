# 交易决策中心 UI 原型说明

本目录提供基于 `doc/trading_decision_online_plan.md` 第三章业务设计输出的静态 HTML 页面原型。

## 文件列表

### 选股功能兼容页

- `stock_screener_page.html`：选股策略兼容页
- `batch_analysis_page.html`：批量分析兼容页
- `single_stock_analysis_page.html`：单股分析兼容页
- `single_stock_analysis_legacy_page.html`：单股分析_老版本兼容页

### 系统配置页

- `ai_config_page.html`：AI 配置页
- `business_config_page.html`：业务配置页

### 主页面

- `watch_stocks_page.html`：关注股票列表页
- `holding_stocks_page.html`：持仓股票列表页
- `portfolio_review_page.html`：整体分析和复盘页

### 关注股票动作页

- `entry_decision_page.html`：进场决策页
- `stock_analysis_page.html`：股票分析页
- `trade_plan_analysis_page.html`：持仓计划分析页

### 持仓股票动作页

- `holding_review_page.html`：统一复盘页
- `holding_records_page.html`：独立持仓历史记录页
- `holding_status_refresh_page.html`：持仓状态刷新兼容页
- `holding_reanalysis_page.html`：二次分析 / 再评估页
- `add_position_decision_page.html`：统一买卖决策页
- `reduce_position_decision_page.html`：减仓决策兼容页
- `sell_decision_page.html`：卖出决策兼容页
- `weekly_holding_review_page.html`：周复盘兼容页
- `monthly_holding_review_page.html`：月复盘兼容页
- `quarterly_holding_review_page.html`：季度复盘兼容页

## 设计目标

这些页面不是最终可运行页面，而是用于：

1. 对齐业务方案中的页面结构
2. 提供新旧功能并存时的兼容入口原型
3. 统一页面风格和布局语言
4. 为后续真实模板开发提供视觉和信息架构参考

## 风格参考

本次原型主要参考当前项目已有页面风格，尤其接近：

- `templates/result.html`
- `templates/stock_ai.html`

提取的主要视觉特征包括：

- 紫蓝色渐变背景
- 白色半透明卡片
- 大标题 + 副标题
- 统计卡片（Score Cards）
- 左侧树状导航（共享渲染 + 可折叠）
- 顶部参数区 / 顶部筛选条
- 表格列表 + 记录卡片混合布局

## 页面说明

### 0. 选股功能兼容页

重点体现：

- 在当前系统左侧导航中新增“选股功能”分组
- 左侧导航改为共享脚本渲染，一级分组支持折叠/展开
- 新增“系统配置”分组，集中承接 AI 配置与业务配置查看
- 统一承接买前研究、筛选与分析类入口
- 新页面只负责兼容导航、参数录入、摘要展示和历史记录查看
- 不替换旧能力入口，仍保留：
  - `/`、`/stockSelector`
  - `/stock`
  - `/stock_ai`

4 个兼容页的职责分工：

- `stock_screener_page.html`：承接老一体化页面中的股票筛选能力
- `batch_analysis_page.html`：承接老一体化页面中的批量分析能力
- `single_stock_analysis_page.html`：承接当前 AI 单股分析入口
- `single_stock_analysis_legacy_page.html`：提供跳转到 `/stock` 的经典单股分析兼容入口

### 1. 关注股票列表页

重点体现：

- 每只股票有三个核心按钮
  - 进场决策
  - 股票分析
  - 持仓计划分析
- 页面下半区提供同级历史记录列表
  - 进场决策记录
  - 股票分析记录
  - 持仓计划分析记录
- 页面按钮已映射到独立原型路由

### 2. 持仓股票列表页

重点体现：

- 列表页先展示“当前摘要”，下半区再展示“Tab + 列表”的历史记录区
- 持仓与成本区按“累计持仓数量 + 持仓均价 + 多次买入批次 + 买卖明细”设计
- 持仓栏目导航只保留 5 个子页面
  - 持仓列表
  - 二次分析
  - 买卖决策
  - 复盘
  - 历史记录
- 其中：
  - 买卖决策页内再区分 补仓 / 减仓 / 卖出
  - 复盘页内再选择 通用 / 周 / 月 / 季度
- 历史记录页统一承接：二次分析记录、买卖决策记录、复盘记录
- 历史记录已从持仓列表页内锚点调整为独立页面，便于按标的和记录类型集中检索
- 当前原型仍由 `misc.py` 返回 `doc/ui/*.html`，后续真实化实现应迁移到真实模板与 API

### 3. 整体分析和复盘页

重点体现：

- 组合层指标总览
- 行业分布与风险暴露
- 交易质量统计
- 整体分析与整体复盘记录列表

## 路由映射说明

当前 Flask 原型已通过 `src/stock_analyse/interfaces/web/routes/misc.py` 提供以下访问入口：

### 选股功能兼容路由

- `/stock-screener` -> `stock_screener_page.html`
- `/batch-analysis` -> `batch_analysis_page.html`
- `/single-stock-analysis` -> `single_stock_analysis_page.html`
- `/single-stock-analysis-legacy` -> `single_stock_analysis_legacy_page.html`

### 系统配置路由

- `/ai-config` -> `ai_config_page.html`
- `/business-config` -> `business_config_page.html`
- `/api/config/ai` -> AI 配置 JSON（掩码后）
- `/api/config/business` -> 业务配置 JSON（掩码后）

### 主页面路由

- `/index` / `/watch-stocks` -> `watch_stocks_page.html`
- `/holding-stocks` -> `holding_stocks_page.html`
- `/portfolio-review` -> `portfolio_review_page.html`

### 关注股票动作路由

- `/entry-decision` -> `entry_decision_page.html`
- `/stock-analysis-record` -> `stock_analysis_page.html`
- `/trade-plan-analysis` -> `trade_plan_analysis_page.html`

### 持仓股票动作路由

- `/holding-stocks` -> `holding_stocks_page.html`
- `/holding-reanalysis` -> `holding_reanalysis_page.html`
- `/position-decision` -> `add_position_decision_page.html`（统一买卖决策页）
- `/holding-review` -> `holding_review_page.html`（统一复盘页，页面内选择周 / 月 / 季度）
- `/holding-records` -> `holding_records_page.html`（独立持仓历史记录页）

兼容保留路由：

- `/add-position-decision`
- `/reduce-position-decision`
- `/sell-decision`
- `/holding-status-refresh`
- `/weekly-holding-review`
- `/monthly-holding-review`
- `/quarterly-holding-review`

## 使用方式

优先通过已接入的 Flask 路由进行查看，也可以直接打开 HTML 文件。

例如：

```bash
xdg-open http://192.168.1.12:38080/index
xdg-open http://192.168.1.12:38080/stock-screener
xdg-open http://192.168.1.12:38080/batch-analysis
xdg-open http://192.168.1.12:38080/single-stock-analysis
xdg-open http://192.168.1.12:38080/single-stock-analysis-legacy
xdg-open http://192.168.1.12:38080/ai-config
xdg-open http://192.168.1.12:38080/business-config
xdg-open http://192.168.1.12:38080/holding-stocks
xdg-open http://192.168.1.12:38080/holding-review
```

## 后续建议

如果下一步进入真实前端开发，建议按以下顺序推进：

1. 先把主页面和动作页面拆成共享布局模板
2. 再把统计卡片、顶部筛选条、参数栏、记录卡片、按钮组抽成可复用组件
3. 最后对接真实数据结构和接口
