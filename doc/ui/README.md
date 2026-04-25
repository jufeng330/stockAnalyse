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

- `holding_review_page.html`：持仓复盘页
- `holding_status_refresh_page.html`：持仓状态刷新页
- `holding_reanalysis_page.html`：二次分析 / 再评估页
- `add_position_decision_page.html`：补仓决策页
- `reduce_position_decision_page.html`：减仓决策页
- `sell_decision_page.html`：卖出决策页
- `weekly_holding_review_page.html`：周复盘页
- `monthly_holding_review_page.html`：月复盘页
- `quarterly_holding_review_page.html`：季度复盘页

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

- 筛选条件位于“当前持仓标的”上方，采用横向筛选条
- 每只持仓股票有多种动作按钮，并已映射到独立页面
  - 复盘
  - 持仓状态刷新
  - 二次分析 / 再评估
  - 补仓决策
  - 减仓决策
  - 卖出决策
  - 周复盘 / 月复盘 / 季度复盘
- 页面同时提供动作历史记录区

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

- `/holding-review` -> `holding_review_page.html`
- `/holding-status-refresh` -> `holding_status_refresh_page.html`
- `/holding-reanalysis` -> `holding_reanalysis_page.html`
- `/add-position-decision` -> `add_position_decision_page.html`
- `/reduce-position-decision` -> `reduce_position_decision_page.html`
- `/sell-decision` -> `sell_decision_page.html`
- `/weekly-holding-review` -> `weekly_holding_review_page.html`
- `/monthly-holding-review` -> `monthly_holding_review_page.html`
- `/quarterly-holding-review` -> `quarterly_holding_review_page.html`

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
