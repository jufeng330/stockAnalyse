# stockAnalyse

一个以 `src/stock_analyse` 为业务核心的股票分析项目，包含：

- 单只股票技术分析与 AI 分析
- 全市场选股与批量扫描
- Web 页面与 SSE 流式分析接口
- 情绪分析、财务筛选、行业/概念辅助分析
- 旧入口兼容层

## 当前架构

核心代码已经收敛到 `src/stock_analyse`：

```text
src/stock_analyse/
├── application/     # 用例、工作流、orchestrator
├── domain/          # 领域策略与服务
├── infrastructure/  # 配置、LLM、数据源、持久化
├── interfaces/      # Web 接口、路由、服务、SSE
└── shared/          # 共享能力
```

### 关键目录

- `src/stock_analyse/application/orchestrators/`
  - 编排股票分析与选股主流程
- `src/stock_analyse/application/workflows/`
  - 技术分析、全市场扫描、回测、分红分析等工作流
- `src/stock_analyse/domain/strategies/`
  - 选股策略与财务筛选核心逻辑
- `src/stock_analyse/interfaces/web/`
  - Flask 应用工厂、路由、服务、SSE 管理
- `stockAI/`
  - AI 分析适配与提示词拼装
- `stocklib/`
  - 行情、公司信息、公告、新闻、指标等数据侧能力
- `scanner/`
  - 仍保留的兼容/遗留入口

## 当前入口

### 1. Web 服务

原始 Web 入口仍可用：

```bash
source venv/bin/activate
python stock_web.py
```

默认监听：

- `http://0.0.0.0:38080`

对应文件：

- `stock_web.py`
- `src/stock_analyse/interfaces/web/app.py`
- `src/stock_analyse/interfaces/web/routes/`

### 2. 全市场扫描

兼容入口：

```bash
source venv/bin/activate
python main.py
```

当前 `main.py` 已转发到：

- `src/stock_analyse/application/use_cases/run_full_market_scan.py`

### 3. 命令行主入口

```bash
source venv/bin/activate
python -m stockAnalyse
```

说明：

- `__main__.py` 仍保留命令行演示/调试入口
- 已迁移完成的技术分析、分红分析、财务筛选、财务报表筛选已直接切到 `src`
- `scanner/stock_financial_analyser.py` 与 `scanner/stock_report_analyser.py` 仍保留，但当前只作为兼容壳层存在

## 兼容层说明

本次重构后，并不是所有 `scanner/` 文件都可以直接删除。

### 已经属于兼容层的旧入口

这些能力的核心实现已经迁入 `src`：

- `scanner/stock_analyzer.py`
- `scanner/stock_fh_analyser.py`
- `scanner/stock_result_utils.py`
- `scanner/stock_select_strategy.py`
- `scanner/top_stock_scanner.py`（主流程已逐步转发到 `src` 工作流）

### 仍保留的兼容壳层文件

以下文件当前仍对外保留旧类名/旧导入路径，但核心逻辑已经转发到 `src`，暂时不建议直接删除：

- `scanner/stock_financial_analyser.py`
- `scanner/stock_report_analyser.py`

## 环境准备

建议使用你当前项目里实际在用的 Python 环境：

```bash
source venv/bin/activate
```

安装依赖：

```bash
pip install -r requirements.txt
```

## 配置

项目配置主要来自：

- `config.json`
- `config/`
- `src/stock_analyse/infrastructure/config/settings.py`

你至少需要确认：

- AI 平台配置（如 `qwen` / `openai`）
- API Key
- Web 鉴权配置
- 数据库配置（如果启用 MySQL 读写）

如果 MySQL 未启动，部分流程会退回缓存路径，但相关数据可能不完整。

## 测试

当前已验证的一组核心回归测试：

```bash
source venv/bin/activate
python -m unittest \
  tests.application.orchestrators.test_stock_analysis_orchestrator \
  tests.application.orchestrators.test_stock_selection_calculate_score \
  tests.application.orchestrators.test_stock_selection_orchestrator \
  tests.application.workflows.test_full_market_scan_workflow \
  tests.domain.strategies.test_selection_strategy_service \
  tests.stocklib.test_stock_sentiment_analysis \
  tests.stock_ai.test_stock_ai_analyzer -v
```

这组测试覆盖了：

- `src` 编排层是否走新工作流
- 选股与扫描是否已收口到 `src`
- 分红分析是否已切换到 `src`
- 情绪分析回归
- AI 分析流程对空行业资金流的容错

## 常见问题

### 1. Web 页面打不开或模板报错

请确认启动入口是：

- `stock_web.py`

并且 `src/stock_analyse/interfaces/web/app.py` 能正确解析：

- `templates/`
- `static/`

### 2. AI 分析过程中出现外部接口连接错误

常见来源：

- akshare 远程接口被对端断开
- MySQL 未启动
- API Key 未配置

这类问题通常不会再直接导致已修复的 `KeyError: 名称` 或缺失方法问题，但会影响分析结果完整性。

### 3. 为什么 `scanner/` 还在

因为当前还有部分历史公共导入路径和兼容入口需要保留。虽然 `scanner/stock_financial_analyser.py` 与 `scanner/stock_report_analyser.py` 的核心实现已经迁入 `src`，但删除这些壳层文件仍可能影响旧调用方。

## 后续建议

如果继续做下一阶段清理，建议顺序是：

1. 审查是否仍有外部代码依赖 `scanner/__init__.py` 导出的旧类名
2. 逐步减少包级 `scanner` 暴露面
3. 在确认无调用方依赖后，再删除失去价值的旧兼容入口文件
4. 最后将 `scanner/` 缩减为最小兼容集合或彻底移除
