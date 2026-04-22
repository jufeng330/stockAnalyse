---
name: "stock-data"
description: "股票数据查询与分析：提供 A 股、港股及美股的实时行情、历史 K 线、财务报表、资金流向和股东变化等全维度数据。支持自定义日期范围，适用于金融研究、基本面分析和市场监测。"
arguments:
  action:
    type: "string"
    description: "必填。操作类型：'info'(基本信息), 'history'(历史行情), 'spot'(实时行情), 'report'(财务报表), 'financial'(指标分析), 'dividend'(分红数据), 'fund_flow'(资金流向), 'holders'(股东持股)。"
  market:
    type: "string"
    description: "必填。市场代码：SH(沪), SZ(深), H(港), usa(美), zq(债)。"
  symbol:
    type: "string"
    description: "股票代码，如 601318。部分 action (如 spot) 可不传此参数。"
  start_date:
    type: "string"
    description: "可选。(格式 YYYYMMDD) 历史查询起始日期。"
  end_date:
    type: "string"
    description: "可选。(格式 YYYYMMDD) 历史查询结束日期，默认为当前日期。"
  date:
    type: "string"
    description: "可选。特定报告日期或分红日期。"
---

# Stock Data Skill

基于专业金融 API 封装的股票数据获取接口，旨在为 LLM 提供结构化的市场情报支持。全面覆盖 A 股、港股与美股全维度数据。

## 核心功能模块

* **行情监测**:实时盘口数据 (`spot`) 与自定义周期历史 K 线 (`history`)。
* **深度财报**:提取标准化资产负债表/利润表/现金流量表 (`report`),支持深度财务比率分析 ([financial](file:///home/inspur/codes/stockAnalyse/backend/main.py#L35-L36))。
* **资本动向**:资金流入流出追踪 (`fund_flow`)、分红配送历史查询 (`dividend`)。
* **治理结构**:大股东持股变化与股东人数统计 ([holders](file:///home/inspur/codes/stockAnalyse/backend/utils/historical_data.py#L125-L142))。

## 使用策略

调用此技能处理以下任务:
- "查询平安银行的市盈率" -> `action="financial", symbol=601318`
- "拉一下近一个月的走势图" -> `action="history", symbol=601318, start_date=20240201`
- "有哪些股票今天分化？" -> `action="dividend"`

## 开发者说明

- **环境要求**:支持 `python3` 或 `uv` 环境。必须设置 `PYTHONPATH` 环境变量包含 `stocklib` 模块路径。
- **调用逻辑**:`PYTHONPATH=/home/inspur/codes/stockAnalyse/src uv run scripts/main.py --action={action} --market={market} ...`
- **数据格式**:输出始终为 JSON，包含 `success`, `data`, `message` 字段。

```bash
# 基本信息查询
PYTHONPATH=/home/inspur/codes/stockAnalyse/src uv run scripts/main.py --action=info --market=SH --symbol=601318

# 历史行情分析 (30 天)
PYTHONPATH=/home/inspur/codes/stockAnalyse/src uv run scripts/main.py --action=history --market=SH --symbol=601318 --start_date=20240201

# 实时盘口数据 (沪深两市全景)
PYTHONPATH=/home/inspur/codes/stockAnalyse/src uv run scripts/main.py --action=spot --market=SH

# 财务报表分析
PYTHONPATH=/home/inspur/codes/stockAnalyse/src uv run scripts/main.py --action=report --market=SH --symbol=601318

# 财务指标分析
PYTHONPATH=/home/inspur/codes/stockAnalyse/src uv run scripts/main.py --action=financial --market=SH --symbol=601318

# 分红查询 (特定日期)
PYTHONPATH=/home/inspur/codes/stockAnalyse/src uv run scripts/main.py --action=dividend --market=SH --date=20240331

# 资金流向追踪
PYTHONPATH=/home/inspur/codes/stockAnalyse/src uv run scripts/main.py --action=fund_flow --market=SH --symbol=601318

# 股东持仓分析
PYTHONPATH=/home/inspur/codes/stockAnalyse/src uv run scripts/main.py --action=holders --market=SH --symbol=601318
```

## 参数说明

- `--action`:操作类型 (info/history/spot/report/financial/dividend/fund_flow/holders)
- `--market`:市场代码 (SH/SZ/H/usa/zq)
- `--symbol`:股票代码 (部分 action 可选)
- `--start_date` / `--end_date`:日期范围 (YYYYMMDD)
- `--date`:特定报告或分红日期 (YYYYMMDD)

## 返回值

JSON 格式数据，包含 success/data/message 字段。
