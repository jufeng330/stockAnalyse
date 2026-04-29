---
name: "stock-financial"
description: "股票财务分析 Skill，提供财务报表、财务指标和估值计算功能"
arguments:
  action:
    type: "string"
    description: "必填。操作类型：'report'(三大报表), 'indicators'(财务指标), 'valuation'(DCF/DISC 估值)。"
  market:
    type: "string"
    description: "必填。市场代码：SH(沪), SZ(深), H(港), usa(美)。"
  symbol:
    type: "string"
    description: "必填。股票代码，如 601318。"
  years:
    type: "integer"
    description: "可选。(默认 5) 获取历史年份数。"
---

# Stock Financial Skill

提供股票财务报表、财务比率分析和估值计算专业功能，支持三大报表提取、杜邦分析及 DCF/DISC 估值模型。

## 核心技能模块

* **财务报表**:资产负债表、利润表、现金流量表的标准化提取 (`report`)。
* **财务指标**:毛利率、净利率、ROE/ROA、资产负债率等关键比率计算 (`indicators`)。
* **估值模型**:DCF(现金流折现) 和 DISC(fit valuation) 两种估值方法 (`valuation`)。

## 使用策略

调用此技能处理以下任务:
- "获取贵州茅台的财务报表" -> `action="report", market=SH, symbol=600519`
- "分析平安银行的关键财务指标" -> `action="indicators", market=SZ, symbol=000001`
- "给腾讯做一次 DCF 估值" -> `action="valuation", market=H, symbol=0700`

## 开发者说明

- **环境要求**:支持 `python3` 或 `uv` 环境。必须设置 `PYTHONPATH` 环境变量包含 `stocklib` 模块路径。
- **调用逻辑**:`PYTHONPATH=/mnt/github/stock/stockAnalyse/src uv run scripts/main.py --action={action} --market={market} ...`
- **数据格式**:输出始终为 JSON，包含 `success`, `data`, `message` 字段。

```bash
# 资产负债表
PYTHONPATH=/mnt/github/stock/stockAnalyse/src uv run scripts/main.py --action=report --market=SH --symbol=601318

# 财务指标分析
PYTHONPATH=/mnt/github/stock/stockAnalyse/src uv run scripts/main.py --action=indicators --market=SH --symbol=601318

# DCF 估值
PYTHONPATH=/mnt/github/stock/stockAnalyse/src uv run scripts/main.py --action=valuation --market=H --symbol=0700
```

## 参数说明

- `--action`:操作类型 (report/indicators/valuation)
- `--market`:市场代码 (SH/SZ/H/usa)
- `--symbol`:股票代码 (必填)
- `--years`:历史年份数 (默认 5)

## 返回值

JSON 格式数据，包含 success/data/message 字段。
