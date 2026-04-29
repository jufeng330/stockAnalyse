---
name: "stock-strategy"
description: "量化策略引擎：集成评分、信号和推荐算法，提供从多因子选股到动量交易的系统化工具。基于价值、成长和质量维度综合评分，支持历史回测与实时信号推送。"
arguments:
  action:
    type: "string"
    description: "必填。操作类型：'score'(综合评分), 'signals'(买卖信号), 'profitable'(盈利股筛选), 'recommend'(推荐列表), 'batch'(批量查询)。"
  market:
    type: "string"
    description: "必填。市场代码：SH(沪), SZ(深), H(港), usa(美), zq(债)。"
  symbol:
    type: "string"
    description: "股票代码，用于单个股票评分或信号查询。"
  start_date:
    type: "string"
    description: "可选。(格式 YYMMDD)分析起始日期。"
  end_date:
    type: "string"
    description: "可选。(格式 YYMMDD)分析截止日期;若未提供，默认为当前日期。"
---

# Stock Strategy Skill

智能选股与分析引擎，为基本面研究与投资组合管理提供策略支持。覆盖价值、成长、质量等多维度因子，实现信号识别与回测验证。

## 核心功能模块

* **综合评分**:基于多因子模型 (价值/成长/质量) 评估个股优劣 (`score`)。
* **信号系统**:实时生成买卖点信号，辅助交易决策 (`signals`)。
* **股票筛选**:根据盈利标准或策略规则批量筛选优质股票 (`profitable`, `recommend`)。
* **历史回测**:基于历史数据验证策略有效性 (`batch`，支持参数化查询)。

## 使用策略

调用此技能处理以下任务:
- "评估中国平安的综合评分" -> `action="score", symbol=601318`
- "查询茅台的买卖信号" -> `action="signals", symbol=600519`
- "帮我推荐一些沪市的优质股票" -> `action="recommend", market=SH, n=10`

## 开发者说明

- **环境要求**:支持 `python3` 或 `uv` 环境。
- **调用逻辑**:`uv run scripts/main.py --action={action} --market={market} ...`
- **数据格式**:输出始终为 JSON，包含 `success`, `data`, `message` 字段。

```bash
uv run scripts/main.py --action=score --market=SH --symbol=601318
uv run scripts/main.py --action=signals --market=SZ --symbol=002594
uv run scripts/main.py --action=profitable --market=H --days=30
uv run scripts/main.py --action=recommend --market=SH --limit=10
uv run scripts/main.py --action=batch --market=SH --start_date=20240101 --end_date=20250101
```

## 参数说明

- `--action`:操作类型 (score/signals/profitable/recommend/batch)
- `--market`:市场代码 (SH/SZ/H/usa/zq)
- `--symbol`:股票代码 (部分 action 需要)
- `--start_date` / `--end_date`:日期范围 (YMMDD)

## 返回值

JSON 格式数据，包含 success/data/message 字段。
