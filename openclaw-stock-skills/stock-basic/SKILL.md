---
name: "stock-basic"
description: "股票基础数据获取 Skill，提供个股信息、历史行情、市场全景等基础数据查询"
arguments:
  action:
    type: "string"
    description: "必填。操作类型：'info'(基本信息), 'history'(历史行情), 'spot'(实时行情)。"
  market:
    type: "string"
    description: "必填。市场代码：SH(沪), SZ(深), H(港), usa(美)。"
  symbol:
    type: "string"
    description: "股票代码，如 601318。部分 action (如 spot) 可不传此参数。"
  start_date:
    type: "string"
    description: "可选。(格式 YYYYMMDD) 历史查询起始日期。"
  end_date:
    type: "string"
    description: "可选。(格式 YYYYMMDD) 历史查询结束日期，默认为当前日期。"
---

# Stock Basic Skill

提供 A 股、港股及美股的基础数据查询服务，支持个股信息查询、历史 K 线分析、市场全景监控等功能。

## 核心功能模块

* **基础信息**:股票代码查询、名称、所属行业等基本信息 (`info`)。
* **历史行情**:自定义周期的历史 K 线数据获取 (`history`)，支持日/周/月线切换。
* **实时盘口**:沪深港市美股的实时行情快照 (`spot`)，可用于市场监控和策略分析。

## 使用策略

调用此技能处理以下任务:
- "查询贵州茅台的基本信息" -> `action="info", market=SH, symbol=600519`
- "拉一下近一个月的走势图" -> `action="history", market=SH, symbol=600519, start_date=20240201`
- "今天市场怎么样？" -> `action="spot", market=SH`

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

# 美股实时行情
PYTHONPATH=/home/inspur/codes/stockAnalyse/src uv run scripts/main.py --action=spot --market=usa

# 港股实时行情
PYTHONPATH=/home/inspur/codes/stockAnalyse/src uv run scripts/main.py --action=spot --market=H
```

## 参数说明

- `--action`:操作类型 (info/history/spot)
- `--market`:市场代码 (SH/SZ/H/usa)
- `--symbol`:股票代码 (部分 action 可选)
- `--start_date` / `--end_date`:日期范围 (YYYYMMDD)

## 返回值

JSON 格式数据，包含 success/data/message 字段。
