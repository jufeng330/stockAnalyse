---
name: "stock-news"
description: "股票消息与情绪分析：提供实时新闻抓取、舆情监控和投资者情绪量化指标。支持主题识别、情感倾向分析和综合情报推送，为基本面研究提供资讯维度。"
arguments:
  action:
    type: "string"
    description: "必填。操作类型：'news'(新闻查询), 'sentiment'(情绪分析), 'comprehensive'(综合情报)。"
  market:
    type: "string"
    description: "必填。市场代码：SH(沪), SZ(深), H(港), usa(美), zq(债)。"
  symbol:
    type: "string"
    description: "必填。股票代码，如 601318、00700.HK。"
  days:
    type: "int"
    description: "可选。(默认 30)查询天数，用于历史新闻查询。"
---

# Stock News Skill

股票情报分析引擎，为基本面研究与投资决策提供资讯支持。旨在从海量数据中识别主题、追踪舆情异动并量化情绪信号。

## 核心功能模块

* **实时新闻**:抓取个股/行业最新公告与媒体资讯 (`news`)。
* **情绪识别**:基于 NLP 模型计算新闻情感得分，识别利好/利空信号 (`sentiment`)。
* **综合情报**:主题 + 舆情聚合分析，提供可操作的投研建议 (`comprehensive`)。

## 使用策略

调用此技能处理以下任务：
- "平安银行有哪些最新新闻？" -> `action="news", symbol=000001`
- "分析贵州茅台最近一周的消息情绪" -> `action="sentiment", symbol=600519, days=7`
- "帮我全面查询比亚迪的基本面舆情" -> `action="comprehensive", symbol=002594`

## 开发者说明

- **环境要求**:支持 `python3` 或 `uv` 环境。
- **调用逻辑**:`uv run scripts/main.py --action={action} --market={market} --symbol={symbol} ...`
- **数据格式**:输出始终为 JSON，包含 `success`, `data`, `message` 字段。

```bash
uv run scripts/main.py --action=news --market=SH --symbol=601318
uv run scripts/main.py --action=sentiment --market=SZ --symbol=002594 --days=7
uv run scripts/main.py --action=comprehensive --market=H --symbol=00700
```

## 参数说明

- `--action`:操作类型 (news/sentiment/comprehensive)
- `--market`:市场代码 (SH/SZ/H/usa/zq)
- `--symbol`:股票代码
- `--days`:查询天数 (仅用于 news/sentiment)

## 返回值

JSON 格式数据，包含 success/data/message 字段。
