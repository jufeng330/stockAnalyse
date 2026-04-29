---
name: "stock-concept"
description: "股票概念板块分析：提供 A 股、港股及美股的主题概念、行业分类查询。支持按概念名称查成分股、按股票代码查所属板块等功能，适用于主题投资分析和产业链研究。"
arguments:
  action:
    type: "string"
    description: "必填。操作类型：'list_concept'(概念列表), 'list_industry'(行业列表), 'components'(成分股), 'by_stock'(股票所属板块), 'detail'(板块详情)。"
  market:
    type: "string"
    description: "必填。市场代码：SH(沪), SZ(深), H(港), usa(美), zq(债)。"
  name:
    type: "string"
    description: "概念或板块名称，action=list_concept/list_industry 时可选过滤条件。"
  symbol:
    type: "string"
    description: "股票代码，用于查找该股票所属的板块。"
---

# Stock Concept Skill

基于行业分类与主题概念数据库的概念分析工具，旨在为 LLM 提供结构化的板块关联数据。

## 核心功能模块

* **板块概览**:系统性地列出所有概念板块 (`list_concept`) 与行业板块 (`list_industry`)。
* **成分查询**:获取特定板块的全部成分股列表 (`components`)。
* **反向映射**:根据股票代码查找其所属的所有概念和行业 (`by_stock`)。
* **深度详情**:查看某板块的详细数据、市值分布及历史表现 (`detail`)。

## 使用策略

当用户提到以下词汇时，建议调用此技能:
- "人工智能板块有哪些股票？" -> `action="components", name="人工智能"`
- "腾讯属于什么概念？" -> `action="by_stock", symbol=00700`
- "帮我查一下半导体行业的所有公司" -> `action="list_industry", type="半导体"`

## 开发者说明

- **环境要求**:支持 `python3` 或 `uv` 环境。
- **调用逻辑**:`uv run scripts/main.py --action={action} --market={market} ...`
- **数据格式**:输出始终为 JSON，包含 `success`, `data`, `message` 字段。

```bash
uv run scripts/main.py --action=list_concept --market=SH --name=人工智能
uv run scripts/main.py --action=list_industry --market=H
uv run scripts/main.py --action=components --market=SH --name="新能源汽车"
uv run scripts/main.py --action=by_stock --market=SH --symbol=601318
uv run scripts/main.py --action=detail --market=SZ --name="元宇宙"
```

## 参数说明

- `--action`:操作类型 (list_concept/list_industry/components/by_stock/detail)
- `--market`:市场代码 (SH/SZ/H/usa/zq)
- `--name`:概念或板块名称
- `--symbol`:股票代码

## 返回值

JSON 格式数据，包含 success/data/message 字段。
