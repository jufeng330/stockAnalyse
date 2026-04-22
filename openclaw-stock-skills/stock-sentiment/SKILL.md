---
name: "stock-sentiment"
description: "股票情绪分析 Skill，提供新闻情感分析、舆情监控和投资者情绪量化指标计算功能"
arguments:
  action:
    type: "string"
    description: "必填。操作类型：'news'(新闻情感), 'sentiment'(情绪指数), 'monitor'(舆情监控)。示例：'news'。"
  symbol:
    type: "string"
    description: "必填。股票代码，如'601318'。"
  market:
    type: "string"
    description: "可选。市场代码：SH(沪), SZ(深)。默认值：SZ。"
  days:
    type: "integer"
    description: "可选。查询天数范围，默认分析最近 7 天数据。默认值：7。"
---
