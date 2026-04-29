---
name: "stock-technical"
description: "技术指标分析 Skill，提供 MA、MACD、RSI、KDJ 等 12+ 种经典指标计算和交易信号生成功能"
arguments:
  action:
    type: "string"
    description: "必填。操作类型：'ma'(移动平均线), 'macd'(MACD), 'rsi'(RSI), 'kdj'(KDJ), 'bollinger'(布林带), 'adx'(ADX), 'sar'(SAR), 'breakout'(突破策略), 'mean_reversion'(均值回归)。示例：'ma'。"
  market:
    type: "string"
    description: "必填。市场代码：SH(沪), SZ(深), H(港), usa(美)。"
  symbol:
    type: "string"
    description: "必填。股票代码，如'601318'或'000001'。"
  start_date:
    type: "string"
    description: "可选。开始日期，格式 YYYYMMDD。默认值：一年前。"
  end_date:
    type: "string"
    description: "可选。结束日期，格式 YYYYMMDD。默认值：当前日期。"
---
