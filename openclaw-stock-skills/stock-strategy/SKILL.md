---
name: "stock-strategy"
description: "量化策略 Skill，提供多因子评分、选股信号和推荐算法功能，支持价值、成长和质量维度综合评分"
arguments:
  action:
    type: "string"
    description: "必填。操作类型：'score'(评分), 'signal'(信号), 'recommend'(推荐)。示例：'score'。"
  symbol:
    type: "string"
    description: "可选。股票代码，如'601318'。多个代码可用逗号分隔。"
  market:
    type: "string"
    description: "可选。市场代码：SH(沪), SZ(深), H(港), usa(美股)。默认值：SH。"
  strategy_type:
    type: "string"
    description: "可选。策略类型：'value'(价值), 'growth'(成长), 'quality'(质量),'momentum'(动量)。默认值：综合考虑所有维度。"
  start_date:
    type: "string"
    description: "可选。开始日期，格式 YYYY-MM-DD。用于历史回测。"
  end_date:
    type: "string"
    description: "可选。结束日期，格式 YYYY-MM-DD。用于历史回测。"
---
