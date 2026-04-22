---
name: "stock-sector"
description: "股票板块概念分析 Skill，提供板块成分股查询、板块分类和关联分析方法"
arguments:
  action:
    type: "string"
    description: "必填。操作类型：'stocks'(查成分股), 'category'(查分类), 'relation'(查关联)。示例：'stocks'。"
  sector_code:
    type: "string"
    description: "必填。板块代码或名称，如行业代码、概念名称。示例：'601318', '新能源'。"
  market:
    type: "string"
    description: "可选。市场代码：SH(沪), SZ(深), H(港)。默认值：SH。"
---
