# Skill 设计方案: stock-concept

## 概述

股票概念板块 Skill，提供行业板块、概念板块查询和成分股分析。

## 功能范围

1. **板块列表查询**
   - 获取所有概念板块
   - 获取所有行业板块
   - 获取板块详情

2. **成分股查询**
   - 获取概念板块成分股
   - 获取行业板块成分股
   - 获取股票所属板块

3. **板块分析**
   - 板块资金流向
   - 板块涨跌幅排行
   - 热点板块识别

## 输入参数

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| action | string | 是 | 操作类型：list_concept/list_industry/components/by_stock/detail |
| market | string | 是 | 市场代码 |
| name | string | 条件 | 板块名称 |
| symbol | string | 条件 | 股票代码 |

## 输出格式

```json
{
  "success": true,
  "data": {
    "sectors": [...],
    "stocks": [...]
  }
}
```

## 依赖

- stock_analyse.stocklib.stock_concept_data
- stock_analyse.stocklib.stock_concept_service
- stock_analyse.stocklib.stock_company
