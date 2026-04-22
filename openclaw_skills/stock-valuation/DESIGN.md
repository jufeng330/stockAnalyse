# Skill 设计方案: stock-valuation

## 概述

股票估值模型 Skill，提供 DCF（现金流折现）估值计算和股价区间预测。

## 功能范围

1. **DCF 估值计算**
   - 基于自由现金流的内在价值计算
   - 多情景分析（保守/正常/乐观）
   - 折现率和增长率调整

2. **股价区间预测**
   - 计算股价下限（保守情景）
   - 计算正常股价（基准情景）
   - 计算股价上限（乐观情景）

3. **估值分析**
   - 当前价格与内在价值比较
   - 安全边际计算
   - 投资建议生成

## 输入参数

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| action | string | 是 | 操作类型：dcf/price_range/compare |
| market | string | 是 | 市场代码 |
| symbol | string | 是 | 股票代码 |
| discount_rate | float | 否 | 折现率，默认0.1 |
| growth_rate | float | 否 | 永续增长率，默认0.03 |

## 输出格式

```json
{
  "success": true,
  "data": {
    "symbol": "601318",
    "dcf_value": 85.6,
    "price_range": {
      "conservative": 65.2,
      "normal": 85.6,
      "optimistic": 108.3
    },
    "current_price": 78.5,
    "margin_of_safety": "9.1%"
  }
}
```

## 依赖

- stock_analyse.stocklib.dcf_model
- stock_analyse.stocklib.stock_annual_report
