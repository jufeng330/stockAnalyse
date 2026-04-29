# Skill 设计方案: stock-strategy

## 概述

股票策略评分 Skill，综合多种技术指标计算股票买入评分和投资建议。

## 功能范围

1. **综合评分计算**
   - 波浪趋势分析评分
   - MACD 信号评分
   - 成交量放大评分
   - RSI/KDJ 信号评分
   - 布林带/突破信号评分

2. **投资建议生成**
   - 根据评分生成建议
   - 买入信号检测
   - 风险评估

3. **持续盈利筛选**
   - 连续三年盈利筛选
   - ROE 筛选
   - 负债率筛选

## 输入参数

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| action | string | 是 | 操作类型：score/signals/profitable/recommend |
| market | string | 是 | 市场代码 |
| symbol | string | 条件 | 股票代码 |
| date | string | 否 | 报告日期 |

## 输出格式

```json
{
  "success": true,
  "data": {
    "symbol": "601318",
    "score": 75,
    "recommendation": "建议买入",
    "signals": [...]
  }
}
```

## 评分规则

| 分数 | 建议 |
|------|------|
| >= 50 | 强烈推荐买入 |
| >= 30 | 建议买入 |
| >= 10 | 建议持有 |
| < 10 | 建议观望 |

## 依赖

- stock_analyse.stocklib.stock_strategy
- stock_analyse.stocklib.stock_wave_analyser
- stock_analyse.stocklib.stock_ak_indicator
