# Skill 设计方案: stock-news

## 概述

股票新闻情绪 Skill，提供个股新闻获取和市场情绪分析。

## 功能范围

1. **新闻获取**
   - 获取个股相关新闻
   - 获取公司公告
   - 获取研究报告

2. **情绪分析**
   - 基于新闻内容的情绪得分
   - 情绪趋势判断
   - 正负向比例统计

3. **综合分析**
   - 多源新闻汇总
   - 情绪变化追踪
   - 风险提示

## 输入参数

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| action | string | 是 | 操作类型：news/sentiment/comprehensive |
| market | string | 是 | 市场代码 |
| symbol | string | 是 | 股票代码 |
| days | int | 否 | 查询天数，默认15 |

## 输出格式

```json
{
  "success": true,
  "data": {
    "symbol": "601318",
    "sentiment_score": 65.5,
    "trend": "偏向积极",
    "news_count": 25
  }
}
```

## 依赖

- stock_analyse.stocklib.stock_news_data
- stock_analyse.stocklib.stock_sentiment_analysis
