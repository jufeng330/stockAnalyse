# Skill 设计方案: stock-wave

## 概述

股票波浪分析 Skill，提供股价波浪形态识别和趋势分析。

## 功能范围

1. **波浪识别**
   - 波峰识别
   - 波谷识别
   - 波浪周期计算

2. **趋势分析**
   - 整体趋势判断
   - 当前趋势状态
   - 趋势转折点识别

3. **波浪可视化**
   - 波浪图生成
   - 转折点标记

## 输入参数

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| action | string | 是 | 操作类型：analyze/trend/visualize |
| market | string | 是 | 市场代码 |
| symbol | string | 是 | 股票代码 |
| days | int | 否 | 分析天数，默认200 |

## 输出格式

```json
{
  "success": true,
  "data": {
    "symbol": "601318",
    "total_trend": "上升",
    "last_trend": "翻转中",
    "waves": [...]
  }
}
```

## 依赖

- stock_analyse.stocklib.stock_wave_analyser
- stock_analyse.stocklib.stock_company
