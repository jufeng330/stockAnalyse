# Skill 设计方案: stock-technical

## 概述

股票技术指标计算 Skill，提供各种技术分析指标计算和交易信号生成。

## 功能范围

1. **趋势指标**
   - 移动平均线 (MA)
   - MACD 指标
   - ADX 趋向指标
   - SAR 抛物线转向

2. **震荡指标**
   - RSI 相对强弱指标
   - KDJ 随机指标
   - 威廉指标 (Williams %R)

3. **波动指标**
   - 布林带 (Bollinger Bands)
   - ATR 真实波幅

4. **成交量指标**
   - OBV 能量潮
   - 成交量比率

5. **复合策略**
   - 突破策略
   - 均值回归策略
   - 次级结构套利

## 输入参数

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| action | string | 是 | 指标类型：ma/macd/rsi/kdj/bollinger/breakout/sar/mean_reversion/williams/adx |
| market | string | 是 | 市场代码：SH/SZ/H/usa/zq |
| symbol | string | 是 | 股票代码 |
| start_date | string | 否 | 开始日期，默认最近一年 |
| end_date | string | 否 | 结束日期，默认今天 |
| params | object | 否 | 指标参数配置 |

## 输出格式

```json
{
  "success": true,
  "data": {
    "symbol": "601318",
    "indicator": "macd",
    "signals": [...],
    "last_signal": "buy"
  },
  "message": ""
}
```

## 使用示例

```bash
# 计算 MACD 指标
python main.py --action=macd --market=SH --symbol=601318

# 计算 RSI 指标
python main.py --action=rsi --market=SH --symbol=601318 --params='{"period": 14}'

# 计算布林带
python main.py --action=bollinger --market=SH --symbol=601318

# 获取所有指标
python main.py --action=all --market=SH --symbol=601318
```

## 信号说明

| 信号值 | 含义 |
|--------|------|
| 1 | 买入信号 |
| -1 | 卖出信号 |
| 0 | 无信号 |

## 依赖

- stock_analyse.stocklib.stock_ak_indicator
- stock_analyse.stocklib.stock_company
