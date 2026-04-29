# Stock Technical Skill

股票技术指标计算 Skill

## 功能

- 移动平均线 (MA) 策略
- MACD 指标
- RSI 相对强弱指标
- KDJ 随机指标
- 布林带 (Bollinger Bands)
- 突破策略
- SAR 抛物线转向
- 均值回归策略
- 威廉指标
- ADX 趋向指标
- OBV 能量潮
- 获取所有指标

## 使用方法

```bash
python main.py --action=macd --market=SH --symbol=601318
python main.py --action=rsi --market=SH --symbol=601318
python main.py --action=bollinger --market=SH --symbol=601318
python main.py --action=all --market=SH --symbol=601318
```

## 参数说明

- `--action`: 指标类型
- `--market`: 市场代码
- `--symbol`: 股票代码
- `--start_date`: 开始日期
- `--end_date`: 结束日期
- `--params`: 指标参数(JSON格式)

## 返回值

包含指标计算结果和交易信号
