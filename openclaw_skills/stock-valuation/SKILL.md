# Stock Valuation Skill

股票估值模型 Skill

## 功能

- DCF（现金流折现）估值计算
- 股价区间预测（保守/正常/乐观）
- 当前价格与内在价值比较
- 安全边际分析

## 使用方法

```bash
# DCF 估值计算
python main.py --action=dcf --market=SH --symbol=601318

# 股价区间预测
python main.py --action=price_range --market=SH --symbol=601318

# 估值比较
python main.py --action=compare --market=SH --symbol=601318
```

## 参数说明

- `--discount_rate`: 折现率（默认0.1）
- `--growth_rate`: 永续增长率（默认0.03）
