# 股票分析应答示例

## 输入参数说明
- `stock_code`: 股票代码
- `market`: 市场标识
- `trade_date`: 分析日期
- `analysis_depth`: 分析深度
- `analysis_scene`: 固定为 `stock_analysis`
- `include_technical`: 是否包含技术面快照
- `include_sentiment`: 是否包含情绪面快照

## 标准输出 JSON 示例
```json
{
  "success": true,
  "data": {
    "stock_code": "600519",
    "market": "SH",
    "trade_date": "2026-04-30",
    "analysis_mode": "agentic",
    "decision": {
      "action": "watch",
      "stance": "等待确认",
      "confidence": 0.67,
      "risk_level": "medium",
      "summary": "趋势未坏，但当前位置不适合追高。",
      "logic": "技术面偏强，基本面确定性高，但短线赔率一般，等待更优介入位。",
      "position_suggestion": {
        "target_position": "0%-20%",
        "add_condition": "回踩确认后再加仓",
        "reduce_condition": "信号走弱时继续观望或减仓",
        "stop_loss_reference": "跌破观察区下沿时止损"
      },
      "time_horizon": "3-10 trading days"
    },
    "scores": {
      "technical": 71.5,
      "fundamental": 78.0,
      "sentiment": 62.0,
      "composite": 69.8
    },
    "signals": ["趋势保持多头", "成交量未明显失控"],
    "risks": ["估值偏高时回撤可能放大"],
    "evidence": ["技术面评分 71.5", "基本面分析师偏正面"],
    "stance": "等待确认",
    "logic": "技术面偏强，基本面确定性高，但短线赔率一般，等待更优介入位。",
    "position_suggestion": {
      "target_position": "0%-20%",
      "add_condition": "回踩确认后再加仓",
      "reduce_condition": "信号走弱时继续观望或减仓",
      "stop_loss_reference": "跌破观察区下沿时止损"
    },
    "time_horizon": "3-10 trading days",
    "final_state": {
      "analyst_outputs": {
        "market": {"summary": "市场中性偏稳"},
        "fundamentals": {"summary": "业绩确定性较强"},
        "news": {"summary": "近期无明显负面"}
      },
      "research_outputs": {
        "bull": {"summary": "高质量资产可继续跟踪"},
        "bear": {"summary": "需防估值回落"}
      },
      "manager_outputs": {
        "research_manager": {"summary": "维持观察名单"},
        "risk_manager": {"summary": "风险中等"}
      },
      "trader_output": {
        "summary": {
          "action": "watch",
          "logic": "等待更优赔率",
          "stance": "等待确认"
        }
      }
    },
    "meta": {
      "duration_ms": 2840,
      "stages": [
        {"stage": "snapshot_ready"},
        {"stage": "analysts_finished"},
        {"stage": "research_finished"},
        {"stage": "managers_finished"},
        {"stage": "trader_finished"},
        {"stage": "decision_ready"}
      ]
    },
    "snapshot": {}
  }
}
```

## 字段说明
- `scores`: 页面分数卡来源，必须保留 `technical / fundamental / sentiment / composite`
- `final_state`: 页面角色详情 tabs 的来源
- `decision`: 历史记录与摘要卡的主字段
