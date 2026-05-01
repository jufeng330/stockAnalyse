# 持仓二次分析应答示例

## 输入参数说明
- `stock_code`: 股票代码
- `market`: 市场标识
- `trade_date`: 分析日期
- `analysis_scene`: 固定为 `holding_reanalysis`
- `holding_stock_id`: 当前持仓记录 ID
- `reanalysis_context`: 持仓、历史进场决策、历史股票分析、历史计划等增强上下文

## 标准输出 JSON 示例
```json
{
  "success": true,
  "data": {
    "stock_code": "600900",
    "market": "SH",
    "trade_date": "2026-04-30",
    "analysis_mode": "agentic",
    "decision": {
      "action": "hold",
      "stance": "继续持有",
      "confidence": 0.72,
      "risk_level": "medium",
      "summary": "趋势未破坏，持仓逻辑仍成立，但不宜在当前位置继续追高。",
      "logic": "当前持仓仍有安全边际，但需要观察下一轮回踩质量。",
      "position_suggestion": {
        "target_position": "10%-30%",
        "add_condition": "回踩确认后小幅加仓",
        "reduce_condition": "跌破观察位或风险事件兑现时降仓",
        "stop_loss_reference": "跌破区间下沿时止损"
      },
      "time_horizon": "3-10 trading days"
    },
    "scores": {
      "technical": 68.0,
      "fundamental": 76.0,
      "sentiment": 58.0,
      "composite": 67.4
    },
    "final_state": {},
    "holding_reanalysis_tabs": [
      {
        "id": "thesis",
        "title": "原逻辑是否仍成立",
        "summary": "核心持有逻辑仍成立，但赔率较首次建仓时下降。"
      },
      {
        "id": "position",
        "title": "当前持仓动作",
        "summary": "继续持有，不追高，等待回踩确认后决定是否加仓。"
      },
      {
        "id": "risk",
        "title": "主要风险",
        "summary": "若跌破关键支撑位，原先持有逻辑需要重估。"
      },
      {
        "id": "next",
        "title": "后续观察点",
        "summary": "重点看量价配合、行业情绪和下一次回踩质量。"
      }
    ],
    "meta": {
      "duration_ms": 3010,
      "stages": [
        {"stage": "snapshot_ready"},
        {"stage": "analysts_finished"},
        {"stage": "research_finished"},
        {"stage": "managers_finished"},
        {"stage": "trader_finished"},
        {"stage": "decision_ready"}
      ]
    }
  }
}
```

## 字段说明
- `analysis_scene=holding_reanalysis`: 用于复用共享 graph 并触发持仓语义处理
- `holding_reanalysis_tabs`: 持仓二次分析页面优先渲染的 tab 数据
- 其余 `decision / scores / final_state / meta` 字段仍与普通股票分析保持兼容
