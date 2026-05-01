# 买卖决策 AI 应答示例

## 输入参数说明
- `holding_stock`: 当前持仓主体信息
- `watch_stock`: 关联关注股票信息
- `request`: 分析日期、分析深度等请求参数
- `financial_context`: 公司画像、财务指标、财报信息
- `trade_history_context`: 成交记录、持仓批次、市场快照
- `holding_plan_context`: 持仓计划与历史计划
- `supporting_context`: 历史股票分析、历史进场决策
- `data_source`: 数据来源标识

## 标准 JSON 输出示例
```json
{
  "recommended_action": "reduce",
  "decision_status": "reduce_candidate",
  "confidence": "medium",
  "conclusion_summary": "短期趋势转弱，建议控制仓位，等待更明确的确认信号。",
  "tabs": [
    {
      "id": "trigger",
      "title": "触发条件",
      "summary": "价格跌破关键支撑并伴随量能放大。",
      "evidence": [
        "最近两个交易日收盘价均位于20日均线下方。",
        "放量下跌，卖压增强。"
      ]
    },
    {
      "id": "reason",
      "title": "核心理由",
      "summary": "基本面未恶化，但短期资金与情绪边际走弱。",
      "evidence": [
        "财务指标未出现显著恶化。",
        "市场情绪指标转弱，资金净流出扩大。"
      ]
    },
    {
      "id": "execution",
      "title": "执行注意事项",
      "summary": "优先分批减仓，避免情绪化一次性卖出。",
      "evidence": [
        "先减仓至目标仓位，再观察反弹强度。",
        "若放量跌破前低，应进一步收缩仓位。"
      ]
    },
    {
      "id": "risk",
      "title": "风险分析",
      "summary": "最大风险在于下跌趋势延续，同时存在超跌反抽扰动。",
      "evidence": [
        "若行业继续走弱，个股可能继续承压。",
        "短期若有消息刺激，可能出现快速反抽。"
      ]
    },
    {
      "id": "conclusion",
      "title": "结论",
      "summary": "建议减仓，当前置信度中等。",
      "evidence": [
        "触发条件和风险信号已出现。",
        "基本面尚未完全破坏，因此不建议直接清仓。"
      ]
    }
  ]
}
```
