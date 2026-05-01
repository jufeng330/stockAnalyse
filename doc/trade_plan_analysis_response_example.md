# 持仓计划分析应答示例

## 输入参数说明
- `watch_stock`: 关注股票主体信息
- `request.trade_date`: 分析日期
- `request.plan_type`: 计划类型
- `request.risk_preference`: 风险偏好
- `cache_context`: 当天命中的进场决策、股票分析、历史计划缓存
- `fallback_context`: 缓存不足时的补充上下文
- `template_markdown`: 持仓计划模板正文
- `data_source`: `cache_first | partial_cache_fallback | fallback_only`

## 标准输出 JSON 示例
```json
{
  "trade_plan_markdown": "## 一、计划摘要\n\n- 标的名称：贵州茅台\n- 代码：600519\n- 计划日期：2026-04-30\n- 交易方向：等待第一笔低吸机会\n\n---\n\n## 二、买前约束条件\n\n- 仅在回踩确认区间后执行\n- 单票仓位不超过 30%\n",
  "decision": {
    "action": "watch",
    "summary": "当前位置不追高，等待回踩后的第一笔确认。",
    "logic": "估值不便宜，但趋势未破坏，适合按三笔计划等待回撤介入。",
    "risk_level": "medium",
    "risks": [
      "若回踩期间放量跌破支撑，计划失效",
      "若行业情绪快速转弱，需要延后执行"
    ],
    "time_horizon": "5-15 trading days",
    "position_suggestion": {
      "target_position": "30%",
      "position_limit": "30%",
      "add_condition": "第一笔确认后，突破前高再加第二笔",
      "reduce_condition": "跌回确认位下方时主动减仓",
      "stop_loss_reference": "跌破关键支撑位离场"
    }
  },
  "plan_metadata": {
    "template_name": "持仓计划模板（买前执行版）",
    "data_source": "partial_cache_fallback",
    "cache_hits": [
      "SH_600519_贵州茅台_Strategy_20260430_.md",
      "SH_600519_贵州茅台_analyse_20260430_.md"
    ]
  }
}
```

## 字段说明
- `trade_plan_markdown`: 页面主渲染内容，必须保留模板章节顺序
- `decision`: 页面摘要卡与历史记录提取的核心字段
- `plan_metadata.cache_hits`: 展示本次计划复用了哪些缓存来源
