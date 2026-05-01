# 进场优化应答示例

## 输入参数说明
- `watch_stock`: 关注股票主体信息
- `request.trade_date`: 分析日期
- `auto_context`: 系统自动构建的财报、技术、情绪、市场快照
- `manual_inputs.position_input`: 用户补充的当前仓位与目标仓位
- `role_outputs`: 六个阶段的历史输出，供恢复执行时继续复用

## 标准输出 JSON 示例
```json
{
  "success": true,
  "data": {
    "session_id": "entry_abc123",
    "watch_stock_id": "watch_01",
    "stock_code": "600900",
    "stock_name": "长江电力",
    "market": "SH",
    "trade_date": "2026-04-30",
    "macro_analysis": {
      "macro_view": "防御风格占优",
      "macro_conclusion": "当前环境适合偏稳健资产"
    },
    "asset_classification": {
      "asset_classification": "高股息防御资产",
      "recommended_playbook": "分批低吸，不追高"
    },
    "value_stage_analysis": {
      "current_stage": "成熟稳健期",
      "stage_reasoning": "盈利稳定，预期弹性有限但确定性较高"
    },
    "price_zone_analysis": {
      "price_zone": "合理偏高区",
      "action_signal": "等待回踩"
    },
    "buy_plan_analysis": {
      "suggested_action": "等待第一笔",
      "suggested_entry_leg": "第一笔",
      "max_target_position": "30%"
    },
    "risk_control_analysis": {
      "risk_level": "medium",
      "conclusion_summary": "只在回踩确认后执行，失守支撑则放弃。",
      "decision_card": {
        "current_stage": "成熟稳健期",
        "current_price_zone": "合理偏高区",
        "suggested_action": "等待第一笔",
        "suggested_entry_leg": "第一笔",
        "max_target_position": "30%",
        "execution_summary": "当前位置不追，等回踩确认后开第一笔。"
      }
    },
    "decision_card": {
      "current_stage": "成熟稳健期",
      "current_price_zone": "合理偏高区",
      "suggested_action": "等待第一笔",
      "suggested_entry_leg": "第一笔",
      "max_target_position": "30%",
      "execution_summary": "当前位置不追，等回踩确认后开第一笔。"
    },
    "entry_decision_summary_markdown": "## Step 1 市场前置判断\n\n[x] 当前值得研究\n\n---\n\n## Step 10 最终一页决策卡\n\n- 当前阶段：成熟稳健期\n- 当前价格区：合理偏高区\n- 建议动作：等待第一笔\n- 执行摘要：当前位置不追，等回踩确认后开第一笔。",
    "meta": {
      "status": "completed",
      "current_role": "risk_control_analysis",
      "completed_roles": [
        "macro_analysis",
        "asset_classification",
        "value_stage_analysis",
        "price_zone_analysis",
        "buy_plan_analysis",
        "risk_control_analysis"
      ]
    }
  }
}
```

## Pause 事件示例
```json
{
  "event": "decision_pause",
  "data": {
    "session_id": "entry_abc123",
    "watch_stock_id": "watch_01",
    "current_role": "buy_plan_analysis",
    "missing_fields": [
      "position_input.current_position",
      "position_input.max_target_position"
    ],
    "prompt": "买卖计划缺少仓位信息，请补充后继续。"
  }
}
```

## 字段说明
- `decision_card`: 页面顶部摘要与历史记录提取来源
- `entry_decision_summary_markdown`: 页面 markdown 主渲染内容
- `meta.completed_roles`: 恢复执行时用于确定继续位置
