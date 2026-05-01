# 持仓复盘 AI 应答示例

## 输入参数说明
- `holding_stock`: 当前持仓主体信息
- `watch_stock`: 关联关注股票信息
- `request`: 复盘日期、复盘类型、周期、分析深度
- `trade_history_context`: 成交记录、持仓批次、最近交易步骤
- `entry_context`: 原始进场决策与历史
- `reanalysis_context`: 二次分析与历史
- `position_decision_context`: 买卖决策记录与历史
- `financial_context`: 公司画像、财务指标、财报信息
- `market_context`: 技术面、情绪面、市场环境、新闻
- `review_focus_context`: 当前仓位与复盘关注点
- `data_source`: 数据来源标识

## 标准 JSON 输出示例
```json
{
  "performance_summary": "整体收益低于预期，主要受卖点延迟影响。",
  "execution_summary": "执行上存在迟疑，减仓动作不够坚决。",
  "risk_summary": "风险在趋势反转初期识别不足。",
  "discipline_summary": "纪律执行一般，部分动作偏离原计划。",
  "next_action_summary": "后续以修正卖出纪律、优化止盈止损规则为主。",
  "conclusion_tag": "need_recheck",
  "tabs": [
    {
      "id": "execution_review",
      "title": "执行与卖出复盘",
      "summary": "卖出节奏偏慢，未能按照预案及时处理。",
      "evidence": [
        "出现破位后仍继续持有两个交易日。",
        "盘中信号确认后未及时减仓。"
      ]
    },
    {
      "id": "result_review",
      "title": "结果复盘",
      "summary": "最终收益被回撤吞噬，结果不理想。",
      "evidence": [
        "浮盈高点未兑现，最终仅保留小幅收益。",
        "收益回撤幅度明显高于预期。"
      ]
    },
    {
      "id": "discipline_review",
      "title": "方法与纪律",
      "summary": "方法本身可用，但执行纪律不足。",
      "evidence": [
        "入场逻辑基本成立。",
        "离场纪律执行弱于入场纪律。"
      ]
    },
    {
      "id": "next_action",
      "title": "后续动作",
      "summary": "建议复查卖出规则，并在下一次类似信号出现时严格执行减仓动作。",
      "evidence": [
        "当前结论标签为 need_recheck，需要重点复查执行环节。",
        "优先修正止盈/止损触发后的动作一致性。"
      ]
    }
  ]
}
```
