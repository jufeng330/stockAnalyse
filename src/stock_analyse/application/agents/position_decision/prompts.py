from __future__ import annotations

import json

from .models import PositionDecisionInput


POSITION_DECISION_SYSTEM_PROMPT = (
    '你是股票分析师。请基于持仓、财报、成交与持仓计划信息，输出严格结构化的买卖决策结果。'
)


def build_position_decision_user_prompt(data: PositionDecisionInput) -> str:
    payload = data.to_prompt_payload()
    return (
        '请根据输入上下文生成一份结构化买卖决策草案。\n\n'
        '输入参数说明：\n'
        '- holding_stock: 当前持仓主体信息。\n'
        '- watch_stock: 关联关注股票信息。\n'
        '- request: 本次分析请求参数，例如 trade_date、analysis_depth。\n'
        '- financial_context: 公司画像、财务指标、财报信息。\n'
        '- trade_history_context: 持仓成交记录、持仓批次、市场快照。\n'
        '- holding_plan_context: 当前持仓计划及历史计划。\n'
        '- supporting_context: 历史股票分析、历史进场决策等辅助信息。\n'
        '- data_source: 数据来源标识。\n\n'
        '输出要求：\n'
        '1. 只返回一个 JSON 对象。\n'
        '2. recommended_action 只能是 buy、reduce、sell、watch。\n'
        '3. decision_status 只能是 buy_candidate、reduce_candidate、sell_candidate、observe。\n'
        '4. confidence 只能是 high、medium、low。\n'
        '5. tabs 必须固定为 5 个，顺序是：触发条件、核心理由、执行注意事项、风险分析、结论。\n'
        '6. 每个 tab 必须包含 id、title、summary、evidence。\n'
        '7. “结论” tab 必须综合前四个 tabs，给出最终推荐动作和置信度。\n\n'
        f'输入上下文：\n{json.dumps(payload, ensure_ascii=False, indent=2, default=str)}'
    )
