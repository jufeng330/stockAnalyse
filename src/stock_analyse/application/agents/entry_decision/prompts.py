from __future__ import annotations

import json

from .models import EntryDecisionInput, EntryDecisionRoleName, EntryDecisionSummaryInput


ENTRY_DECISION_ROLE_CONFIG: dict[EntryDecisionRoleName, dict[str, str]] = {
    'macro_analysis': {
        'title': '宏观AI分析师',
        'instruction': '你是宏观AI分析师。请基于提供的数据，判断当前市场环境、风格偏好、资金风险偏好和该标的所处宏观适配度。必须严格输出 JSON 对象，字段至少包含: macro_view, macro_conclusion, macro_reasoning, market_style, liquidity_signal, risks, opportunities。',
    },
    'asset_classification': {
        'title': '资产分类AI分析师',
        'instruction': '你是资产分类AI分析师。请判断该标的属于什么资产类型、这类资产主要靠什么上涨、当前适合什么打法。必须严格输出 JSON 对象，字段至少包含: asset_classification, classification_reasoning, upside_logic, risk_logic, recommended_playbook, forbidden_playbook。',
    },
    'value_stage_analysis': {
        'title': '价值阶段AI分析师',
        'instruction': '你是价值阶段AI分析师。请结合系统自动提取的历史财报摘要、预期变化与公司质量，判断当前价值阶段。必须严格输出 JSON 对象，字段至少包含: current_stage, stage_reasoning, revenue_growth_view, profit_growth_view, cashflow_view, margin_trend_view, expectation_view, stage_risks。',
    },
    'price_zone_analysis': {
        'title': '价格分区AI分析师',
        'instruction': '你是价格分区AI分析师。请结合系统自动提取的估值、价格、技术位置和安全边际判断当前价格区间。必须严格输出 JSON 对象，字段至少包含: price_zone, zone_reasoning, action_signal, action_reasoning, valuation_comment, technical_comment, cheap_reason, danger_reason。',
    },
    'buy_plan_analysis': {
        'title': '买卖计划AI分析师',
        'instruction': '你是买卖计划AI分析师。请结合系统自动提取的周期偏好、估值与价格位置，并参考用户给定仓位约束，给出分笔建仓与后续应对计划。必须严格输出 JSON 对象，字段至少包含: suggested_action, action_reasoning, suggested_entry_leg, max_target_position, current_position, buy_plan, rise_plan, fall_plan, sell_rules, execution_notes。',
    },
    'risk_control_analysis': {
        'title': '风险控制AI分析师',
        'instruction': '你是风险控制AI分析师。请输出最终风险约束与决策卡。必须严格输出 JSON 对象，字段至少包含: risk_level, risk_reasoning, key_risks, invalidation_signals, position_constraints, decision_card, conclusion_summary。decision_card 必须包含 current_stage, current_price_zone, suggested_action, suggested_entry_leg, max_target_position, execution_summary。',
    },
}

ENTRY_DECISION_SUMMARY_SYSTEM_PROMPT = (
    '你是进场决策总结AI，也是把研究结论压缩成交易执行卡的编辑器。'
)


def build_entry_decision_role_user_prompt(data: EntryDecisionInput) -> str:
    payload = data.to_prompt_payload()
    role_config = ENTRY_DECISION_ROLE_CONFIG[data.target_role]
    return (
        f"角色: {role_config['title']}\n"
        f"任务说明:\n{role_config['instruction']}\n\n"
        '请仅输出 JSON 对象，不要输出 markdown 代码块，不要输出额外解释。\n\n'
        '输入参数说明：\n'
        '- session_id: 当前分析会话标识。\n'
        '- watch_stock: 当前关注股票主体信息。\n'
        '- request: 本次分析请求参数。\n'
        '- auto_context: 系统自动提取的快照、市场、财报、技术、情绪上下文。\n'
        '- manual_inputs: 用户补充输入，尤其是仓位和执行约束。\n'
        '- completed_role_outputs: 已完成阶段的输出，可供当前阶段复用。\n'
        '- derived_inputs: 从快照加工得到的估值、阶段、投资周期等辅助输入。\n'
        '- target_role: 当前目标角色。\n\n'
        f'上下文数据:\n{json.dumps(payload, ensure_ascii=False, indent=2, default=str)}'
    )


def build_entry_decision_summary_user_prompt(data: EntryDecisionSummaryInput) -> str:
    payload = data.to_prompt_payload()
    return (
        '请根据提供的进场决策模板、6个分析师输出和自动提取数据，生成一份“进场决策实战版” markdown。\n\n'
        '你的目标不是泛泛总结，而是产出一份可以直接给用户执行的买前决策卡，风格必须接近“/mnt/github/stock/进场决策_600900.md”。\n\n'
        '硬性要求：\n'
        '1. 严格保留模板原有章节标题、顺序、编号体系，不允许删节章节，不允许重命名标题。\n'
        '2. 输出必须是完整 markdown 正文，不要输出代码块，不要输出前言，不要输出“以下是结果”这类说明。\n'
        '3. 风格必须像实战版，不像空白表单。\n'
        '4. 能明确判断的字段必须直接填写；仍无法确定时写“待确认”或“不适用”；严禁编造不存在的财务数字、价格区间、事件催化。\n'
        '5. Step 3/4/5/6/7/9/10 这些核心章节必须给出具体动作。\n'
        '6. 最终一页决策卡要让用户一眼看到：当前阶段、当前价格区、建议动作、今天买第几笔、今天买多少、如果不买看什么、涨跌后如何处理。\n\n'
        '输入参数说明：\n'
        '- template_markdown: 原始空白模板。\n'
        '- watch_stock: 当前关注股票主体信息。\n'
        '- auto_context: 自动构建的快照与导出信息。\n'
        '- manual_inputs: 用户补充输入。\n'
        '- role_outputs: 六个阶段的结构化结论。\n\n'
        f'输入数据:\n{json.dumps(payload, ensure_ascii=False, indent=2, default=str)}'
    )
