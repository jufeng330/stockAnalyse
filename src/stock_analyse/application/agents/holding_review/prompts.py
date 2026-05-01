from __future__ import annotations

import json

from .models import HoldingReviewInput


HOLDING_REVIEW_SYSTEM_PROMPT = (
    '你是交易专家。请基于成交、原始决策、复盘历史、财报和市场信息，输出严格结构化的持仓复盘结果。'
)


def build_holding_review_user_prompt(data: HoldingReviewInput) -> str:
    payload = data.to_prompt_payload()
    return (
        '请根据输入上下文生成一份结构化持仓复盘草案。\n\n'
        '输入参数说明：\n'
        '- holding_stock: 当前持仓主体信息。\n'
        '- watch_stock: 关联关注股票信息。\n'
        '- request: 本次复盘请求参数，例如 trade_date、review_type、period_key、analysis_depth。\n'
        '- trade_history_context: 成交记录、批次和最近交易步骤。\n'
        '- entry_context: 原始进场决策及其历史。\n'
        '- reanalysis_context: 二次分析及其历史。\n'
        '- position_decision_context: 买卖决策记录及其历史。\n'
        '- financial_context: 公司画像、财务指标、财报信息。\n'
        '- market_context: 技术面、情绪面、市场环境和新闻。\n'
        '- review_focus_context: 当前仓位和复盘关注点。\n'
        '- data_source: 数据来源标识。\n\n'
        '输出要求：\n'
        '1. 只返回一个 JSON 对象。\n'
        '2. 必须输出 performance_summary、execution_summary、risk_summary、discipline_summary、next_action_summary、conclusion_tag、tabs。\n'
        '3. conclusion_tag 只能是 logic_ok、need_recheck、execution_issue、risk_rising、prepare_reduce、prepare_sell。\n'
        '4. tabs 必须固定为 4 个，顺序是：执行与卖出复盘、结果复盘、方法与纪律、后续动作。\n'
        '5. 每个 tab 必须包含 id、title、summary、evidence。\n'
        '6. “后续动作” tab 必须综合前 3 个 tabs，明确下一步动作建议，并体现 conclusion_tag。\n\n'
        f'输入上下文：\n{json.dumps(payload, ensure_ascii=False, indent=2, default=str)}'
    )
