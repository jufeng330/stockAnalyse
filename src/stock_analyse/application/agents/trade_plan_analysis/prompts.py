from __future__ import annotations

import json

from .models import TradePlanAnalysisInput


TRADE_PLAN_ANALYSIS_SYSTEM_PROMPT = (
    '你是一名股票交易专家，擅长把研究结论转化为可执行的仓位、价格、下单和失败预案。'
)


def build_trade_plan_analysis_user_prompt(data: TradePlanAnalysisInput) -> str:
    payload = data.to_prompt_payload()
    return (
        '请根据给定的模板、缓存文件内容和补充数据，生成一份专业的持仓计划草案。\n\n'
        '输入参数说明：\n'
        '- template_markdown: 持仓计划模板原文。\n'
        '- watch_stock: 当前关注股票主体信息。\n'
        '- request: 本次分析请求参数，例如 trade_date、plan_type、risk_preference。\n'
        '- cache_context: 当天缓存命中的进场决策、股票分析、历史计划等上下文。\n'
        '- fallback_context: 缓存不足时的补充业务上下文。\n'
        '- data_source: 数据来源标识，仅能为 cache_first、partial_cache_fallback、fallback_only。\n\n'
        '输出要求：\n'
        '1. 只返回一个 JSON 对象。\n'
        '2. 必须包含 trade_plan_markdown、decision、plan_metadata 三个顶级字段。\n'
        '3. trade_plan_markdown 必须严格按模板章节顺序组织，不要删除章节标题。\n'
        '4. decision.action 只能是 buy、hold、watch、sell。\n'
        '5. decision.risk_level 只能是 low、medium、high。\n'
        '6. plan_metadata.data_source 只能是 cache_first、partial_cache_fallback、fallback_only。\n'
        '7. 若信息不足，可写“待确认”，但不要编造不存在的数据。\n\n'
        f'输入上下文：\n{json.dumps(payload, ensure_ascii=False, indent=2, default=str)}'
    )
