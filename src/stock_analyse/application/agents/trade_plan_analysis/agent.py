from __future__ import annotations

import json
from typing import Any

from stock_analyse.infrastructure.llm.langchain_client_factory import build_langchain_chat_model

from .models import TradePlanAnalysisInput, TradePlanAnalysisOutput
from .prompts import TRADE_PLAN_ANALYSIS_SYSTEM_PROMPT, build_trade_plan_analysis_user_prompt


class TradePlanAnalysisAgent:
    """持仓计划分析链路的 AI 访问器。

    用于关注股票列表的持仓计划分析场景，负责根据模板、缓存与补充上下文生成结构化计划结果。
    """

    def run(
        self,
        *,
        data: TradePlanAnalysisInput,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> TradePlanAnalysisOutput:
        llm = build_langchain_chat_model(
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
        )
        prompt = build_trade_plan_analysis_user_prompt(data)
        effective_system_prompt = system_prompt or TRADE_PLAN_ANALYSIS_SYSTEM_PROMPT
        structured_llm = llm.with_structured_output(TradePlanAnalysisOutput)
        try:
            result = structured_llm.invoke([
                {'role': 'system', 'content': effective_system_prompt},
                {'role': 'user', 'content': prompt},
            ])
            if isinstance(result, TradePlanAnalysisOutput):
                return result
            return TradePlanAnalysisOutput.model_validate(result)
        except Exception:
            fallback = llm.invoke([
                {'role': 'system', 'content': effective_system_prompt},
                {'role': 'user', 'content': prompt},
            ])
            content = getattr(fallback, 'content', fallback)
            return self._parse_fallback_output(content)

    def _parse_fallback_output(self, raw_content: Any) -> TradePlanAnalysisOutput:
        text = str(raw_content or '').strip()
        if text.startswith('```'):
            text = text.strip('`').strip()
            if text.lower().startswith('json'):
                text = text[4:].strip()
        try:
            data = json.loads(text)
        except Exception:
            start = text.find('{')
            end = text.rfind('}')
            if start == -1 or end == -1 or end <= start:
                raise ValueError('持仓计划分析 AI 返回内容无法解析为 JSON 对象')
            data = json.loads(text[start:end + 1])
        return TradePlanAnalysisOutput.model_validate(data)
