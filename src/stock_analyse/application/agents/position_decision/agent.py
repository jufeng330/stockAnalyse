from __future__ import annotations

import json
from typing import Any

from stock_analyse.infrastructure.llm.langchain_client_factory import build_langchain_chat_model

from .models import PositionDecisionInput, PositionDecisionOutput
from .prompts import POSITION_DECISION_SYSTEM_PROMPT, build_position_decision_user_prompt


class PositionDecisionAgent:
    """买卖决策链路的 AI 访问器。

    用于持仓股票列表的买卖决策场景，负责把持仓、财报、交易历史与计划上下文转换为固定 tab 的卖出/减仓建议。
    """

    def run(
        self,
        *,
        data: PositionDecisionInput,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> PositionDecisionOutput:
        llm = build_langchain_chat_model(
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
        )
        prompt = build_position_decision_user_prompt(data)
        effective_system_prompt = system_prompt or POSITION_DECISION_SYSTEM_PROMPT
        structured_llm = llm.with_structured_output(PositionDecisionOutput)
        try:
            result = structured_llm.invoke([
                {'role': 'system', 'content': effective_system_prompt},
                {'role': 'user', 'content': prompt},
            ])
            if isinstance(result, PositionDecisionOutput):
                return result
            return PositionDecisionOutput.model_validate(result)
        except Exception:
            fallback = llm.invoke([
                {'role': 'system', 'content': effective_system_prompt},
                {'role': 'user', 'content': prompt},
            ])
            content = getattr(fallback, 'content', fallback)
            return self._parse_fallback_output(content)

    def _parse_fallback_output(self, raw_content: Any) -> PositionDecisionOutput:
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
                raise ValueError('买卖决策 AI 返回内容无法解析为 JSON 对象')
            data = json.loads(text[start:end + 1])
        return PositionDecisionOutput.model_validate(data)
