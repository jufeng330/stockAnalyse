from __future__ import annotations

import json
from typing import Any

from stock_analyse.infrastructure.llm.langchain_client_factory import build_langchain_chat_model

from .models import HoldingReviewInput, HoldingReviewOutput
from .prompts import HOLDING_REVIEW_SYSTEM_PROMPT, build_holding_review_user_prompt


class HoldingReviewAgent:
    def run(
        self,
        *,
        data: HoldingReviewInput,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> HoldingReviewOutput:
        llm = build_langchain_chat_model(
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
        )
        prompt = build_holding_review_user_prompt(data)
        effective_system_prompt = system_prompt or HOLDING_REVIEW_SYSTEM_PROMPT
        structured_llm = llm.with_structured_output(HoldingReviewOutput)
        try:
            result = structured_llm.invoke([
                {'role': 'system', 'content': effective_system_prompt},
                {'role': 'user', 'content': prompt},
            ])
            if isinstance(result, HoldingReviewOutput):
                return result
            return HoldingReviewOutput.model_validate(result)
        except Exception:
            fallback = llm.invoke([
                {'role': 'system', 'content': effective_system_prompt},
                {'role': 'user', 'content': prompt},
            ])
            content = getattr(fallback, 'content', fallback)
            return self._parse_fallback_output(content)

    def _parse_fallback_output(self, raw_content: Any) -> HoldingReviewOutput:
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
                raise ValueError('持仓复盘 AI 返回内容无法解析为 JSON 对象')
            data = json.loads(text[start:end + 1])
        return HoldingReviewOutput.model_validate(data)
