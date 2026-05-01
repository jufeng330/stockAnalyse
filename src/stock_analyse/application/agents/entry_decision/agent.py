from __future__ import annotations

import json
from typing import Any

from stock_analyse.infrastructure.llm.langchain_client_factory import build_langchain_chat_model

from .models import EntryDecisionInput, EntryDecisionRoleOutputMap, EntryDecisionSummaryInput
from .prompts import (
    ENTRY_DECISION_ROLE_CONFIG,
    ENTRY_DECISION_SUMMARY_SYSTEM_PROMPT,
    build_entry_decision_role_user_prompt,
    build_entry_decision_summary_user_prompt,
)


class EntryDecisionAgent:
    def run_role(
        self,
        *,
        data: EntryDecisionInput,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> dict[str, Any]:
        llm = build_langchain_chat_model(
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
        )
        prompt = build_entry_decision_role_user_prompt(data)
        instruction = ENTRY_DECISION_ROLE_CONFIG[data.target_role]['instruction']
        structured_llm = llm.with_structured_output(EntryDecisionRoleOutputMap)
        try:
            result = structured_llm.invoke([
                {'role': 'system', 'content': system_prompt or instruction},
                {'role': 'user', 'content': prompt},
            ])
            if isinstance(result, EntryDecisionRoleOutputMap):
                return result.model_dump(mode='json')
            return EntryDecisionRoleOutputMap.model_validate(result).model_dump(mode='json')
        except Exception:
            fallback = llm.invoke([
                {'role': 'system', 'content': system_prompt or instruction},
                {'role': 'user', 'content': prompt},
            ])
            content = getattr(fallback, 'content', fallback)
            return self._parse_role_output(content)

    def build_summary_markdown(
        self,
        *,
        data: EntryDecisionSummaryInput,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> str:
        llm = build_langchain_chat_model(
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
        )
        prompt = build_entry_decision_summary_user_prompt(data)
        response = llm.invoke([
            {'role': 'system', 'content': system_prompt or ENTRY_DECISION_SUMMARY_SYSTEM_PROMPT},
            {'role': 'user', 'content': prompt},
        ])
        return self._normalize_markdown_response(getattr(response, 'content', response), data.template_markdown)

    def _parse_role_output(self, raw_content: Any) -> dict[str, Any]:
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
                raise ValueError('进场决策阶段 AI 返回内容无法解析为 JSON 对象')
            data = json.loads(text[start:end + 1])
        return EntryDecisionRoleOutputMap.model_validate(data).model_dump(mode='json')

    def _normalize_markdown_response(self, response: Any, fallback_markdown: str) -> str:
        text = str(response or '').strip()
        if text.startswith('```'):
            lines = text.splitlines()
            if lines:
                lines = lines[1:]
            if lines and lines[-1].strip() == '```':
                lines = lines[:-1]
            text = '\n'.join(lines).strip()
        if not text or '发生异常:' in text:
            return fallback_markdown
        return text
