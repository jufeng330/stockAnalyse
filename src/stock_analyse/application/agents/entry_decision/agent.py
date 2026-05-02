from __future__ import annotations

import json
from typing import Any

from stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer

from .models import EntryDecisionInput, EntryDecisionRoleOutputMap, EntryDecisionSummaryInput
from .prompts import (
    ENTRY_DECISION_ROLE_CONFIG,
    ENTRY_DECISION_SUMMARY_SYSTEM_PROMPT,
    build_entry_decision_role_user_prompt,
    build_entry_decision_summary_user_prompt,
)


class EntryDecisionAgent:
    """进场决策链路的 AI 访问器。

    用于关注股票列表的进场优化场景，负责执行单个角色分析和最终 markdown 汇总，不处理会话、暂停或持久化。
    """

    def run_role(
        self,
        *,
        data: EntryDecisionInput,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> dict[str, Any]:
        prompt = build_entry_decision_role_user_prompt(data)
        instruction = system_prompt or ENTRY_DECISION_ROLE_CONFIG[data.target_role]['instruction']
        analyzer = StockAiAnalyzer(
            system_prompt=instruction,
            prompt_template='{content}',
            ai_platform=llm_provider,
            model=llm_model,
            api_token=api_code,
        )
        response = analyzer.openai_api_call(
            symbol=data.watch_stock.get('stock_code', ''),
            message=prompt,
            instruction=instruction,
        )
        return self._parse_role_output(response)

    def build_summary_markdown(
        self,
        *,
        data: EntryDecisionSummaryInput,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> str:
        instruction = system_prompt or ENTRY_DECISION_SUMMARY_SYSTEM_PROMPT
        analyzer = StockAiAnalyzer(
            system_prompt=instruction,
            prompt_template='{content}',
            ai_platform=llm_provider,
            model=llm_model,
            api_token=api_code,
        )
        prompt = build_entry_decision_summary_user_prompt(data)
        response = analyzer.openai_api_call(
            symbol=data.watch_stock.get('stock_code', ''),
            message=prompt,
            instruction=instruction,
        )
        return self._normalize_markdown_response(response, data.template_markdown)

    def _parse_role_output(self, raw_content: Any) -> dict[str, Any]:
        if isinstance(raw_content, dict):
            return EntryDecisionRoleOutputMap.model_validate(raw_content).model_dump(mode='json')
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
