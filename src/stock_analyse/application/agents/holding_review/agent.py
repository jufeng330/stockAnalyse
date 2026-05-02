from __future__ import annotations

import json
from typing import Any

from stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer

from .models import HoldingReviewInput, HoldingReviewOutput
from .prompts import HOLDING_REVIEW_SYSTEM_PROMPT, build_holding_review_user_prompt


class HoldingReviewAgent:
    """持仓复盘链路的 AI 访问器。

    用于持仓股票列表的复盘场景，负责把成交、计划、二次分析等上下文整合后生成固定 tab 结构的复盘结果。
    """

    _TAB_ID_MAP = {
        'tab_execution_sell_review': 'execution_review',
        'tab_result_review': 'result_review',
        'tab_method_discipline': 'discipline_review',
        'tab_next_action': 'next_action',
    }

    _TAB_TITLE_MAP = {
        'execution_review': '执行与卖出复盘',
        'result_review': '结果复盘',
        'discipline_review': '方法与纪律',
        'next_action': '后续动作',
    }

    _TAB_ORDER = ['execution_review', 'result_review', 'discipline_review', 'next_action']

    def run(
        self,
        *,
        data: HoldingReviewInput,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> HoldingReviewOutput:
        prompt = build_holding_review_user_prompt(data)
        effective_system_prompt = system_prompt or HOLDING_REVIEW_SYSTEM_PROMPT
        analyzer = StockAiAnalyzer(
            system_prompt=effective_system_prompt,
            prompt_template='{content}',
            ai_platform=llm_provider,
            model=llm_model,
            api_token=api_code,
        )
        response = analyzer.openai_api_call(
            symbol=data.holding_stock.get('stock_code', ''),
            message=prompt,
            instruction=effective_system_prompt,
        )
        return self._parse_output(response)

    def _parse_output(self, raw_content: Any) -> HoldingReviewOutput:
        if isinstance(raw_content, dict):
            return self._validate_output(raw_content)
        text = self._strip_code_block(str(raw_content or '').strip())
        return self._validate_output(self._extract_json_object(text))

    def _validate_output(self, raw_content: dict[str, Any]) -> HoldingReviewOutput:
        try:
            return HoldingReviewOutput.model_validate(raw_content)
        except Exception:
            return HoldingReviewOutput.model_validate(self._normalize_output(raw_content))

    def _normalize_output(self, raw: dict[str, Any]) -> dict[str, Any]:
        tabs_by_id: dict[str, dict[str, Any]] = {}
        for item in raw.get('tabs') or []:
            if not isinstance(item, dict):
                continue
            tab_id = self._TAB_ID_MAP.get(str(item.get('id') or '').strip(), str(item.get('id') or '').strip())
            if tab_id not in self._TAB_ORDER:
                continue
            evidence = item.get('evidence') if isinstance(item.get('evidence'), list) else []
            tabs_by_id[tab_id] = {
                'id': tab_id,
                'title': self._TAB_TITLE_MAP[tab_id],
                'summary': str(item.get('summary') or '待确认').strip() or '待确认',
                'evidence': [self._stringify_evidence(x) for x in evidence if self._stringify_evidence(x)] or ['待确认'],
            }
        tabs = [
            tabs_by_id.get(tab_id, {'id': tab_id, 'title': self._TAB_TITLE_MAP[tab_id], 'summary': '待确认', 'evidence': ['待确认']})
            for tab_id in self._TAB_ORDER
        ]
        return {
            'performance_summary': self._summary_text(raw.get('performance_summary')) or tabs[1]['summary'],
            'execution_summary': self._summary_text(raw.get('execution_summary')) or tabs[0]['summary'],
            'risk_summary': self._summary_text(raw.get('risk_summary')) or tabs[1]['summary'],
            'discipline_summary': self._summary_text(raw.get('discipline_summary')) or tabs[2]['summary'],
            'next_action_summary': self._summary_text(raw.get('next_action_summary')) or tabs[3]['summary'],
            'conclusion_tag': str(raw.get('conclusion_tag') or 'need_recheck').strip() or 'need_recheck',
            'tabs': tabs,
        }

    def _stringify_evidence(self, value: Any) -> str:
        if isinstance(value, dict):
            return str(value.get('detail') or value.get('summary') or value.get('description') or value.get('text') or '').strip()
        return str(value or '').strip()

    def _summary_text(self, value: Any) -> str:
        if isinstance(value, dict):
            return str(value.get('summary') or value.get('detail') or value.get('description') or value.get('text') or '').strip()
        return str(value or '').strip()

    def _extract_json_object(self, text: str) -> dict[str, Any]:
        try:
            return json.loads(text)
        except Exception:
            start = text.find('{')
            end = text.rfind('}')
            if start == -1 or end == -1 or end <= start:
                raise ValueError('持仓复盘 AI 返回内容无法解析为 JSON 对象')
            return json.loads(text[start:end + 1])

    def _strip_code_block(self, text: str) -> str:
        if text.startswith('```'):
            text = text.strip('`').strip()
            if text.lower().startswith('json'):
                text = text[4:].strip()
        return text
