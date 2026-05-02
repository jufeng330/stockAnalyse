from __future__ import annotations

import json
from typing import Any

from stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer

from .models import PositionDecisionInput, PositionDecisionOutput
from .prompts import POSITION_DECISION_SYSTEM_PROMPT, build_position_decision_user_prompt


class PositionDecisionAgent:
    """买卖决策链路的 AI 访问器。

    用于持仓股票列表的买卖决策场景，负责把持仓、财报、交易历史与计划上下文转换为固定 tab 的卖出/减仓建议。
    """

    _TAB_ID_MAP = {
        'tab_trigger_conditions': 'trigger',
        'tab_core_reasons': 'reason',
        'tab_execution_notes': 'execution',
        'tab_risk_analysis': 'risk',
        'tab_conclusion': 'conclusion',
    }

    _TAB_TITLE_MAP = {
        'trigger': '触发条件',
        'reason': '核心理由',
        'execution': '执行注意事项',
        'risk': '风险分析',
        'conclusion': '结论',
    }

    _TAB_ORDER = ['trigger', 'reason', 'execution', 'risk', 'conclusion']

    _ACTION_MAP = {
        'buy': 'buy',
        'hold': 'watch',
        'watch': 'watch',
        'observe': 'watch',
        'reduce': 'reduce',
        'sell': 'sell',
    }

    _CONFIDENCE_MAP = {
        'high': 'high',
        'medium': 'medium',
        'mid': 'medium',
        'low': 'low',
    }

    _STATUS_MAP = {
        'buy': 'buy_candidate',
        'reduce': 'reduce_candidate',
        'sell': 'sell_candidate',
        'watch': 'observe',
    }

    def run(
        self,
        *,
        data: PositionDecisionInput,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> PositionDecisionOutput:
        prompt = build_position_decision_user_prompt(data)
        effective_system_prompt = system_prompt or POSITION_DECISION_SYSTEM_PROMPT
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

    def _parse_output(self, raw_content: Any) -> PositionDecisionOutput:
        if isinstance(raw_content, dict):
            return self._validate_output(raw_content)
        text = self._strip_code_block(str(raw_content or '').strip())
        return self._validate_output(self._extract_json_object(text))

    def _validate_output(self, raw_content: dict[str, Any]) -> PositionDecisionOutput:
        try:
            return PositionDecisionOutput.model_validate(raw_content)
        except Exception:
            return PositionDecisionOutput.model_validate(self._normalize_output(raw_content))

    def _normalize_output(self, raw: dict[str, Any]) -> dict[str, Any]:
        action = self._ACTION_MAP.get(str(raw.get('recommended_action') or '').strip().lower(), 'watch')
        confidence = self._CONFIDENCE_MAP.get(str(raw.get('confidence') or '').strip().lower(), 'medium')
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
        conclusion_summary = str(raw.get('conclusion_summary') or tabs[-1]['summary'] or '').strip()
        return {
            'recommended_action': action,
            'decision_status': self._STATUS_MAP.get(action, 'observe'),
            'confidence': confidence,
            'conclusion_summary': conclusion_summary or '待确认',
            'tabs': tabs,
        }

    def _stringify_evidence(self, value: Any) -> str:
        if isinstance(value, dict):
            return str(value.get('detail') or value.get('summary') or value.get('description') or value.get('text') or '').strip()
        return str(value or '').strip()

    def _extract_json_object(self, text: str) -> dict[str, Any]:
        try:
            return json.loads(text)
        except Exception:
            start = text.find('{')
            end = text.rfind('}')
            if start == -1 or end == -1 or end <= start:
                raise ValueError('买卖决策 AI 返回内容无法解析为 JSON 对象')
            return json.loads(text[start:end + 1])

    def _strip_code_block(self, text: str) -> str:
        if text.startswith('```'):
            text = text.strip('`').strip()
            if text.lower().startswith('json'):
                text = text[4:].strip()
        return text
