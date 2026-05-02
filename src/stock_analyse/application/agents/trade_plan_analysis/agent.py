from __future__ import annotations

import json
from typing import Any

from stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer

from .models import TradePlanAnalysisInput, TradePlanAnalysisOutput
from .prompts import TRADE_PLAN_ANALYSIS_SYSTEM_PROMPT, build_trade_plan_analysis_user_prompt


class TradePlanAnalysisAgent:
    """持仓计划分析链路的 AI 访问器。

    用于关注股票列表的持仓计划分析场景，负责根据模板、缓存与补充上下文生成结构化计划结果。
    """

    _ACTION_MAP = {
        'buy': 'buy',
        'hold': 'hold',
        'watch': 'watch',
        'observe': 'watch',
        'sell': 'sell',
        'reduce': 'watch',
    }

    _RISK_LEVEL_MAP = {
        'low': 'low',
        'medium': 'medium',
        'mid': 'medium',
        'high': 'high',
    }

    def run(
        self,
        *,
        data: TradePlanAnalysisInput,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> TradePlanAnalysisOutput:
        prompt = build_trade_plan_analysis_user_prompt(data)
        effective_system_prompt = system_prompt or TRADE_PLAN_ANALYSIS_SYSTEM_PROMPT
        analyzer = StockAiAnalyzer(
            system_prompt=effective_system_prompt,
            prompt_template='{content}',
            ai_platform=llm_provider,
            model=llm_model,
            api_token=api_code,
        )
        response = analyzer.openai_api_call(
            symbol=data.watch_stock.get('stock_code', ''),
            message=prompt,
            instruction=effective_system_prompt,
        )
        return self._parse_fallback_output(response)

    def _parse_fallback_output(self, raw_content: Any) -> TradePlanAnalysisOutput:
        if isinstance(raw_content, dict):
            return self._validate_output(raw_content)
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
        return self._validate_output(data)

    def _validate_output(self, raw_content: dict[str, Any]) -> TradePlanAnalysisOutput:
        try:
            return TradePlanAnalysisOutput.model_validate(raw_content)
        except Exception:
            return TradePlanAnalysisOutput.model_validate(self._normalize_output(raw_content))

    def _normalize_output(self, raw: dict[str, Any]) -> dict[str, Any]:
        raw_decision = raw.get('decision') if isinstance(raw.get('decision'), dict) else {}
        raw_plan_metadata = raw.get('plan_metadata') if isinstance(raw.get('plan_metadata'), dict) else {}
        decision_source = raw_decision.get('logic') if isinstance(raw_decision.get('logic'), dict) else raw_decision
        decision = decision_source if isinstance(decision_source, dict) else raw_decision
        plan_metadata_source = raw_plan_metadata.get('template_name') if isinstance(raw_plan_metadata.get('template_name'), dict) else raw_plan_metadata
        plan_metadata = plan_metadata_source if isinstance(plan_metadata_source, dict) else raw_plan_metadata
        position_suggestion = decision.get('position_suggestion') if isinstance(decision.get('position_suggestion'), dict) else {}
        summary = self._stringify_text(decision.get('summary')) or self._stringify_text(raw_decision.get('summary')) or self._stringify_text(raw.get('summary')) or self._stringify_text(raw.get('conclusion_summary')) or '待确认'
        logic = self._stringify_text(decision.get('logic')) or self._stringify_text(decision.get('reasoning')) or summary
        return {
            'trade_plan_markdown': self._stringify_text(raw.get('trade_plan_markdown')) or self._stringify_text(raw.get('markdown')) or self._stringify_text(raw.get('content')) or '',
            'decision': {
                'action': self._ACTION_MAP.get(str(decision.get('action') or raw_decision.get('action') or raw.get('action') or raw.get('recommended_action') or '').strip().lower(), 'watch'),
                'summary': summary,
                'logic': logic,
                'risk_level': self._RISK_LEVEL_MAP.get(str(decision.get('risk_level') or raw_decision.get('risk_level') or raw.get('risk_level') or '').strip().lower(), 'medium'),
                'risks': self._normalize_string_list(decision.get('risks') or raw_decision.get('risks') or raw.get('risks') or plan_metadata.get('limitations') or []),
                'time_horizon': self._stringify_text(decision.get('time_horizon')) or self._stringify_text(raw_decision.get('time_horizon')) or self._stringify_text(raw.get('time_horizon')) or '',
                'position_suggestion': {
                    'target_position': self._stringify_text(position_suggestion.get('target_position')),
                    'position_limit': self._stringify_text(position_suggestion.get('position_limit')) or self._stringify_text(position_suggestion.get('target_position')),
                    'add_condition': self._stringify_text(position_suggestion.get('add_condition')),
                    'reduce_condition': self._stringify_text(position_suggestion.get('reduce_condition')),
                    'stop_loss_reference': self._stringify_text(position_suggestion.get('stop_loss_reference')),
                },
            },
            'plan_metadata': {
                'template_name': self._stringify_text(plan_metadata.get('template_name')) or self._stringify_text(plan_metadata.get('name')) or '持仓计划模板（买前执行版）',
                'data_source': self._normalize_data_source(plan_metadata.get('data_source') or raw_plan_metadata.get('data_source') or raw.get('data_source')),
                'cache_hits': self._normalize_string_list(plan_metadata.get('cache_hits') or raw_plan_metadata.get('cache_hits') or plan_metadata.get('used_context') or raw_plan_metadata.get('used_context') or []),
            },
        }

    def _normalize_data_source(self, value: Any) -> str:
        normalized = str(value or '').strip()
        if normalized in {'cache_first', 'partial_cache_fallback', 'fallback_only'}:
            return normalized
        return 'fallback_only'

    def _normalize_string_list(self, value: Any) -> list[str]:
        if isinstance(value, list):
            return [item for item in (self._stringify_text(entry) for entry in value) if item]
        text = self._stringify_text(value)
        return [text] if text else []

    def _stringify_text(self, value: Any) -> str:
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            for key in ('summary', 'logic', 'detail', 'description', 'text', 'content', 'action'):
                nested = value.get(key)
                if isinstance(nested, str) and nested.strip():
                    return nested.strip()
            return ''
        if value is None:
            return ''
        return str(value).strip()
