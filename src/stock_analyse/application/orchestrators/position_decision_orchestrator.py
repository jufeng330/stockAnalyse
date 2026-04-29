from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

from stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer


class PositionDecisionOutputError(ValueError):
    pass


class PositionDecisionOrchestrator:
    def __init__(self, *, analyzer_factory: Any | None = None) -> None:
        self.analyzer_factory = analyzer_factory or (lambda **kwargs: StockAiAnalyzer(**kwargs))

    def run(
        self,
        *,
        context: dict[str, Any],
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
        callbacks: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        callbacks = callbacks or {}
        started_at = time.time()
        send_log = callbacks.get('send_log')
        send_progress = callbacks.get('send_progress')

        def log(message: str, log_type: str = 'info') -> None:
            if send_log:
                send_log(message, log_type)

        def progress(percent: int, message: str) -> None:
            if send_progress:
                send_progress('singleProgress', percent, message)

        analyzer = self.analyzer_factory(
            ai_platform=llm_provider,
            model=llm_model,
            api_token=api_code,
            system_prompt=system_prompt,
        )

        log('🚀 开始准备买卖决策上下文', 'header')
        progress(15, '正在整理财报、成交和持仓计划数据...')
        response = self._run_position_analyst(analyzer, context)
        progress(85, '正在生成买卖决策草案...')
        result = self._build_final_result(context, response, duration_ms=int((time.time() - started_at) * 1000))
        progress(100, '买卖决策草案生成完成')
        return result

    def _run_position_analyst(self, analyzer: Any, context: dict[str, Any]) -> dict[str, Any]:
        payload = {
            'holding_stock': context.get('holding_stock') or {},
            'watch_stock': context.get('watch_stock') or {},
            'request': context.get('request') or {},
            'financial_context': context.get('financial_context') or {},
            'trade_history_context': context.get('trade_history_context') or {},
            'holding_plan_context': context.get('holding_plan_context') or {},
            'supporting_context': context.get('supporting_context') or {},
            'data_source': context.get('data_source') or 'holding_snapshot',
        }
        instruction = (
            '你是股票分析师。请基于财报数据、历史成交数据、持仓计划数据做买卖决策分析。'
            '必须返回结构化 JSON 对象，不要输出 markdown 代码块，不要输出额外解释。'
        )
        message = (
            '请根据输入上下文生成一份结构化买卖决策草案。\n\n'
            '硬性要求：\n'
            '1. 不要预设动作，必须先分析后给出推荐动作。\n'
            '2. 推荐动作 recommended_action 只能是：buy、reduce、sell、watch。\n'
            '3. 必须输出 5 个固定 tabs，顺序必须是：触发条件、核心理由、执行注意事项、风险分析、结论。\n'
            '4. 每个 tab 必须包含：id、title、summary、evidence。\n'
            '5. 每个 tab 的 summary 是顶部结论，evidence 是底部理由列表。\n'
            '6. 最后一个“结论”tab 必须综合前四个 tabs，给出最终推荐动作与置信度。\n\n'
            f'输入上下文：\n{json.dumps(payload, ensure_ascii=False, indent=2, default=str)}'
        )
        raw_response = analyzer.openai_api_call(
            symbol=(context.get('holding_stock') or {}).get('stock_code', ''),
            message=self._build_json_output_message(message),
            instruction=instruction,
            require_tool_call=False,
        )
        parsed = self._parse_json_response(raw_response)
        self._validate_position_decision_payload(parsed)
        return parsed

    def _build_json_output_message(self, base_message: str) -> str:
        schema = self._build_position_decision_tool_schema()['function']['parameters']
        return (
            f"{base_message}\n\n"
            '输出要求：只返回一个 JSON 对象，不要输出 markdown，不要输出额外解释。\n'
            f"JSON Schema:\n{json.dumps(schema, ensure_ascii=False, indent=2)}"
        )

    def _build_position_decision_tool_schema(self) -> dict[str, Any]:
        return {
            'type': 'function',
            'function': {
                'name': 'submit_position_decision',
                'description': '提交买卖决策结构化结果。',
                'parameters': {
                    'type': 'object',
                    'additionalProperties': False,
                    'properties': {
                        'recommended_action': {
                            'type': 'string',
                            'enum': ['buy', 'reduce', 'sell', 'watch'],
                        },
                        'decision_status': {
                            'type': 'string',
                            'enum': ['buy_candidate', 'reduce_candidate', 'sell_candidate', 'observe'],
                        },
                        'confidence': {
                            'type': 'string',
                            'enum': ['high', 'medium', 'low'],
                        },
                        'conclusion_summary': {'type': 'string'},
                        'tabs': {
                            'type': 'array',
                            'minItems': 5,
                            'maxItems': 5,
                            'items': {
                                'type': 'object',
                                'additionalProperties': False,
                                'properties': {
                                    'id': {
                                        'type': 'string',
                                        'enum': ['trigger', 'reason', 'execution', 'risk', 'conclusion'],
                                    },
                                    'title': {
                                        'type': 'string',
                                        'enum': ['触发条件', '核心理由', '执行注意事项', '风险分析', '结论'],
                                    },
                                    'summary': {'type': 'string'},
                                    'evidence': {
                                        'type': 'array',
                                        'items': {'type': 'string'},
                                    },
                                },
                                'required': ['id', 'title', 'summary', 'evidence'],
                            },
                        },
                    },
                    'required': ['recommended_action', 'decision_status', 'confidence', 'conclusion_summary', 'tabs'],
                },
            },
        }

    def _validate_position_decision_payload(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            raise PositionDecisionOutputError('AI 未返回对象结构')
        required_fields = ['recommended_action', 'decision_status', 'confidence', 'conclusion_summary', 'tabs']
        missing = [field for field in required_fields if field not in payload]
        if missing:
            raise PositionDecisionOutputError(f'AI 返回缺少字段: {", ".join(missing)}')
        action = str(payload.get('recommended_action') or '').strip().lower()
        if action not in {'buy', 'reduce', 'sell', 'watch'}:
            raise PositionDecisionOutputError(f'AI 返回了非法 recommended_action: {action or "<empty>"}')
        status = str(payload.get('decision_status') or '').strip()
        if status not in {'buy_candidate', 'reduce_candidate', 'sell_candidate', 'observe'}:
            raise PositionDecisionOutputError(f'AI 返回了非法 decision_status: {status or "<empty>"}')
        confidence = str(payload.get('confidence') or '').strip()
        if confidence not in {'high', 'medium', 'low'}:
            raise PositionDecisionOutputError(f'AI 返回了非法 confidence: {confidence or "<empty>"}')
        tabs = payload.get('tabs')
        if not isinstance(tabs, list) or len(tabs) != 5:
            raise PositionDecisionOutputError('AI 返回的 tabs 数量不是固定 5 个')
        expected = [
            ('trigger', '触发条件'),
            ('reason', '核心理由'),
            ('execution', '执行注意事项'),
            ('risk', '风险分析'),
            ('conclusion', '结论'),
        ]
        for index, (expected_id, expected_title) in enumerate(expected):
            item = tabs[index]
            if not isinstance(item, dict):
                raise PositionDecisionOutputError(f'AI 返回的第 {index + 1} 个 tab 不是对象')
            if str(item.get('id') or '').strip() != expected_id:
                raise PositionDecisionOutputError(f'AI 返回的第 {index + 1} 个 tab id 非法: {item.get("id")}')
            if str(item.get('title') or '').strip() != expected_title:
                raise PositionDecisionOutputError(f'AI 返回的第 {index + 1} 个 tab title 非法: {item.get("title")}')
            if not str(item.get('summary') or '').strip():
                raise PositionDecisionOutputError(f'AI 返回的第 {index + 1} 个 tab summary 为空')
            evidence = item.get('evidence')
            if not isinstance(evidence, list) or not evidence or not all(str(detail).strip() for detail in evidence):
                raise PositionDecisionOutputError(f'AI 返回的第 {index + 1} 个 tab evidence 非法')
        if not str(payload.get('conclusion_summary') or '').strip():
            raise PositionDecisionOutputError('AI 返回的 conclusion_summary 为空')



    def _build_final_result(self, context: dict[str, Any], response: dict[str, Any], *, duration_ms: int) -> dict[str, Any]:
        holding_stock = context.get('holding_stock') or {}
        request = context.get('request') or {}
        tabs = self._normalize_tabs(response.get('tabs'))
        conclusion_summary = str(response.get('conclusion_summary') or '').strip() or (tabs[-1].get('summary') if tabs else '')
        recommended_action = str(response.get('recommended_action') or 'watch').strip().lower() or 'watch'
        decision_status = str(response.get('decision_status') or self._map_decision_status(recommended_action)).strip()
        confidence = str(response.get('confidence') or 'medium').strip() or 'medium'
        evidence = []
        for tab in tabs:
            for item in tab.get('evidence', []):
                text = str(item).strip()
                if text:
                    evidence.append({'tab': tab.get('title', ''), 'detail': text})
        return {
            'success': True,
            'data': {
                'holding_stock_id': holding_stock.get('id', ''),
                'watch_stock_id': (context.get('watch_stock') or {}).get('id', ''),
                'stock_code': holding_stock.get('stock_code', ''),
                'stock_name': holding_stock.get('stock_name', ''),
                'market': holding_stock.get('market', ''),
                'trade_date': request.get('trade_date', ''),
                'analysis_depth': request.get('analysis_depth', 'standard'),
                'decision': {
                    'action': recommended_action,
                    'status': decision_status,
                    'confidence': confidence,
                    'summary': conclusion_summary,
                },
                'tabs': tabs,
                'evidence': evidence,
                'meta': {
                    'role': '股票分析师',
                    'data_source': context.get('data_source') or 'holding_snapshot',
                    'duration_ms': duration_ms,
                },
                'context_snapshot': {
                    'financial_context': context.get('financial_context') or {},
                    'trade_history_context': context.get('trade_history_context') or {},
                    'holding_plan_context': context.get('holding_plan_context') or {},
                },
            },
        }

    def _normalize_tabs(self, value: Any) -> list[dict[str, Any]]:
        expected = [
            ('trigger', '触发条件'),
            ('reason', '核心理由'),
            ('execution', '执行注意事项'),
            ('risk', '风险分析'),
            ('conclusion', '结论'),
        ]
        source = value if isinstance(value, list) else []
        normalized: list[dict[str, Any]] = []
        for index, (default_id, default_title) in enumerate(expected):
            item = source[index] if index < len(source) and isinstance(source[index], dict) else {}
            evidence = item.get('evidence') if isinstance(item.get('evidence'), list) else []
            normalized.append(
                {
                    'id': str(item.get('id') or default_id).strip() or default_id,
                    'title': str(item.get('title') or default_title).strip() or default_title,
                    'summary': str(item.get('summary') or '待确认').strip() or '待确认',
                    'evidence': [str(detail).strip() for detail in evidence if str(detail).strip()] or ['待确认'],
                }
            )
        return normalized

    def _map_decision_status(self, action: str) -> str:
        mapping = {
            'buy': 'buy_candidate',
            'reduce': 'reduce_candidate',
            'sell': 'sell_candidate',
            'watch': 'observe',
        }
        return mapping.get(action, 'observe')

    def _parse_json_response(self, raw_response: Any) -> dict[str, Any]:
        if isinstance(raw_response, dict):
            return raw_response
        text = str(raw_response or '').strip()
        if not text:
            return {}
        if text.startswith('```'):
            text = text.strip('`')
            if text.lower().startswith('json'):
                text = text[4:].strip()
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1 and end > start:
                try:
                    parsed = json.loads(text[start : end + 1])
                    return parsed if isinstance(parsed, dict) else {}
                except Exception:
                    return {}
            return {}
