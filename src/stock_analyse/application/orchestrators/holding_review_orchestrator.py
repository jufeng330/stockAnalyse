from __future__ import annotations

import json
import time
from typing import Any

from stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer


class HoldingReviewOutputError(ValueError):
    pass


class HoldingReviewOrchestrator:
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

        log('🚀 开始准备持仓复盘上下文', 'header')
        progress(15, '正在整理成交、决策、报表与市场数据...')
        response = self._run_holding_review_analyst(analyzer, context)
        progress(85, '正在生成持仓复盘草案...')
        result = self._build_final_result(context, response, duration_ms=int((time.time() - started_at) * 1000))
        progress(100, '持仓复盘草案生成完成')
        return result

    def _run_holding_review_analyst(self, analyzer: Any, context: dict[str, Any]) -> dict[str, Any]:
        payload = {
            'holding_stock': context.get('holding_stock') or {},
            'watch_stock': context.get('watch_stock') or {},
            'request': context.get('request') or {},
            'trade_history_context': context.get('trade_history_context') or {},
            'entry_context': context.get('entry_context') or {},
            'reanalysis_context': context.get('reanalysis_context') or {},
            'position_decision_context': context.get('position_decision_context') or {},
            'financial_context': context.get('financial_context') or {},
            'market_context': context.get('market_context') or {},
            'review_focus_context': context.get('review_focus_context') or {},
            'data_source': context.get('data_source') or 'holding_snapshot',
        }
        instruction = (
            '你是交易专家。请基于持仓成交、原始决策、复盘相关记录、财报与市场数据做持仓复盘。'
            '必须返回结构化 JSON 对象，不要输出 markdown 代码块，不要输出额外解释。'
        )
        message = (
            '请根据输入上下文生成一份结构化持仓复盘草案。\n\n'
            '硬性要求：\n'
            '1. 这是持仓复盘，不是普通荐股报告，必须同时看结果与过程。\n'
            '2. 必须输出以下字段：performance_summary、execution_summary、risk_summary、discipline_summary、next_action_summary、conclusion_tag、tabs。\n'
            '3. conclusion_tag 只能是：logic_ok、need_recheck、execution_issue、risk_rising、prepare_reduce、prepare_sell。\n'
            '4. 必须输出 4 个固定 tabs，顺序必须是：执行与卖出复盘、结果复盘、方法与纪律、后续动作。\n'
            '5. 每个 tab 必须包含：id、title、summary、evidence。\n'
            '6. 每个 tab 的 summary 是顶部结论，evidence 是底部理由列表。\n'
            '7. 后续动作 tab 必须综合前 3 个 tabs，明确下一步动作建议，并体现 conclusion_tag。\n\n'
            f'输入上下文：\n{json.dumps(payload, ensure_ascii=False, indent=2, default=str)}'
        )
        raw_response = analyzer.openai_api_call(
            symbol=(context.get('holding_stock') or {}).get('stock_code', ''),
            message=self._build_json_output_message(message),
            instruction=instruction,
            require_tool_call=False,
        )
        parsed = self._parse_json_response(raw_response)
        self._validate_holding_review_payload(parsed)
        return parsed

    def _build_json_output_message(self, base_message: str) -> str:
        schema = self._build_holding_review_tool_schema()['function']['parameters']
        return (
            f"{base_message}\n\n"
            '输出要求：只返回一个 JSON 对象，不要输出 markdown，不要输出额外解释。\n'
            f"JSON Schema:\n{json.dumps(schema, ensure_ascii=False, indent=2)}"
        )

    def _build_holding_review_tool_schema(self) -> dict[str, Any]:
        return {
            'type': 'function',
            'function': {
                'name': 'submit_holding_review',
                'description': '提交持仓复盘结构化结果。',
                'parameters': {
                    'type': 'object',
                    'additionalProperties': False,
                    'properties': {
                        'performance_summary': {'type': 'string'},
                        'execution_summary': {'type': 'string'},
                        'risk_summary': {'type': 'string'},
                        'discipline_summary': {'type': 'string'},
                        'next_action_summary': {'type': 'string'},
                        'conclusion_tag': {
                            'type': 'string',
                            'enum': ['logic_ok', 'need_recheck', 'execution_issue', 'risk_rising', 'prepare_reduce', 'prepare_sell'],
                        },
                        'tabs': {
                            'type': 'array',
                            'minItems': 4,
                            'maxItems': 4,
                            'items': {
                                'type': 'object',
                                'additionalProperties': False,
                                'properties': {
                                    'id': {
                                        'type': 'string',
                                        'enum': ['execution_review', 'result_review', 'discipline_review', 'next_action'],
                                    },
                                    'title': {
                                        'type': 'string',
                                        'enum': ['执行与卖出复盘', '结果复盘', '方法与纪律', '后续动作'],
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
                    'required': [
                        'performance_summary',
                        'execution_summary',
                        'risk_summary',
                        'discipline_summary',
                        'next_action_summary',
                        'conclusion_tag',
                        'tabs',
                    ],
                },
            },
        }

    def _validate_holding_review_payload(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            raise HoldingReviewOutputError('AI 未返回对象结构')
        required_fields = [
            'performance_summary',
            'execution_summary',
            'risk_summary',
            'discipline_summary',
            'next_action_summary',
            'conclusion_tag',
            'tabs',
        ]
        missing = [field for field in required_fields if field not in payload]
        if missing:
            raise HoldingReviewOutputError(f'AI 返回缺少字段: {", ".join(missing)}')
        tag = str(payload.get('conclusion_tag') or '').strip()
        if tag not in {'logic_ok', 'need_recheck', 'execution_issue', 'risk_rising', 'prepare_reduce', 'prepare_sell'}:
            raise HoldingReviewOutputError(f'AI 返回了非法 conclusion_tag: {tag or "<empty>"}')
        for field in required_fields[:-2]:
            if not str(payload.get(field) or '').strip():
                raise HoldingReviewOutputError(f'AI 返回的 {field} 为空')
        tabs = payload.get('tabs')
        if not isinstance(tabs, list) or len(tabs) != 4:
            raise HoldingReviewOutputError('AI 返回的 tabs 数量不是固定 4 个')
        expected = [
            ('execution_review', '执行与卖出复盘'),
            ('result_review', '结果复盘'),
            ('discipline_review', '方法与纪律'),
            ('next_action', '后续动作'),
        ]
        for index, (expected_id, expected_title) in enumerate(expected):
            item = tabs[index]
            if not isinstance(item, dict):
                raise HoldingReviewOutputError(f'AI 返回的第 {index + 1} 个 tab 不是对象')
            if str(item.get('id') or '').strip() != expected_id:
                raise HoldingReviewOutputError(f'AI 返回的第 {index + 1} 个 tab id 非法: {item.get("id")}')
            if str(item.get('title') or '').strip() != expected_title:
                raise HoldingReviewOutputError(f'AI 返回的第 {index + 1} 个 tab title 非法: {item.get("title")}')
            if not str(item.get('summary') or '').strip():
                raise HoldingReviewOutputError(f'AI 返回的第 {index + 1} 个 tab summary 为空')
            evidence = item.get('evidence')
            if not isinstance(evidence, list) or not evidence or not all(str(detail).strip() for detail in evidence):
                raise HoldingReviewOutputError(f'AI 返回的第 {index + 1} 个 tab evidence 非法')

    def _build_final_result(self, context: dict[str, Any], response: dict[str, Any], *, duration_ms: int) -> dict[str, Any]:
        holding_stock = context.get('holding_stock') or {}
        request = context.get('request') or {}
        tabs = self._normalize_tabs(response.get('tabs'))
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
                'review_type': request.get('review_type', 'general'),
                'period_key': request.get('period_key', ''),
                'analysis_depth': request.get('analysis_depth', 'standard'),
                'performance_summary': str(response.get('performance_summary') or '').strip(),
                'execution_summary': str(response.get('execution_summary') or '').strip(),
                'risk_summary': str(response.get('risk_summary') or '').strip(),
                'discipline_summary': str(response.get('discipline_summary') or '').strip(),
                'next_action_summary': str(response.get('next_action_summary') or '').strip(),
                'conclusion_tag': str(response.get('conclusion_tag') or '').strip(),
                'tabs': tabs,
                'evidence': evidence,
                'meta': {
                    'role': '交易专家',
                    'data_source': context.get('data_source') or 'holding_snapshot',
                    'duration_ms': duration_ms,
                },
                'context_snapshot': {
                    'trade_history_context': context.get('trade_history_context') or {},
                    'entry_context': context.get('entry_context') or {},
                    'reanalysis_context': context.get('reanalysis_context') or {},
                    'position_decision_context': context.get('position_decision_context') or {},
                    'financial_context': context.get('financial_context') or {},
                    'market_context': context.get('market_context') or {},
                },
            },
        }

    def _normalize_tabs(self, value: Any) -> list[dict[str, Any]]:
        expected = [
            ('execution_review', '执行与卖出复盘'),
            ('result_review', '结果复盘'),
            ('discipline_review', '方法与纪律'),
            ('next_action', '后续动作'),
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
