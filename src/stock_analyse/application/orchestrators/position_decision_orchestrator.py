from __future__ import annotations

import json
import logging
import time
from typing import Any

from stock_analyse.application.graphs.trading_decision.position_decision_graph import run_position_decision_graph


logger = logging.getLogger(__name__)


class HoldingPositionDecisionOrchestrator:
    """Holding 买卖决策 AI 编排器。

    用于持仓股票列表的买卖决策场景，负责承接持仓上下文、调用 position decision graph，并映射为页面与落库兼容结构。
    """

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

        log('🚀 开始准备买卖决策上下文', 'header')
        progress(15, '正在整理财报、成交和持仓计划数据...')
        response = run_position_decision_graph(
            context=context,
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
            system_prompt=system_prompt,
        )
        response_payload = response.model_dump(mode='json')
        response_preview = self._build_response_preview(response_payload)
        logger.info('买卖决策 AI 原始应答摘要: %s', response_preview)
        log(f'AI 应答摘要: {response_preview}', 'info')
        progress(85, '正在生成买卖决策草案...')
        result = self._build_final_result(
            context,
            response_payload,
            duration_ms=int((time.time() - started_at) * 1000),
        )
        result_summary = self._build_result_summary(result)
        logger.info('买卖决策归一化结果摘要: %s', result_summary)
        log(f'买卖决策结果摘要: {result_summary}', 'info')
        progress(100, '买卖决策草案生成完成')
        return result

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

    def _build_response_preview(self, response: dict[str, Any]) -> str:
        preview = {
            'recommended_action': str(response.get('recommended_action') or '').strip(),
            'decision_status': str(response.get('decision_status') or '').strip(),
            'confidence': str(response.get('confidence') or '').strip(),
            'conclusion_summary': str(response.get('conclusion_summary') or '').strip(),
            'tabs_count': len(response.get('tabs') or []) if isinstance(response.get('tabs'), list) else 0,
        }
        return json.dumps(preview, ensure_ascii=False)

    def _build_result_summary(self, result: dict[str, Any]) -> str:
        data = result.get('data') or {}
        decision = data.get('decision') or {}
        summary = {
            'stock_code': str(data.get('stock_code') or '').strip(),
            'trade_date': str(data.get('trade_date') or '').strip(),
            'action': str(decision.get('action') or '').strip(),
            'status': str(decision.get('status') or '').strip(),
            'confidence': str(decision.get('confidence') or '').strip(),
            'conclusion_summary': str(decision.get('summary') or '').strip(),
            'tabs_count': len(data.get('tabs') or []) if isinstance(data.get('tabs'), list) else 0,
        }
        return json.dumps(summary, ensure_ascii=False)

    def _normalize_tabs(self, value: Any) -> list[dict[str, Any]]:
        expected = [
            ('trigger', '触发条件'),
            ('reason', '核心理由'),
            ('execution', '执行注意事项'),
            ('risk', '风险分析'),
            ('conclusion', '结论'),
        ]
        source = [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []
        normalized: list[dict[str, Any]] = []
        for index, (default_id, default_title) in enumerate(expected):
            item = next(
                (
                    candidate
                    for candidate in source
                    if str(candidate.get('id') or '').strip().lower() == default_id
                    or str(candidate.get('title') or '').strip() == default_title
                ),
                source[index] if index < len(source) else {},
            )
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


class PositionDecisionOrchestrator(HoldingPositionDecisionOrchestrator):
    """兼容保留的旧买卖决策编排器名称。"""

    pass
