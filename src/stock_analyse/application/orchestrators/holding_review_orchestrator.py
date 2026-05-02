from __future__ import annotations

import json
import logging
import time
from typing import Any

from stock_analyse.application.graphs.trading_decision.holding_review_graph import run_holding_review_graph


logger = logging.getLogger(__name__)


class HoldingReviewOrchestrator:
    """持仓复盘 AI 编排器。

    用于持仓股票列表的复盘场景，负责承接页面/服务层上下文、调用 holding review graph，并输出前端兼容结果结构。
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

        log('🚀 开始准备持仓复盘上下文', 'header')
        progress(15, '正在整理成交、决策、报表与市场数据...')
        response = run_holding_review_graph(
            context=context,
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
            system_prompt=system_prompt,
        )
        response_payload = response.model_dump(mode='json')
        response_preview = self._build_response_preview(response_payload)
        logger.info('持仓复盘 AI 原始应答摘要: %s', response_preview)
        log(f'AI 应答摘要: {response_preview}', 'info')
        progress(85, '正在生成持仓复盘草案...')
        result = self._build_final_result(
            context,
            response_payload,
            duration_ms=int((time.time() - started_at) * 1000),
        )
        result_summary = self._build_result_summary(result)
        logger.info('持仓复盘归一化结果摘要: %s', result_summary)
        log(f'复盘结果摘要: {result_summary}', 'info')
        progress(100, '持仓复盘草案生成完成')
        return result

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

    def _build_response_preview(self, response: dict[str, Any]) -> str:
        preview = {
            'performance_summary': str(response.get('performance_summary') or '').strip(),
            'execution_summary': str(response.get('execution_summary') or '').strip(),
            'risk_summary': str(response.get('risk_summary') or '').strip(),
            'discipline_summary': str(response.get('discipline_summary') or '').strip(),
            'next_action_summary': str(response.get('next_action_summary') or '').strip(),
            'conclusion_tag': str(response.get('conclusion_tag') or '').strip(),
            'tabs_count': len(response.get('tabs') or []) if isinstance(response.get('tabs'), list) else 0,
        }
        return json.dumps(preview, ensure_ascii=False)

    def _build_result_summary(self, result: dict[str, Any]) -> str:
        data = result.get('data') or {}
        summary = {
            'stock_code': str(data.get('stock_code') or '').strip(),
            'trade_date': str(data.get('trade_date') or '').strip(),
            'review_type': str(data.get('review_type') or '').strip(),
            'conclusion_tag': str(data.get('conclusion_tag') or '').strip(),
            'next_action_summary': str(data.get('next_action_summary') or '').strip(),
            'tabs_count': len(data.get('tabs') or []) if isinstance(data.get('tabs'), list) else 0,
        }
        return json.dumps(summary, ensure_ascii=False)

    def _normalize_tabs(self, value: Any) -> list[dict[str, Any]]:
        expected = [
            ('execution_review', '执行与卖出复盘'),
            ('result_review', '结果复盘'),
            ('discipline_review', '方法与纪律'),
            ('next_action', '后续动作'),
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
