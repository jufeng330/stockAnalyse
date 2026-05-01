from __future__ import annotations

import time
from typing import Any

from stock_analyse.application.graphs.trading_decision.holding_review_graph import run_holding_review_graph


class HoldingReviewOrchestrator:
    """持仓复盘 AI 编排器。"""

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
        progress(85, '正在生成持仓复盘草案...')
        result = self._build_final_result(
            context,
            response.model_dump(mode='json'),
            duration_ms=int((time.time() - started_at) * 1000),
        )
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
