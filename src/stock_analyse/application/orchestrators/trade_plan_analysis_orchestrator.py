from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from stock_analyse.application.graphs.trading_decision import run_trade_plan_analysis_graph


TRADE_PLAN_TEMPLATE_PATH = Path('/mnt/github/stock/stockAnalyse/doc/持仓计划.md')


def _legacy_analyzer_factory(**kwargs):
    return kwargs


class TradePlanAnalysisOrchestrator:
    """持仓计划分析 AI 编排器。

    负责把模板、缓存与回退数据整理为一份可执行的持仓计划草案及决策摘要。
    """

    def __init__(self, *, analyzer_factory: Any | None = None) -> None:
        """初始化持仓计划分析兼容参数。"""
        self.analyzer_factory = analyzer_factory or _legacy_analyzer_factory

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
        """执行持仓计划生成，并返回 markdown 与决策摘要。"""
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

        self.analyzer_factory(
            ai_platform=llm_provider,
            model=llm_model,
            api_token=api_code,
            system_prompt=system_prompt,
        )

        log('🚀 开始准备持仓计划上下文', 'header')
        progress(15, '正在整理缓存与回退数据...')
        response = run_trade_plan_analysis_graph(
            context=context,
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
            system_prompt=system_prompt,
        )
        progress(85, '正在生成持仓计划草案...')
        final_result = self._build_final_result(
            context,
            response.model_dump(mode='json'),
            duration_ms=int((time.time() - started_at) * 1000),
        )
        progress(100, '持仓计划草案生成完成')
        return final_result

    def _build_final_result(self, context: dict[str, Any], response: dict[str, Any], *, duration_ms: int) -> dict[str, Any]:
        watch_stock = context.get('watch_stock') or {}
        request = context.get('request') or {}
        decision = response.get('decision') or {}
        position_suggestion = decision.get('position_suggestion') or {}
        plan_metadata = response.get('plan_metadata') or {}
        trade_plan_markdown = str(response.get('trade_plan_markdown') or '').strip()
        if not trade_plan_markdown:
            trade_plan_markdown = self._fallback_markdown(context, decision)

        return {
            'success': True,
            'data': {
                'watch_stock_id': watch_stock.get('id', ''),
                'stock_code': watch_stock.get('stock_code', ''),
                'stock_name': watch_stock.get('stock_name', ''),
                'market': watch_stock.get('market', ''),
                'trade_date': request.get('trade_date', ''),
                'plan_type': request.get('plan_type', ''),
                'risk_preference': request.get('risk_preference', ''),
                'trade_plan_markdown': trade_plan_markdown,
                'decision': {
                    'action': str(decision.get('action') or 'watch').strip().lower() or 'watch',
                    'summary': str(decision.get('summary') or '').strip(),
                    'logic': str(decision.get('logic') or '').strip(),
                    'risk_level': str(decision.get('risk_level') or 'medium').strip() or 'medium',
                    'risks': self._normalize_list(decision.get('risks')),
                    'time_horizon': str(decision.get('time_horizon') or '').strip(),
                    'position_suggestion': {
                        'target_position': str(position_suggestion.get('target_position') or '').strip(),
                        'position_limit': str(position_suggestion.get('position_limit') or position_suggestion.get('target_position') or '').strip(),
                        'add_condition': str(position_suggestion.get('add_condition') or '').strip(),
                        'reduce_condition': str(position_suggestion.get('reduce_condition') or '').strip(),
                        'stop_loss_reference': str(position_suggestion.get('stop_loss_reference') or '').strip(),
                    },
                },
                'meta': {
                    'template_name': str(plan_metadata.get('template_name') or '持仓计划模板（买前执行版）').strip(),
                    'data_source': str(plan_metadata.get('data_source') or context.get('data_source') or 'fallback_only').strip(),
                    'cache_hits': self._normalize_list(plan_metadata.get('cache_hits') or (context.get('cache_context') or {}).get('cache_hits') or []),
                    'duration_ms': duration_ms,
                },
                'cache_context': context.get('cache_context') or {},
                'fallback_context': context.get('fallback_context') or {},
            },
        }

    def _fallback_markdown(self, context: dict[str, Any], decision: dict[str, Any]) -> str:
        watch_stock = context.get('watch_stock') or {}
        request = context.get('request') or {}
        summary = str(decision.get('summary') or '待确认').strip()
        logic = str(decision.get('logic') or '待确认').strip()
        return (
            '## 一、计划摘要\n\n'
            f'- 标的名称：{watch_stock.get("stock_name", "待确认")}\n'
            f'- 代码：{watch_stock.get("stock_code", "待确认")}\n'
            f'- 市场：{watch_stock.get("market", "待确认")}\n'
            f'- 计划日期：{request.get("trade_date", "待确认")}\n'
            f'- 交易方向：{summary or "待确认"}\n\n'
            '---\n\n'
            '## 二、买前约束条件\n\n'
            '- 待确认：需要结合账户仓位纪律进一步补齐。\n\n'
            '---\n\n'
            '## 三、建仓计划\n\n'
            f'- 核心逻辑：{logic or "待确认"}\n'
        )

    def _normalize_list(self, value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if value in (None, ''):
            return []
        text = str(value).strip()
        return [text] if text else []

    def _load_trade_plan_template_markdown(self) -> str:
        try:
            return TRADE_PLAN_TEMPLATE_PATH.read_text(encoding='utf-8').strip()
        except Exception:
            return ''
