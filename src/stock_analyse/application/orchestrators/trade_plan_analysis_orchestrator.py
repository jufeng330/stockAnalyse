from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer


TRADE_PLAN_TEMPLATE_PATH = Path('/mnt/github/stock/stockAnalyse/doc/持仓计划.md')


class TradePlanAnalysisOrchestrator:
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

        log('🚀 开始准备持仓计划上下文', 'header')
        progress(15, '正在整理缓存与回退数据...')
        response = self._run_trading_expert(analyzer, context)
        progress(85, '正在生成持仓计划草案...')
        final_result = self._build_final_result(context, response, duration_ms=int((time.time() - started_at) * 1000))
        progress(100, '持仓计划草案生成完成')
        return final_result

    def _run_trading_expert(self, analyzer: Any, context: dict[str, Any]) -> dict[str, Any]:
        payload = {
            'template_markdown': context.get('template_markdown') or self._load_trade_plan_template_markdown(),
            'watch_stock': context.get('watch_stock') or {},
            'request': context.get('request') or {},
            'cache_context': context.get('cache_context') or {},
            'fallback_context': context.get('fallback_context') or {},
            'data_source': context.get('data_source') or 'fallback_only',
        }
        instruction = (
            '你是一名股票交易专家，擅长把研究结论转化为可执行的仓位、价格、下单和失败预案。'
            '请输出严格 JSON，不要输出 markdown 代码块，不要输出额外解释。'
        )
        message = (
            '请根据给定的模板、缓存文件内容和补充数据，生成一份专业的持仓计划草案。\n\n'
            '硬性要求：\n'
            '1. 必须严格按模板章节顺序输出 `trade_plan_markdown`，不要删改章节标题。\n'
            '2. 输出必须可执行，优先给出明确动作、仓位和条件，不要空话。\n'
            '3. 若缓存中已有进场决策或股票分析结论，应优先复用这些结论。\n'
            '4. 若信息不足，可以结合 fallback 数据补足；仍无法确定的字段写“待确认”，不要编造。\n'
            '5. 返回 JSON 结构必须包含：\n'
            '{\n'
            '  "trade_plan_markdown": "完整 markdown 正文",\n'
            '  "decision": {\n'
            '    "action": "buy|hold|watch|sell",\n'
            '    "summary": "一句话总结",\n'
            '    "logic": "核心逻辑",\n'
            '    "risk_level": "low|medium|high",\n'
            '    "risks": ["风险1", "风险2"],\n'
            '    "time_horizon": "执行周期",\n'
            '    "position_suggestion": {\n'
            '      "target_position": "最大目标仓位",\n'
            '      "position_limit": "单票仓位上限",\n'
            '      "add_condition": "加仓条件",\n'
            '      "reduce_condition": "减仓条件",\n'
            '      "stop_loss_reference": "止损或退出参考"\n'
            '    }\n'
            '  },\n'
            '  "plan_metadata": {\n'
            '    "template_name": "持仓计划模板（买前执行版）",\n'
            '    "data_source": "cache_first|partial_cache_fallback|fallback_only",\n'
            '    "cache_hits": ["命中文件名"]\n'
            '  }\n'
            '}\n\n'
            f'输入上下文：\n{json.dumps(payload, ensure_ascii=False, indent=2, default=str)}'
        )
        raw_response = analyzer.openai_api_call(
            symbol=(context.get('watch_stock') or {}).get('stock_code', ''),
            message=message,
            instruction=instruction,
        )
        return self._parse_json_response(raw_response)

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
