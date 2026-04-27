from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

from stock_analyse.application.dto.entry_decision_state import EntryDecisionState
from stock_analyse.application.services.ai_stock_data_facade import AIStockDataFacade
from stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer


ROLE_SEQUENCE = [
    'macro_analysis',
    'asset_classification',
    'value_stage_analysis',
    'price_zone_analysis',
    'buy_plan_analysis',
    'risk_control_analysis',
]

ROLE_CONFIG = {
    'macro_analysis': {
        'title': '宏观AI分析师',
        'progress': 15,
        'prompt': '你是宏观AI分析师。请基于提供的数据，判断当前市场环境、风格偏好、资金风险偏好和该标的所处宏观适配度。必须严格输出 JSON 对象，字段至少包含: macro_view, macro_conclusion, macro_reasoning, market_style, liquidity_signal, risks, opportunities。',
    },
    'asset_classification': {
        'title': '资产分类AI分析师',
        'progress': 28,
        'prompt': '你是资产分类AI分析师。请判断该标的属于什么资产类型、这类资产主要靠什么上涨、当前适合什么打法。必须严格输出 JSON 对象，字段至少包含: asset_classification, classification_reasoning, upside_logic, risk_logic, recommended_playbook, forbidden_playbook。',
    },
    'value_stage_analysis': {
        'title': '价值阶段AI分析师',
        'progress': 45,
        'prompt': '你是价值阶段AI分析师。请结合系统自动提取的历史财报摘要、预期变化与公司质量，判断当前价值阶段。必须严格输出 JSON 对象，字段至少包含: current_stage, stage_reasoning, revenue_growth_view, profit_growth_view, cashflow_view, margin_trend_view, expectation_view, stage_risks。',
    },
    'price_zone_analysis': {
        'title': '价格分区AI分析师',
        'progress': 62,
        'prompt': '你是价格分区AI分析师。请结合系统自动提取的估值、价格、技术位置和安全边际判断当前价格区间。必须严格输出 JSON 对象，字段至少包含: price_zone, zone_reasoning, action_signal, action_reasoning, valuation_comment, technical_comment, cheap_reason, danger_reason。',
    },
    'buy_plan_analysis': {
        'title': '买卖计划AI分析师',
        'progress': 78,
        'prompt': '你是买卖计划AI分析师。请结合系统自动提取的周期偏好、估值与价格位置，并参考用户给定仓位约束，给出分笔建仓与后续应对计划。必须严格输出 JSON 对象，字段至少包含: suggested_action, action_reasoning, suggested_entry_leg, max_target_position, current_position, buy_plan, rise_plan, fall_plan, sell_rules, execution_notes。',
        'required_inputs': ['position_input.current_position', 'position_input.max_target_position'],
        'pause_prompt': '买卖计划缺少仓位信息，请补充后继续。',
    },
    'risk_control_analysis': {
        'title': '风险控制AI分析师',
        'progress': 92,
        'prompt': '你是风险控制AI分析师。请输出最终风险约束与决策卡。必须严格输出 JSON 对象，字段至少包含: risk_level, risk_reasoning, key_risks, invalidation_signals, position_constraints, decision_card, conclusion_summary。decision_card 必须包含 current_stage, current_price_zone, suggested_action, suggested_entry_leg, max_target_position, execution_summary。',
    },
}


class EntryDecisionOrchestrator:
    def __init__(
        self,
        *,
        data_facade: AIStockDataFacade | None = None,
        analyzer_factory: Any | None = None,
    ) -> None:
        self.data_facade = data_facade or AIStockDataFacade()
        self.analyzer_factory = analyzer_factory or (lambda **kwargs: StockAiAnalyzer(**kwargs))

    def run(
        self,
        *,
        state: EntryDecisionState,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
        callbacks: dict[str, Any] | None = None,
    ) -> EntryDecisionState:
        callbacks = callbacks or {}
        started_at = time.time()
        send_log = callbacks.get('send_log')
        send_progress = callbacks.get('send_progress')
        send_role_result = callbacks.get('send_role_result')
        send_pause = callbacks.get('send_pause')

        def log(message: str, log_type: str = 'info') -> None:
            if send_log:
                send_log(message, log_type)

        def progress(role_name: str, message: str) -> None:
            if send_progress:
                send_progress('singleProgress', ROLE_CONFIG[role_name]['progress'], message)

        if not state.auto_context:
            log(f"🚀 开始准备进场决策上下文: {state.watch_stock.get('stock_code', '')}", 'header')
            progress('macro_analysis', '正在构建进场决策数据上下文...')
            state.auto_context = self._build_auto_context(state)
            state.add_timeline('context', '上下文构建完成')

        analyzer = self.analyzer_factory(
            ai_platform=llm_provider,
            model=llm_model,
            api_token=api_code,
            system_prompt=system_prompt,
        )

        start_index = self._determine_start_index(state)
        for role_name in ROLE_SEQUENCE[start_index:]:
            config = ROLE_CONFIG[role_name]
            progress(role_name, f"正在执行{config['title']}...")
            log(f"🚀 开始执行 {config['title']}", 'header')

            missing_fields = self._missing_required_fields(state.manual_inputs, config.get('required_inputs', []))
            if missing_fields:
                state.status = 'paused'
                state.current_role = role_name
                state.missing_fields = missing_fields
                state.pause_prompt = config.get('pause_prompt', '请补充缺失字段后继续。')
                state.add_timeline(role_name, state.pause_prompt, status='paused')
                if send_pause:
                    send_pause(
                        {
                            'session_id': state.session_id,
                            'watch_stock_id': state.watch_stock_id,
                            'current_role': role_name,
                            'role_title': config['title'],
                            'missing_fields': missing_fields,
                            'prompt': state.pause_prompt,
                        }
                    )
                return state

            try:
                role_output = self._run_role(analyzer, role_name, state)
            except Exception as exc:
                state.status = 'failed'
                state.current_role = role_name
                state.add_error(role_name, str(exc))
                raise

            state.role_outputs[role_name] = role_output
            state.mark_role_completed(role_name)
            state.add_timeline(role_name, f"{config['title']}执行完成")
            if send_role_result:
                send_role_result(
                    {
                        'session_id': state.session_id,
                        'watch_stock_id': state.watch_stock_id,
                        'role_name': role_name,
                        'role_title': config['title'],
                        'output': role_output,
                    }
                )

        state.status = 'completed'
        state.current_role = ROLE_SEQUENCE[-1]
        state.missing_fields = []
        state.pause_prompt = ''
        state.final_result = self._build_final_result(state, duration_ms=int((time.time() - started_at) * 1000))
        state.meta['finished_at'] = datetime.now().isoformat()
        return state

    def _build_auto_context(self, state: EntryDecisionState) -> dict[str, Any]:
        watch_stock = state.watch_stock
        market = self._normalize_market(watch_stock.get('market'))
        trade_date = (state.request.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        snapshot = self.data_facade.build_snapshot(
            stock_code=watch_stock.get('stock_code', ''),
            market=market,
            trade_date=trade_date,
            include_technical=True,
            include_sentiment=True,
        )
        derived_inputs = self._build_derived_inputs(watch_stock, snapshot)
        return {
            'trade_date': trade_date,
            'analysis_depth': state.request.get('analysis_depth') or 'standard',
            'watch_stock_context': {
                'watch_stock_id': watch_stock.get('id', ''),
                'stock_code': watch_stock.get('stock_code', ''),
                'stock_name': watch_stock.get('stock_name', ''),
                'market': market,
                'industry': watch_stock.get('industry', ''),
                'asset_type': watch_stock.get('asset_type', ''),
                'current_price': watch_stock.get('current_price'),
                'pe': watch_stock.get('pe'),
                'note': watch_stock.get('note', ''),
            },
            'derived_inputs': derived_inputs,
            'snapshot': snapshot,
        }

    def _run_role(self, analyzer: Any, role_name: str, state: EntryDecisionState) -> dict[str, Any]:
        config = ROLE_CONFIG[role_name]
        prompt = self._build_prompt(role_name, state)
        raw_response = analyzer.openai_api_call(
            symbol=state.watch_stock.get('stock_code', ''),
            message=prompt,
            instruction=config['prompt'],
        )
        return self._parse_json_response(raw_response, role_name)

    def _build_prompt(self, role_name: str, state: EntryDecisionState) -> str:
        payload = {
            'session_id': state.session_id,
            'watch_stock': state.watch_stock,
            'request': state.request,
            'auto_context': state.auto_context,
            'manual_inputs': state.manual_inputs,
            'completed_role_outputs': state.role_outputs,
            'derived_inputs': state.auto_context.get('derived_inputs') or {},
            'target_role': role_name,
        }
        return (
            f"角色: {ROLE_CONFIG[role_name]['title']}\n"
            f"任务说明:\n{ROLE_CONFIG[role_name]['prompt']}\n\n"
            '请仅输出 JSON 对象，不要输出 markdown 代码块，不要输出额外解释。\n\n'
            f"上下文数据:\n{json.dumps(payload, ensure_ascii=False, indent=2, default=str)}"
        )

    def _build_final_result(self, state: EntryDecisionState, *, duration_ms: int) -> dict[str, Any]:
        snapshot = state.auto_context.get('snapshot') or {}
        watch_stock_context = state.auto_context.get('watch_stock_context') or {}
        macro_analysis = state.role_outputs.get('macro_analysis') or {}
        asset_classification = state.role_outputs.get('asset_classification') or {}
        value_stage_analysis = state.role_outputs.get('value_stage_analysis') or {}
        price_zone_analysis = state.role_outputs.get('price_zone_analysis') or {}
        buy_plan_analysis = state.role_outputs.get('buy_plan_analysis') or {}
        risk_control_analysis = state.role_outputs.get('risk_control_analysis') or {}
        decision_card = dict(risk_control_analysis.get('decision_card') or {})
        derived_inputs = state.auto_context.get('derived_inputs') or {}
        decision_card.setdefault('current_stage', value_stage_analysis.get('current_stage') or '')
        decision_card.setdefault('current_price_zone', price_zone_analysis.get('price_zone') or '')
        decision_card.setdefault('suggested_action', buy_plan_analysis.get('suggested_action') or '')
        decision_card.setdefault('suggested_entry_leg', buy_plan_analysis.get('suggested_entry_leg') or '')
        decision_card.setdefault('max_target_position', buy_plan_analysis.get('max_target_position') or state.manual_inputs.get('position_input', {}).get('max_target_position', ''))
        decision_card.setdefault('execution_summary', risk_control_analysis.get('conclusion_summary') or buy_plan_analysis.get('action_reasoning') or '')

        return {
            'success': True,
            'data': {
                'session_id': state.session_id,
                'watch_stock_id': state.watch_stock_id,
                'stock_code': watch_stock_context.get('stock_code') or state.watch_stock.get('stock_code', ''),
                'stock_name': watch_stock_context.get('stock_name') or state.watch_stock.get('stock_name', ''),
                'market': watch_stock_context.get('market') or state.watch_stock.get('market', ''),
                'trade_date': state.auto_context.get('trade_date') or state.request.get('trade_date') or '',
                'basic_info': {
                    'stock_code': watch_stock_context.get('stock_code') or '',
                    'stock_name': watch_stock_context.get('stock_name') or '',
                    'market': watch_stock_context.get('market') or '',
                    'industry': watch_stock_context.get('industry') or '',
                    'asset_type': watch_stock_context.get('asset_type') or '',
                    'current_price': watch_stock_context.get('current_price'),
                    'pe': watch_stock_context.get('pe'),
                    'investment_horizon': derived_inputs.get('investment_horizon') or '',
                },
                'macro_analysis': macro_analysis,
                'asset_classification': asset_classification,
                'value_stage_analysis': value_stage_analysis,
                'price_zone_analysis': price_zone_analysis,
                'buy_plan_analysis': buy_plan_analysis,
                'risk_control_analysis': risk_control_analysis,
                'decision_card': decision_card,
                'snapshot': snapshot,
                'manual_inputs': state.manual_inputs,
                'meta': {
                    'status': state.status,
                    'current_role': state.current_role,
                    'completed_roles': state.meta.get('completed_roles', []),
                    'timeline': state.meta.get('timeline', []),
                    'duration_ms': duration_ms,
                    'started_at': state.meta.get('started_at'),
                    'finished_at': state.meta.get('finished_at') or datetime.now().isoformat(),
                    'errors': state.meta.get('errors', []),
                },
            },
        }

    def _parse_json_response(self, raw_response: Any, role_name: str) -> dict[str, Any]:
        text = str(raw_response or '').strip()
        if text.startswith('```'):
            text = text.strip('`')
            if text.startswith('json'):
                text = text[4:].strip()
        try:
            parsed = json.loads(text)
        except Exception as exc:
            raise ValueError(f'{role_name} 返回非 JSON 内容: {exc}; 原始返回: {text[:800]}') from exc
        if not isinstance(parsed, dict):
            raise ValueError(f'{role_name} 返回结果必须是 JSON 对象')
        return parsed

    def _build_derived_inputs(self, watch_stock: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
        market_context = snapshot.get('market_context') or {}
        spot = market_context.get('spot') or {}
        reports = snapshot.get('reports') or {}
        financial_indicators = snapshot.get('financial_indicators') or {}
        technical = snapshot.get('technical') or {}
        technical_summary = technical.get('summary') or {}
        sentiment = snapshot.get('sentiment') or {}

        revenue_growth = self._pick_first_non_empty(
            self._read_nested(financial_indicators, ['主营业务收入增长率(%)']),
            self._read_latest_report_value(reports.get('income_statement'), ['营业总收入同比', '营业总收入同比增长率']),
        )
        profit_growth = self._pick_first_non_empty(
            self._read_nested(financial_indicators, ['净利润增长率(%)']),
            self._read_latest_report_value(reports.get('income_statement'), ['净利润同比', '净利润同比增长率']),
        )
        cashflow_status = self._pick_first_non_empty(
            self._read_latest_report_value(reports.get('cash_flow'), ['经营性现金流-现金流量净额', '经营性现金流-净现金流占比']),
            self._read_nested(financial_indicators, ['经营现金净流量与净利润的比率(%)']),
        )
        margin_trend = self._pick_first_non_empty(
            self._read_nested(financial_indicators, ['销售毛利率(%)', '营业利润率(%)']),
            self._read_latest_report_value(reports.get('income_statement'), ['营业利润']),
        )
        valuation_pb = self._pick_first_non_empty(
            self._read_nested(spot, ['市净率', 'PB', 'pb']),
            self._read_nested(financial_indicators, ['每股净资产_调整后(元)']),
        )
        valuation_judgement = self._build_valuation_judgement(watch_stock, spot, technical_summary)
        expectation_summary = self._build_expectation_summary(sentiment, technical_summary)
        investment_horizon = self._build_investment_horizon(technical_summary, sentiment)

        return {
            'investment_horizon': investment_horizon,
            'expectation_summary': expectation_summary,
            'financial_summary': {
                'revenue_growth': self._stringify_value(revenue_growth),
                'profit_growth': self._stringify_value(profit_growth),
                'cashflow_status': self._stringify_value(cashflow_status),
                'margin_trend': self._stringify_value(margin_trend),
            },
            'valuation_input': {
                'pe': self._stringify_value(self._pick_first_non_empty(watch_stock.get('pe'), self._read_nested(spot, ['市盈率-动态', '动态市盈率', '市盈率']))),
                'pb': self._stringify_value(valuation_pb),
                'valuation_judgement': valuation_judgement,
            },
        }

    def _build_expectation_summary(self, sentiment: dict[str, Any], technical_summary: dict[str, Any]) -> str:
        return self._stringify_value(
            self._pick_first_non_empty(
                self._read_nested(sentiment, ['summary']),
                self._read_nested(sentiment, ['conclusion']),
                self._read_nested(technical_summary, ['summary']),
                self._read_nested(technical_summary, ['conclusion']),
            )
        )

    def _build_investment_horizon(self, technical_summary: dict[str, Any], sentiment: dict[str, Any]) -> str:
        swing_signal = self._pick_first_non_empty(
            self._read_nested(technical_summary, ['short_term_trend']),
            self._read_nested(technical_summary, ['trend']),
            self._read_nested(sentiment, ['trend']),
        )
        return '中期+长期' if not swing_signal else '波段+中期'

    def _build_valuation_judgement(self, watch_stock: dict[str, Any], spot: dict[str, Any], technical_summary: dict[str, Any]) -> str:
        pe_value = self._pick_first_non_empty(watch_stock.get('pe'), self._read_nested(spot, ['市盈率-动态', '动态市盈率', '市盈率']))
        if isinstance(pe_value, (int, float)):
            if pe_value <= 15:
                return '估值偏低'
            if pe_value <= 30:
                return '估值大致合理'
            return '估值偏高，需结合景气度消化'
        if isinstance(pe_value, str) and pe_value.strip():
            return f'估值参考: {pe_value.strip()}'
        technical_bias = self._pick_first_non_empty(
            self._read_nested(technical_summary, ['valuation_comment']),
            self._read_nested(technical_summary, ['summary']),
        )
        return self._stringify_value(technical_bias)

    def _read_latest_report_value(self, rows: Any, keys: list[str]) -> Any:
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0] if isinstance(rows[0], dict) else None
        if not row:
            return None
        return self._read_nested(row, keys)

    def _read_nested(self, payload: Any, keys: list[str]) -> Any:
        if not isinstance(payload, dict):
            return None
        for key in keys:
            if key in payload and payload.get(key) not in (None, ''):
                return payload.get(key)
        return None

    def _pick_first_non_empty(self, *values: Any) -> Any:
        for value in values:
            if value not in (None, ''):
                return value
        return None

    def _stringify_value(self, value: Any) -> str:
        if value in (None, ''):
            return ''
        return str(value).strip()

    def _missing_required_fields(self, manual_inputs: dict[str, Any], required_fields: list[str]) -> list[str]:
        missing: list[str] = []
        for field_path in required_fields:
            value = self._get_nested_value(manual_inputs, field_path)
            if value in (None, ''):
                missing.append(field_path)
        return missing

    def _missing_required_fields(self, manual_inputs: dict[str, Any], required_fields: list[str]) -> list[str]:
        missing: list[str] = []
        for field_path in required_fields:
            value = self._get_nested_value(manual_inputs, field_path)
            if value in (None, ''):
                missing.append(field_path)
        return missing

    def _get_nested_value(self, payload: dict[str, Any], field_path: str) -> Any:
        current: Any = payload
        for part in field_path.split('.'):
            if not isinstance(current, dict) or part not in current:
                return None
            current = current[part]
        return current

    def _determine_start_index(self, state: EntryDecisionState) -> int:
        completed_roles = state.meta.get('completed_roles', [])
        if state.status == 'paused' and state.current_role in ROLE_SEQUENCE:
            return ROLE_SEQUENCE.index(state.current_role)
        if not completed_roles:
            return 0
        for index, role_name in enumerate(ROLE_SEQUENCE):
            if role_name not in completed_roles:
                return index
        return len(ROLE_SEQUENCE)

    def _normalize_market(self, market: Any) -> str:
        normalized = str(market or '').strip().lower()
        if normalized in {'a股', 'cn', 'sh', 'sz'}:
            return 'SH'
        if normalized in {'港股', 'hk', 'h'}:
            return 'H'
        if normalized in {'美股', 'us', 'usa'}:
            return 'usa'
        return str(market or 'SH').strip() or 'SH'
