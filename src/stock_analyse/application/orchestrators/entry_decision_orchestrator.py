from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from stock_analyse.application.dto.entry_decision_state import EntryDecisionState
from stock_analyse.application.graphs.trading_decision import run_entry_decision_role_graph, run_entry_decision_summary_graph
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

ENTRY_DECISION_TEMPLATE_PATH = Path('/mnt/github/stock/进场决策模板_空白实战版.md')

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


class FocusEntryDecisionOrchestrator:
    """Focus 进场决策多角色编排器。

    负责组织宏观、资产分类、价值阶段、价格区间、买卖计划和风险控制等角色串行产出结果。
    """

    def __init__(
        self,
        *,
        data_facade: AIStockDataFacade | None = None,
        analyzer_factory: Any | None = None,
    ) -> None:
        """初始化数据快照门面与统一 AI 调用工厂。"""
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
        """按角色顺序推进进场决策，并支持暂停后继续。"""
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
                role_output = self._run_role(
                    analyzer,
                    role_name,
                    state,
                    llm_provider=llm_provider,
                    llm_model=llm_model,
                    api_code=api_code,
                    system_prompt=system_prompt,
                )
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
        summary_markdown = self._build_entry_decision_summary_markdown(
            analyzer,
            state,
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
            system_prompt=system_prompt,
        )
        state.final_result = self._build_final_result(
            state,
            duration_ms=int((time.time() - started_at) * 1000),
            entry_decision_summary_markdown=summary_markdown,
        )
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
                'source': watch_stock.get('source', ''),
                'notes': watch_stock.get('notes', ''),
            },
            'snapshot': snapshot,
            'derived_inputs': derived_inputs,
        }

    def _build_derived_inputs(self, watch_stock: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
        return {
            'position_input': {
                'current_position': watch_stock.get('current_position', ''),
                'max_target_position': watch_stock.get('target_position', ''),
            },
            'valuation_context': snapshot.get('valuation') or {},
            'technical_context': snapshot.get('technical') or {},
            'sentiment_context': snapshot.get('sentiment') or {},
            'fundamental_context': snapshot.get('fundamental') or {},
            'news_context': snapshot.get('news') or {},
            'market_context': snapshot.get('market') or {},
        }

    def _determine_start_index(self, state: EntryDecisionState) -> int:
        current_role = (state.current_role or '').strip()
        if state.status != 'paused' or not current_role:
            return 0
        try:
            return ROLE_SEQUENCE.index(current_role)
        except ValueError:
            return 0

    def _missing_required_fields(self, manual_inputs: dict[str, Any], fields: list[str]) -> list[str]:
        missing = []
        for field in fields:
            value = self._deep_get(manual_inputs, field)
            if value in (None, ''):
                missing.append(field)
        return missing

    def _run_role(
        self,
        analyzer: Any,
        role_name: str,
        state: EntryDecisionState,
        *,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> dict[str, Any]:
        return run_entry_decision_role_graph(
            role_name=role_name,
            state=state,
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
            system_prompt=system_prompt,
        )


    def _build_entry_decision_summary_markdown(
        self,
        analyzer: Any,
        state: EntryDecisionState,
        *,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> str:
        try:
            return str(
                run_entry_decision_summary_graph(
                    template_markdown=self._load_entry_decision_template_markdown(),
                    state=state,
                    llm_provider=llm_provider,
                    llm_model=llm_model,
                    api_code=api_code,
                    system_prompt=system_prompt,
                )
                or ''
            ).strip()
        except Exception:
            return ''

    def _build_final_result(
        self,
        state: EntryDecisionState,
        *,
        duration_ms: int,
        entry_decision_summary_markdown: str,
    ) -> dict[str, Any]:
        role_outputs = state.role_outputs
        watch_stock = state.watch_stock
        request = state.request
        risk_output = role_outputs.get('risk_control_analysis') or {}
        decision_card = risk_output.get('decision_card') or {}
        return {
            'success': True,
            'data': {
                'session_id': state.session_id,
                'watch_stock_id': state.watch_stock_id,
                'stock_code': watch_stock.get('stock_code', ''),
                'stock_name': watch_stock.get('stock_name', ''),
                'market': watch_stock.get('market', ''),
                'trade_date': request.get('trade_date', ''),
                'analysis_depth': request.get('analysis_depth', 'standard'),
                'status': state.status,
                'decision_card': decision_card,
                'macro_analysis': role_outputs.get('macro_analysis') or {},
                'asset_classification': role_outputs.get('asset_classification') or {},
                'value_stage_analysis': role_outputs.get('value_stage_analysis') or {},
                'price_zone_analysis': role_outputs.get('price_zone_analysis') or {},
                'buy_plan_analysis': role_outputs.get('buy_plan_analysis') or {},
                'risk_control_analysis': risk_output,
                'entry_decision_summary_markdown': entry_decision_summary_markdown,
                'meta': {
                    'duration_ms': duration_ms,
                    'completed_roles': list(state.completed_roles),
                    'timeline': list(state.timeline),
                },
            },
        }

    def _load_entry_decision_template_markdown(self) -> str:
        try:
            return ENTRY_DECISION_TEMPLATE_PATH.read_text(encoding='utf-8').strip()
        except Exception:
            return ''

    def _normalize_market(self, value: Any) -> str:
        market = str(value or '').strip()
        lowered = market.lower()
        if lowered in {'cn', 'sh', 'sz', 'a股'}:
            return 'SH'
        if lowered in {'h', 'hk', '港股'}:
            return 'H'
        if lowered in {'usa', 'us', '美股'}:
            return 'usa'
        if lowered == 'zq':
            return 'zq'
        return market or 'SH'

    def _deep_get(self, data: dict[str, Any], path: str) -> Any:
        current: Any = data
        for segment in path.split('.'):
            if not isinstance(current, dict):
                return None
            current = current.get(segment)
        return current

    def dumps_state(self, state: EntryDecisionState) -> str:
        return json.dumps(state.model_dump(mode='json'), ensure_ascii=False)


class EntryDecisionOrchestrator(FocusEntryDecisionOrchestrator):
    """兼容保留的旧进场决策编排器名称。"""

    pass
