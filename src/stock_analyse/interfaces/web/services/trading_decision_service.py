from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from stock_analyse.application.dto.entry_decision_state import EntryDecisionState
from stock_analyse.infrastructure.persistence.trading_decision.entry_decision_record_repository import (
    EntryDecisionRecordRepository,
)
from stock_analyse.infrastructure.persistence.trading_decision.entry_decision_session_repository import (
    EntryDecisionSessionRepository,
)
from stock_analyse.infrastructure.persistence.trading_decision.trade_plan_analysis_record_repository import (
    TradePlanAnalysisRecordRepository,
)
from stock_analyse.infrastructure.persistence.trading_decision.watch_stock_repository import WatchStockRepository
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo


class TradingDecisionService:
    def __init__(self, db_path: str | Path | None = None) -> None:
        resolved_db_path = db_path or self._default_db_path()
        self.repository = WatchStockRepository(resolved_db_path)
        self.trade_plan_repository = TradePlanAnalysisRecordRepository(resolved_db_path)
        self.entry_decision_session_repository = EntryDecisionSessionRepository(resolved_db_path)
        self.entry_decision_record_repository = EntryDecisionRecordRepository(resolved_db_path)

    def build_watch_stocks_page_data(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized_filters = self.normalize_filters(filters or {})
        result = self.repository.list(normalized_filters)
        return {
            'summary': result.summary,
            'items': result.items,
            'pagination': result.pagination,
            'filters': normalized_filters,
            'filter_options': self._build_filter_options(result.items),
            'history_placeholder': '持仓计划分析已进入真实页面，统一历史中心会在后续阶段补齐。',
        }

    def list_watch_stocks(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized_filters = self.normalize_filters(filters or {})
        result = self.repository.list(normalized_filters)
        return {
            'items': result.items,
            'summary': result.summary,
            'pagination': result.pagination,
            'filters': normalized_filters,
        }

    def create_watch_stock(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_payload(payload, creating=True)
        return self.repository.create(normalized)

    def get_watch_stock(self, watch_stock_id: str) -> dict[str, Any] | None:
        return self.repository.get_by_id(watch_stock_id)

    def update_watch_stock(self, watch_stock_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        normalized = self._normalize_payload(payload, creating=False)
        return self.repository.update(watch_stock_id, normalized)

    def archive_watch_stock(self, watch_stock_id: str) -> dict[str, Any] | None:
        return self.repository.archive(watch_stock_id)

    def search_stock_candidates(self, query: str, market: str, limit: int = 20) -> list[dict[str, Any]]:
        keyword = (query or '').strip()
        if not keyword:
            return []

        normalized_market = self._normalize_lookup_market(market)
        spot_df = stockBorderInfo(market=normalized_market).get_stock_spot()
        if spot_df is None or spot_df.empty:
            return []

        code_column = self._first_existing_column(spot_df, ['股票代码', '代码'])
        name_column = self._first_existing_column(spot_df, ['名称', '股票简称'])
        price_column = self._first_existing_column(spot_df, ['最新价', '最新价格', '当前价', '收盘价'])
        pe_column = self._resolve_pe_column(spot_df, normalized_market)
        if not code_column or not name_column:
            return []

        selected_columns = [code_column, name_column]
        if price_column:
            selected_columns.append(price_column)
        if pe_column and pe_column not in selected_columns:
            selected_columns.append(pe_column)

        candidates = spot_df[selected_columns].copy()
        candidates[code_column] = candidates[code_column].astype(str).str.strip()
        candidates[name_column] = candidates[name_column].astype(str).str.strip()
        candidates = candidates[(candidates[code_column] != '') & (candidates[name_column] != '')].drop_duplicates()

        lowered_keyword = keyword.lower()
        exact_code = candidates[candidates[code_column].str.lower() == lowered_keyword]
        prefix_code = candidates[candidates[code_column].str.lower().str.startswith(lowered_keyword)]
        contains_name = candidates[candidates[name_column].str.lower().str.contains(lowered_keyword, na=False)]
        merged = exact_code.to_dict('records') + prefix_code.to_dict('records') + contains_name.to_dict('records')

        results: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        display_market = self._display_market(normalized_market)
        for row in merged:
            code = str(row.get(code_column, '')).strip()
            name = str(row.get(name_column, '')).strip()
            key = (code, name)
            if not code or not name or key in seen:
                continue
            seen.add(key)
            results.append(
                {
                    'code': code,
                    'name': name,
                    'market': display_market,
                    'display_label': f'{code} - {name} ({display_market})',
                    'source': 'spot',
                    'current_price': self._to_float(row.get(price_column)) if price_column else None,
                    'pe': self._to_float(row.get(pe_column)) if pe_column else None,
                }
            )
            if len(results) >= limit:
                break
        return results

    def build_entry_decision_page_data(self, watch_stock_id: str) -> dict[str, Any]:
        watch_stock = self.get_watch_stock(watch_stock_id)
        if not watch_stock:
            raise ValueError('关注股票不存在')

        watch_stock = self._hydrate_watch_stock_market_metrics(watch_stock)
        active_session = self.get_latest_active_entry_decision_session(watch_stock_id)
        history_items = self.list_entry_decision_records(watch_stock_id, limit=10)
        selected_record = history_items[0] if history_items else None

        manual_inputs = (active_session or {}).get('manual_inputs_json', {})
        position_input = manual_inputs.get('position_input', {})

        return {
            'watch_stock': watch_stock,
            'display_market': watch_stock.get('market') or 'A股',
            'active_session': active_session,
            'selected_record': selected_record,
            'history_items': history_items,
            'form_defaults': {
                'trade_date': datetime.now().strftime('%Y-%m-%d'),
                'analysis_depth': 'standard',
                'position_input': {
                    'current_position': position_input.get('current_position') or '0%',
                    'max_target_position': position_input.get('max_target_position') or '',
                },
                'current_stage': watch_stock.get('current_stage') or '',
                'current_price_zone': watch_stock.get('current_price_zone') or '',
                'suggested_action': watch_stock.get('suggested_action') or '',
                'last_conclusion_summary': watch_stock.get('last_conclusion_summary') or '',
                'last_analysis_at': watch_stock.get('last_analysis_at') or '',
            },
        }

    def create_entry_decision_session(self, watch_stock_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        watch_stock = self.get_watch_stock(watch_stock_id)
        if not watch_stock:
            raise ValueError('关注股票不存在')

        request_payload = self._normalize_entry_decision_request(payload)
        created = self.entry_decision_session_repository.create(
            {
                'watch_stock_id': watch_stock_id,
                'stock_code': watch_stock.get('stock_code', ''),
                'trade_date': request_payload['trade_date'],
                'status': 'running',
                'current_role': 'macro_analysis',
                'request_json': request_payload,
                'manual_inputs_json': request_payload['manual_inputs'],
                'auto_context_json': {},
                'role_outputs_json': {},
                'missing_fields_json': [],
                'pause_prompt': '',
                'final_result_json': {},
            }
        )
        return self._format_entry_decision_session(created)

    def get_entry_decision_session(self, session_id: str) -> dict[str, Any] | None:
        row = self.entry_decision_session_repository.get_by_id(session_id)
        return self._format_entry_decision_session(row) if row else None

    def get_latest_active_entry_decision_session(self, watch_stock_id: str) -> dict[str, Any] | None:
        row = self.entry_decision_session_repository.find_latest_active_by_watch_stock(watch_stock_id)
        return self._format_entry_decision_session(row) if row else None

    def build_entry_decision_state(self, session_id: str) -> EntryDecisionState:
        session = self.get_entry_decision_session(session_id)
        if not session:
            raise ValueError('进场决策会话不存在')

        watch_stock = self.get_watch_stock(session['watch_stock_id'])
        if not watch_stock:
            raise ValueError('关注股票不存在')

        return EntryDecisionState(
            session_id=session['id'],
            watch_stock_id=session['watch_stock_id'],
            request=session.get('request_json') or {},
            watch_stock=watch_stock,
            auto_context=session.get('auto_context_json') or {},
            manual_inputs=session.get('manual_inputs_json') or {},
            role_outputs=session.get('role_outputs_json') or {},
            current_role=session.get('current_role') or 'macro_analysis',
            status=session.get('status') or 'running',
            missing_fields=session.get('missing_fields_json') or [],
            pause_prompt=session.get('pause_prompt') or '',
            final_result=session.get('final_result_json') or {},
            meta=self._extract_entry_decision_meta(session),
        )

    def update_entry_decision_session_from_state(self, state: EntryDecisionState) -> dict[str, Any] | None:
        updated = self.entry_decision_session_repository.update(
            state.session_id,
            {
                'stock_code': state.watch_stock.get('stock_code', ''),
                'trade_date': state.request.get('trade_date') or '',
                'status': state.status,
                'current_role': state.current_role,
                'request_json': state.request,
                'manual_inputs_json': state.manual_inputs,
                'auto_context_json': state.auto_context,
                'role_outputs_json': state.role_outputs,
                'missing_fields_json': state.missing_fields,
                'pause_prompt': state.pause_prompt,
                'final_result_json': state.final_result,
            },
        )
        return self._format_entry_decision_session(updated) if updated else None

    def resume_entry_decision_session(self, session_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        session = self.get_entry_decision_session(session_id)
        if not session:
            raise ValueError('进场决策会话不存在')
        if session.get('status') not in {'paused', 'running'}:
            raise ValueError('当前会话不可继续执行')

        prior_request = session.get('request_json') or {}
        next_request = self._normalize_entry_decision_request({**prior_request, **payload}, fallback_trade_date=prior_request.get('trade_date') or '')
        merged_manual_inputs = self._deep_merge_dicts(session.get('manual_inputs_json') or {}, next_request['manual_inputs'])
        updated = self.entry_decision_session_repository.update(
            session_id,
            {
                'status': 'running',
                'request_json': {**prior_request, **next_request, 'manual_inputs': merged_manual_inputs},
                'manual_inputs_json': merged_manual_inputs,
                'missing_fields_json': [],
                'pause_prompt': '',
            },
        )
        return self._format_entry_decision_session(updated) if updated else None

    def save_entry_decision_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        watch_stock_id = (payload.get('watch_stock_id') or '').strip()
        if not watch_stock_id:
            raise ValueError('缺少 watch_stock_id')

        watch_stock = self.get_watch_stock(watch_stock_id)
        if not watch_stock:
            raise ValueError('关注股票不存在')

        raw_result = payload.get('raw_result') or {}
        if raw_result and not isinstance(raw_result, dict):
            raise ValueError('raw_result 必须是对象')

        record_payload = self.build_entry_decision_record_payload(raw_result, watch_stock, payload)
        created = self.entry_decision_record_repository.create(record_payload)
        formatted = self._format_entry_decision_record(created)
        self.update_watch_stock_from_entry_decision_record(formatted)
        return formatted

    def list_entry_decision_records(self, watch_stock_id: str, limit: int = 10) -> list[dict[str, Any]]:
        rows = self.entry_decision_record_repository.list_by_watch_stock(watch_stock_id, limit=limit)
        return [self._format_entry_decision_record(row) for row in rows]

    def get_entry_decision_record(self, record_id: str) -> dict[str, Any] | None:
        row = self.entry_decision_record_repository.get_by_id(record_id)
        return self._format_entry_decision_record(row) if row else None

    def update_watch_stock_from_entry_decision_record(self, record: dict[str, Any]) -> dict[str, Any] | None:
        return self.update_watch_stock(
            record['watch_stock_id'],
            {
                'current_stage': record.get('current_stage') or '',
                'current_price_zone': record.get('current_price_zone') or '',
                'suggested_action': record.get('suggested_action') or '',
                'last_conclusion_summary': record.get('conclusion_summary') or '',
                'last_analysis_at': record.get('trade_date') or '',
            },
        )

    def build_trade_plan_analysis_page_data(self, watch_stock_id: str, record_id: str | None = None) -> dict[str, Any]:
        watch_stock = self.get_watch_stock(watch_stock_id)
        if not watch_stock:
            raise ValueError('关注股票不存在')

        history_items = self.list_trade_plan_analysis_records(watch_stock_id)
        selected_record = None
        if record_id:
            selected_record = self.get_trade_plan_analysis_record(record_id)
            if not selected_record or selected_record.get('watch_stock_id') != watch_stock_id:
                raise ValueError('计划分析记录不存在')
        elif history_items:
            selected_record = history_items[0]

        return {
            'watch_stock': watch_stock,
            'display_market': watch_stock.get('market') or 'A股',
            'selected_record': selected_record,
            'history_items': history_items,
            'form_defaults': {
                'trade_date': datetime.now().strftime('%Y-%m-%d'),
                'plan_type': (selected_record or {}).get('plan_type') or '三笔计划',
                'risk_preference': (selected_record or {}).get('risk_preference') or '中高风险',
                'analysis_depth': 'standard',
                'suggested_action': (selected_record or {}).get('suggested_action') or watch_stock.get('suggested_action') or '',
                'conclusion_summary': (selected_record or {}).get('conclusion_summary') or watch_stock.get('last_conclusion_summary') or '',
                'max_target_position': (selected_record or {}).get('max_target_position') or '',
                'position_limit': (selected_record or {}).get('position_limit') or '',
            },
        }

    def save_trade_plan_analysis_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        watch_stock_id = (payload.get('watch_stock_id') or '').strip()
        if not watch_stock_id:
            raise ValueError('缺少 watch_stock_id')

        watch_stock = self.get_watch_stock(watch_stock_id)
        if not watch_stock:
            raise ValueError('关注股票不存在')

        raw_result = payload.get('raw_result') or {}
        if raw_result and not isinstance(raw_result, dict):
            raise ValueError('raw_result 必须是对象')

        trade_plan_payload = self.build_trade_plan_analysis_payload(raw_result, watch_stock, payload)
        created = self.trade_plan_repository.create(trade_plan_payload)
        formatted = self._format_trade_plan_record(created)
        self.update_watch_stock(
            watch_stock_id,
            {
                'suggested_action': formatted.get('suggested_action') or watch_stock.get('suggested_action') or '',
                'last_conclusion_summary': formatted.get('conclusion_summary') or watch_stock.get('last_conclusion_summary') or '',
                'last_analysis_at': formatted.get('trade_date') or watch_stock.get('last_analysis_at') or '',
            },
        )
        return formatted

    def list_trade_plan_analysis_records(self, watch_stock_id: str, limit: int = 10) -> list[dict[str, Any]]:
        rows = self.trade_plan_repository.list_by_watch_stock(watch_stock_id, limit=limit)
        return [self._format_trade_plan_record(row) for row in rows]

    def get_trade_plan_analysis_record(self, record_id: str) -> dict[str, Any] | None:
        row = self.trade_plan_repository.get_by_id(record_id)
        return self._format_trade_plan_record(row) if row else None

    def build_trade_plan_analysis_payload(self, raw_result: dict[str, Any], watch_stock: dict[str, Any], request_payload: dict[str, Any]) -> dict[str, Any]:
        data = raw_result.get('data') or {}
        decision = data.get('decision') or {}
        scores = data.get('scores') or {}
        position_suggestion = decision.get('position_suggestion') or {}
        if not isinstance(position_suggestion, dict):
            position_suggestion = {}

        suggested_action = (request_payload.get('suggested_action') or '').strip() or self._map_ai_decision_action_to_label(decision.get('action'))
        conclusion_summary = (request_payload.get('conclusion_summary') or '').strip() or (decision.get('summary') or '').strip() or (decision.get('logic') or '').strip()
        trade_date = (request_payload.get('trade_date') or '').strip() or (data.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        plan_type = (request_payload.get('plan_type') or '').strip() or '三笔计划'
        risk_preference = (request_payload.get('risk_preference') or '').strip() or '中高风险'
        max_target_position = (request_payload.get('max_target_position') or '').strip() or str(position_suggestion.get('target_position') or '').strip()
        position_limit = (request_payload.get('position_limit') or '').strip() or str(position_suggestion.get('target_position') or '').strip()
        add_position_rules = str(position_suggestion.get('add_condition') or '').strip()
        reduce_position_rules = str(position_suggestion.get('reduce_condition') or '').strip()
        sell_rules = str(position_suggestion.get('stop_loss_reference') or '').strip()
        risk_items = decision.get('risks') or []
        if not isinstance(risk_items, list):
            risk_items = [str(risk_items)]
        risk_notes = '\n'.join(str(item).strip() for item in risk_items if str(item).strip())
        if not risk_notes:
            risk_notes = str(decision.get('risk_level') or '').strip()

        plan_steps = [
            {'title': '建仓条件', 'content': add_position_rules},
            {'title': '减仓条件', 'content': reduce_position_rules},
            {'title': '止损参考', 'content': sell_rules},
        ]
        plan_steps = [item for item in plan_steps if item['content']]

        return {
            'watch_stock_id': watch_stock['id'],
            'stock_code': watch_stock.get('stock_code', ''),
            'stock_name': watch_stock.get('stock_name', ''),
            'market': watch_stock.get('market', ''),
            'trade_date': trade_date,
            'plan_type': plan_type,
            'risk_preference': risk_preference,
            'risk_level': str(decision.get('risk_level') or 'medium').strip() or 'medium',
            'suggested_action': suggested_action,
            'conclusion_summary': conclusion_summary,
            'max_target_position': max_target_position,
            'position_limit': position_limit,
            'entry_plan_json': {
                'plan_steps': plan_steps,
                'scores': scores,
                'time_horizon': decision.get('time_horizon') or '',
            },
            'add_position_rules': add_position_rules,
            'reduce_position_rules': reduce_position_rules,
            'sell_rules': sell_rules,
            'risk_notes': risk_notes,
            'raw_result_json': raw_result,
        }

    def build_entry_decision_record_payload(self, raw_result: dict[str, Any], watch_stock: dict[str, Any], request_payload: dict[str, Any]) -> dict[str, Any]:
        data = raw_result.get('data') or {}
        value_stage_analysis = data.get('value_stage_analysis') or {}
        price_zone_analysis = data.get('price_zone_analysis') or {}
        buy_plan_analysis = data.get('buy_plan_analysis') or {}
        risk_control_analysis = data.get('risk_control_analysis') or {}
        decision_card = data.get('decision_card') or {}

        current_stage = (request_payload.get('current_stage') or '').strip() or str(decision_card.get('current_stage') or value_stage_analysis.get('current_stage') or '').strip()
        current_price_zone = (request_payload.get('current_price_zone') or '').strip() or str(decision_card.get('current_price_zone') or price_zone_analysis.get('price_zone') or '').strip()
        suggested_action = (request_payload.get('suggested_action') or '').strip() or str(decision_card.get('suggested_action') or buy_plan_analysis.get('suggested_action') or '').strip()
        suggested_entry_leg = str(request_payload.get('suggested_entry_leg') or decision_card.get('suggested_entry_leg') or buy_plan_analysis.get('suggested_entry_leg') or '').strip()
        conclusion_summary = (request_payload.get('conclusion_summary') or '').strip() or str(risk_control_analysis.get('conclusion_summary') or decision_card.get('execution_summary') or '').strip()
        trade_date = (request_payload.get('trade_date') or '').strip() or str(data.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d')

        return {
            'watch_stock_id': watch_stock['id'],
            'session_id': (request_payload.get('session_id') or '').strip(),
            'stock_code': watch_stock.get('stock_code', ''),
            'stock_name': watch_stock.get('stock_name', ''),
            'market': watch_stock.get('market', ''),
            'trade_date': trade_date,
            'current_stage': current_stage,
            'current_price_zone': current_price_zone,
            'suggested_action': suggested_action,
            'suggested_entry_leg': suggested_entry_leg,
            'conclusion_summary': conclusion_summary,
            'decision_card_json': decision_card,
            'full_result_json': raw_result,
        }

    def normalize_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        return {
            'keyword': (filters.get('keyword') or '').strip(),
            'market': (filters.get('market') or '').strip(),
            'asset_type': (filters.get('asset_type') or '').strip(),
            'stage': (filters.get('stage') or '').strip(),
            'price_zone': (filters.get('price_zone') or '').strip(),
            'status': (filters.get('status') or '').strip(),
            'page': self._to_int(filters.get('page'), default=1, minimum=1),
            'page_size': self._to_int(filters.get('page_size'), default=20, minimum=1, maximum=100),
        }

    def _format_trade_plan_record(self, row: dict[str, Any]) -> dict[str, Any]:
        entry_plan_json = row.get('entry_plan_json') or {}
        raw_result_json = row.get('raw_result_json') or {}
        if isinstance(entry_plan_json, str):
            entry_plan_json = self._safe_json_loads(entry_plan_json)
        if isinstance(raw_result_json, str):
            raw_result_json = self._safe_json_loads(raw_result_json)
        return {
            **row,
            'entry_plan_json': entry_plan_json if isinstance(entry_plan_json, dict) else {},
            'raw_result_json': raw_result_json if isinstance(raw_result_json, dict) else {},
        }

    def _format_entry_decision_session(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not row:
            return None
        return {
            **row,
            'request_json': row.get('request_json') or {},
            'manual_inputs_json': row.get('manual_inputs_json') or {},
            'auto_context_json': row.get('auto_context_json') or {},
            'role_outputs_json': row.get('role_outputs_json') or {},
            'missing_fields_json': row.get('missing_fields_json') or [],
            'final_result_json': row.get('final_result_json') or {},
        }

    def _format_entry_decision_record(self, row: dict[str, Any]) -> dict[str, Any]:
        decision_card_json = row.get('decision_card_json') or {}
        full_result_json = row.get('full_result_json') or {}
        if isinstance(decision_card_json, str):
            decision_card_json = self._safe_json_loads(decision_card_json)
        if isinstance(full_result_json, str):
            full_result_json = self._safe_json_loads(full_result_json)
        return {
            **row,
            'decision_card_json': decision_card_json if isinstance(decision_card_json, dict) else {},
            'full_result_json': full_result_json if isinstance(full_result_json, dict) else {},
        }

    def _extract_entry_decision_meta(self, session: dict[str, Any]) -> dict[str, Any]:
        meta = ((session.get('final_result_json') or {}).get('data') or {}).get('meta')
        if isinstance(meta, dict):
            meta = dict(meta)
        else:
            meta = {}
        meta.setdefault('completed_roles', list((session.get('role_outputs_json') or {}).keys()))
        meta.setdefault('errors', [])
        meta.setdefault('timeline', [])
        return meta

    def _normalize_entry_decision_request(self, payload: dict[str, Any], fallback_trade_date: str = '') -> dict[str, Any]:
        trade_date = (payload.get('trade_date') or fallback_trade_date or '').strip() or datetime.now().strftime('%Y-%m-%d')
        analysis_depth = (payload.get('analysis_depth') or 'standard').strip() or 'standard'
        client_id = (payload.get('client_id') or '').strip() or None
        manual_inputs = {
            'position_input': self._normalize_nested_object(payload.get('position_input'), ['current_position', 'max_target_position']),
        }
        return {
            'trade_date': trade_date,
            'analysis_depth': analysis_depth,
            'client_id': client_id,
            'manual_inputs': manual_inputs,
        }

    def _normalize_nested_object(self, value: Any, fields: list[str]) -> dict[str, str]:
        source = value if isinstance(value, dict) else {}
        return {field: self._stringify_manual_value(source.get(field)) for field in fields}

    def _stringify_manual_value(self, value: Any) -> str:
        if value is None or isinstance(value, (dict, list, tuple, set)):
            return ''
        return str(value).strip()

    def _deep_merge_dicts(self, base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base)
        for key, value in extra.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = self._deep_merge_dicts(merged[key], value)
            elif value not in (None, ''):
                merged[key] = value
            elif key not in merged:
                merged[key] = value
        return merged

    def _safe_json_loads(self, value: str) -> dict[str, Any]:
        try:
            loaded = __import__('json').loads(value)
        except Exception:
            return {}
        return loaded if isinstance(loaded, dict) else {}

    def _normalize_payload(self, payload: dict[str, Any], creating: bool) -> dict[str, Any]:
        normalized = {
            'stock_code': (payload.get('stock_code') or '').strip(),
            'stock_name': (payload.get('stock_name') or '').strip(),
            'market': (payload.get('market') or '').strip(),
            'industry': (payload.get('industry') or '').strip(),
            'asset_type': (payload.get('asset_type') or '').strip(),
            'source': (payload.get('source') or '').strip(),
            'note': (payload.get('note') or '').strip(),
            'status': (payload.get('status') or '').strip(),
            'current_price': payload.get('current_price'),
            'pe': payload.get('pe'),
            'current_stage': (payload.get('current_stage') or '').strip(),
            'current_price_zone': (payload.get('current_price_zone') or '').strip(),
            'suggested_action': (payload.get('suggested_action') or '').strip(),
            'last_conclusion_summary': (payload.get('last_conclusion_summary') or '').strip(),
            'last_analysis_at': (payload.get('last_analysis_at') or '').strip(),
        }
        if creating:
            self._require_fields(normalized, ['stock_code', 'stock_name', 'market', 'asset_type'])
            return normalized

        populated = {key: value for key, value in normalized.items() if value not in (None, '')}
        if payload.get('note') == '':
            populated['note'] = ''
        if payload.get('industry') == '':
            populated['industry'] = ''
        if payload.get('source') == '':
            populated['source'] = ''
        if payload.get('current_stage') == '':
            populated['current_stage'] = ''
        if payload.get('current_price_zone') == '':
            populated['current_price_zone'] = ''
        if payload.get('suggested_action') == '':
            populated['suggested_action'] = ''
        if payload.get('last_conclusion_summary') == '':
            populated['last_conclusion_summary'] = ''
        if payload.get('last_analysis_at') == '':
            populated['last_analysis_at'] = ''
        if payload.get('current_price') in ('', None):
            populated['current_price'] = None
        if payload.get('pe') in ('', None):
            populated['pe'] = None
        return populated

    def _build_filter_options(self, items: list[dict[str, Any]]) -> dict[str, list[str]]:
        return {
            'markets': sorted({item['market'] for item in items if item.get('market')}),
            'asset_types': sorted({item['asset_type'] for item in items if item.get('asset_type')}),
            'stages': sorted({item['current_stage'] for item in items if item.get('current_stage')}),
            'price_zones': sorted({item['current_price_zone'] for item in items if item.get('current_price_zone')}),
        }

    def _hydrate_watch_stock_market_metrics(self, watch_stock: dict[str, Any]) -> dict[str, Any]:
        needs_price = watch_stock.get('current_price') in (None, '')
        needs_pe = watch_stock.get('pe') in (None, '')
        if not (needs_price or needs_pe):
            return watch_stock

        stock_code = (watch_stock.get('stock_code') or '').strip()
        market = self._normalize_lookup_market(watch_stock.get('market') or '')
        if not stock_code:
            return watch_stock

        try:
            spot_df = stockBorderInfo(market=market).get_stock_spot()
        except Exception:
            return watch_stock
        if spot_df is None or spot_df.empty:
            return watch_stock

        code_column = self._first_existing_column(spot_df, ['股票代码', '代码'])
        if not code_column:
            return watch_stock

        matched = spot_df[spot_df[code_column].astype(str).str.strip().str.upper() == stock_code.upper()]
        if matched.empty:
            return watch_stock

        row = matched.iloc[0]
        price_column = self._first_existing_column(spot_df, ['最新价', '最新价格', '当前价', '收盘价'])
        pe_column = self._resolve_pe_column(spot_df, market)
        enriched = dict(watch_stock)
        if needs_price and price_column:
            enriched['current_price'] = self._to_float(row.get(price_column))
        if needs_pe and pe_column:
            enriched['pe'] = self._to_float(row.get(pe_column))
        return enriched

    def _normalize_lookup_market(self, market: str) -> str:
        normalized = (market or '').strip()
        if normalized in {'SH', 'SZ', 'A股', 'CN'}:
            return 'SH'
        if normalized in {'H', 'HK', '港股'}:
            return 'H'
        if normalized in {'usa', 'US', '美股'}:
            return 'usa'
        return 'SH'

    def _display_market(self, market: str) -> str:
        if market == 'H':
            return '港股'
        if market == 'usa':
            return '美股'
        return 'A股'

    def _map_ai_decision_action_to_label(self, action: str | None) -> str:
        normalized = (action or '').strip().lower()
        mapping = {'buy': '适合买入', 'hold': '继续观察', 'watch': '继续观察', 'sell': '不适合买入'}
        return mapping.get(normalized, action or '')

    def _resolve_pe_column(self, dataframe: Any, market: str) -> str | None:
        if market == 'SH':
            return self._first_existing_column(dataframe, ['市盈率-动态', '动态市盈率'])
        if market == 'H':
            return self._first_existing_column(dataframe, ['市盈率-动态', '动态市盈率', '市盈率'])
        if market == 'usa':
            return self._first_existing_column(dataframe, ['市盈率', '动态市盈率', '市盈率-动态'])
        return self._first_existing_column(dataframe, ['市盈率-动态', '动态市盈率'])

    def _first_existing_column(self, dataframe: Any, column_names: list[str]) -> str | None:
        for name in column_names:
            if name in dataframe.columns:
                return name
        return None

    def _require_fields(self, payload: dict[str, Any], fields: list[str]) -> None:
        missing = [field for field in fields if not payload.get(field)]
        if missing:
            raise ValueError(f"缺少必填字段: {', '.join(missing)}")

    def _default_db_path(self) -> Path:
        configured = os.getenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', '').strip()
        if configured:
            return Path(configured)
        project_root = Path(__file__).resolve().parents[5]
        return project_root / 'data' / 'trading_decision.sqlite3'

    def _to_int(self, value: Any, *, default: int, minimum: int | None = None, maximum: int | None = None) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        if minimum is not None:
            parsed = max(parsed, minimum)
        if maximum is not None:
            parsed = min(parsed, maximum)
        return parsed

    def _to_float(self, value: Any) -> float | None:
        if value in (None, ''):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
