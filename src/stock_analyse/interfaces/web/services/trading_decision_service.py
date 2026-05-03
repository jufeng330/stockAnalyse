from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from stock_analyse.application.dto.entry_decision_state import EntryDecisionState
from stock_analyse.application.services.ai_stock_data_facade import AIStockDataFacade
from stock_analyse.infrastructure.persistence.trading_decision.entry_decision_record_repository import (
    EntryDecisionRecordRepository,
)
from stock_analyse.infrastructure.persistence.trading_decision.entry_decision_session_repository import (
    EntryDecisionSessionRepository,
)
from stock_analyse.infrastructure.persistence.trading_decision.holding_review_record_repository import (
    HoldingReviewRecordRepository,
)
from stock_analyse.infrastructure.persistence.trading_decision.holding_stock_repository import HoldingStockRepository
from stock_analyse.infrastructure.persistence.trading_decision.position_decision_record_repository import (
    PositionDecisionRecordRepository,
)
from stock_analyse.infrastructure.persistence.trading_decision.stock_analysis_record_repository import (
    StockAnalysisRecordRepository,
)
from stock_analyse.infrastructure.persistence.trading_decision.trade_plan_analysis_record_repository import (
    TradePlanAnalysisRecordRepository,
)
from stock_analyse.infrastructure.persistence.trading_decision.watch_stock_repository import WatchStockRepository
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo


class TradingDecisionService:
    """交易决策服务门面。

    负责在 Web 层与仓储、数据快照、AI 编排之间组装上下文、归一化结果并保存业务记录。
    """

    def __init__(self, db_path: str | Path | None = None, *, data_facade: AIStockDataFacade | None = None) -> None:
        """初始化交易决策主链路依赖的仓储、缓存目录和数据门面。"""
        resolved_db_path = db_path or self._default_db_path()
        self.repository = WatchStockRepository(resolved_db_path)
        self.holding_repository = HoldingStockRepository(resolved_db_path)
        self.trade_plan_repository = TradePlanAnalysisRecordRepository(resolved_db_path)
        self.trade_plan_analysis_record_repository = self.trade_plan_repository
        self.stock_analysis_record_repository = StockAnalysisRecordRepository(resolved_db_path)
        self.position_decision_record_repository = PositionDecisionRecordRepository(resolved_db_path)
        self.holding_review_record_repository = HoldingReviewRecordRepository(resolved_db_path)
        self.entry_decision_session_repository = EntryDecisionSessionRepository(resolved_db_path)
        self.entry_decision_record_repository = EntryDecisionRecordRepository(resolved_db_path)
        self.data_facade = data_facade or AIStockDataFacade()
        self.trade_plan_cache_dir = Path(__file__).resolve().parents[5] / 'cache' / 'tranding_plan'
        self.trade_plan_template_path = Path(__file__).resolve().parents[5] / 'doc' / '持仓计划.md'
        self.trade_plan_cache_biz_markers = {
            'entry_decision': 'Strategy',
            'stock_analysis': 'analyse',
            'holding_reanalysis': 'Reanalysis',
            'trade_plan': 'plan',
            'position_decision': 'Decision',
            'holding_review': 'Review',
        }
        self.trade_plan_cache_display_labels = {
            'entry_decision': '进场策略',
            'stock_analysis': '股票分析',
            'holding_reanalysis': '二次分析',
            'trade_plan': '买入计划',
            'position_decision': '买卖决策',
            'holding_review': '持仓复盘',
        }
        self.trade_plan_template_name = '持仓计划模板（买前执行版）'
        self.trade_plan_cache_keywords = {
            'entry_decision': ['entry-decision', 'entry_decision', '进场决策', 'strategy'],
            'stock_analysis': ['stock-analysis', 'stock_analysis', 'analysis-plan', 'analysis_plan', '股票分析', '分析计划', 'analyse'],
            'trade_plan': ['trade-plan', 'trade_plan', '持仓计划', '买入计划', 'plan'],
        }
        self.trade_plan_role_instruction = '股票交易专家'

    def build_focus_stocks_page_data(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        raw_filters = filters or {}
        normalized_filters = self.normalize_filters(raw_filters)
        result = self.repository.list(normalized_filters)
        all_history_items = self._build_focus_records_history_items(result.items)
        history_filters = self.build_watch_history_filters(raw_filters)
        filtered_history_items = self._filter_watch_records_history_items(all_history_items, history_filters['history_type'])
        history_items, history_pagination = self._paginate_items(
            filtered_history_items,
            page=history_filters['history_page'],
            page_size=history_filters['history_page_size'],
        )
        pagination = {
            **result.pagination,
            'total_pages': max((result.pagination.get('total', 0) + result.pagination.get('page_size', 20) - 1) // result.pagination.get('page_size', 20), 1),
        }
        return {
            'summary': result.summary,
            'items': result.items,
            'pagination': pagination,
            'pagination_links': {
                'prev': self.build_watch_stocks_page_href(page=max(pagination.get('page', 1) - 1, 1), filters=normalized_filters, history_filters=history_filters),
                'next': self.build_watch_stocks_page_href(page=pagination.get('page', 1) + 1, filters=normalized_filters, history_filters=history_filters),
            },
            'filters': normalized_filters,
            'filter_options': self._build_filter_options(result.items),
            'history_items': history_items,
            'history_filter_options': self.build_watch_history_filter_options(),
            'history_filters': history_filters,
            'history_filter_summary': self.build_watch_history_filter_summary(history_filters),
            'history_pagination': history_pagination,
            'history_pagination_links': {
                'prev': self.build_watch_stocks_history_href(page=max(history_pagination.get('page', 1) - 1, 1), filters=normalized_filters, history_filters=history_filters),
                'next': self.build_watch_stocks_history_href(page=history_pagination.get('page', 1) + 1, filters=normalized_filters, history_filters=history_filters),
            },
        }

    def build_watch_stocks_page_data(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.build_focus_stocks_page_data(filters)

    def build_focus_stocks_page_vm(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.build_focus_stocks_page_data(filters)

    def build_focus_stocks_page_context(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.build_focus_stocks_page_vm(filters)

    def build_focus_history_center_page_data(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.build_history_center_page_data(filters)

    def build_focus_history_center_page_vm(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.build_focus_history_center_page_data(filters)

    def build_focus_history_center_page_context(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.build_focus_history_center_page_vm(filters)

    def build_history_center_page_data(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._build_focus_history_center_page_data(filters)

    def _build_focus_history_center_page_data(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        raw_filters = filters or {}
        normalized_filters = self.normalize_filters(raw_filters)
        active_tab = (raw_filters.get('tab') or 'all').strip() or 'all'
        if active_tab not in {'all', 'entry-decision', 'stock-analysis', 'trade-plan', 'files'}:
            active_tab = 'all'

        all_filters = {**normalized_filters, 'page': 1, 'page_size': 1000}
        all_result = self.repository.list(all_filters)
        filtered_watch_stocks = all_result.items
        all_history_items = self._build_focus_records_history_items(filtered_watch_stocks)
        all_entry_records = self._build_history_center_entry_records(filtered_watch_stocks)
        all_stock_analysis_records = self._build_history_center_focus_stock_analysis_records(filtered_watch_stocks)
        all_trade_plan_records = self._build_history_center_trade_plan_records(filtered_watch_stocks)

        page = normalized_filters.get('page', 1)
        page_size = normalized_filters.get('page_size', 20)
        history_items, history_pagination = self._paginate_items(all_history_items, page=page, page_size=page_size)
        entry_records, entry_pagination = self._paginate_items(all_entry_records, page=page, page_size=page_size)
        stock_analysis_records, stock_analysis_pagination = self._paginate_items(all_stock_analysis_records, page=page, page_size=page_size)
        trade_plan_records, trade_plan_pagination = self._paginate_items(all_trade_plan_records, page=page, page_size=page_size)

        active_pagination = {
            'all': history_pagination,
            'entry-decision': entry_pagination,
            'stock-analysis': stock_analysis_pagination,
            'trade-plan': trade_plan_pagination,
            'files': {'page': 1, 'page_size': page_size, 'total': 0, 'total_pages': 1},
        }.get(active_tab, history_pagination)
        view_filters = {**normalized_filters, 'tab': active_tab}
        pagination_links = {
            'prev': self.build_history_center_page_href(tab=active_tab, page=max(active_pagination.get('page', 1) - 1, 1), filters=view_filters),
            'next': self.build_history_center_page_href(tab=active_tab, page=active_pagination.get('page', 1) + 1, filters=view_filters),
        }

        return {
            'summary_cards': {
                'watch_count': len(all_history_items),
                'entry_decision_record_count': len(all_entry_records),
                'stock_analysis_record_count': len(all_stock_analysis_records),
                'trade_plan_record_count': len(all_trade_plan_records),
            },
            'filters': view_filters,
            'filter_form': self.build_history_center_filter_form_state(view_filters),
            'filter_summary': self.build_history_center_filter_summary(view_filters),
            'filter_reset_href': self.build_history_center_reset_href(),
            'filter_options': self._build_filter_options(filtered_watch_stocks),
            'tab_links': {
                'all': self.build_history_center_page_href(tab='all', page=1, filters=view_filters),
                'entry-decision': self.build_history_center_page_href(tab='entry-decision', page=1, filters=view_filters),
                'stock-analysis': self.build_history_center_page_href(tab='stock-analysis', page=1, filters=view_filters),
                'trade-plan': self.build_history_center_page_href(tab='trade-plan', page=1, filters=view_filters),
                'files': self.build_history_center_page_href(tab='files', page=1, filters=view_filters),
            },
            'active_tab': active_tab,
            'history_items': history_items,
            'entry_decision_records': entry_records,
            'stock_analysis_records': stock_analysis_records,
            'trade_plan_records': trade_plan_records,
            'active_pagination': active_pagination,
            'pagination_links': pagination_links,
            'legacy_history_url': '/history',
        }

    def build_history_center_page_vm(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.build_focus_history_center_page_vm(filters)

    def build_history_center_page_context(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.build_focus_history_center_page_context(filters)


    def build_holding_stocks_page_data(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        raw_filters = filters or {}
        normalized_filters = self.normalize_holding_filters(raw_filters)
        result = self.holding_repository.list(normalized_filters)
        items = [self._build_holding_stock_payload(item) for item in result.items]
        pagination = {
            **result.pagination,
            'total_pages': max((result.pagination.get('total', 0) + result.pagination.get('page_size', 20) - 1) // result.pagination.get('page_size', 20), 1),
        }
        return {
            'summary': result.summary,
            'items': items,
            'pagination': pagination,
            'pagination_links': {
                'prev': self.build_holding_stocks_page_href(page=max(pagination.get('page', 1) - 1, 1), filters=normalized_filters),
                'next': self.build_holding_stocks_page_href(page=pagination.get('page', 1) + 1, filters=normalized_filters),
            },
            'filters': normalized_filters,
            'filter_options': self._build_holding_filter_options(result.items),
        }

    def build_holding_stocks_page_href(self, *, page: int, filters: dict[str, Any]) -> str:
        query = {
            'page': max(int(page or 1), 1),
            'page_size': self._to_int(filters.get('page_size'), default=20, minimum=1, maximum=100),
        }
        for key in ('keyword', 'market', 'asset_type', 'risk_status', 'suggested_action'):
            value = (filters.get(key) or '').strip() if isinstance(filters.get(key), str) else filters.get(key)
            if value:
                query[key] = value
        return '/holding-stocks?' + urlencode(query)

    def build_holding_stocks_reset_href(self) -> str:
        return '/holding-stocks'

    def normalize_holding_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        return {
            'keyword': (filters.get('keyword') or '').strip(),
            'market': (filters.get('market') or '').strip(),
            'asset_type': (filters.get('asset_type') or '').strip(),
            'risk_status': (filters.get('risk_status') or '').strip(),
            'suggested_action': (filters.get('suggested_action') or '').strip(),
            'status': (filters.get('status') or '').strip(),
            'page': self._to_int(filters.get('page'), default=1, minimum=1),
            'page_size': self._to_int(filters.get('page_size'), default=20, minimum=1, maximum=100),
        }

    def list_holding_stocks(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized_filters = self.normalize_holding_filters(filters or {})
        result = self.holding_repository.list(normalized_filters)
        return {
            'items': [self._build_holding_stock_payload(item) for item in result.items],
            'summary': result.summary,
            'pagination': result.pagination,
            'filters': normalized_filters,
        }

    def get_holding_stock(self, holding_stock_id: str) -> dict[str, Any] | None:
        item = self.holding_repository.get_by_id(holding_stock_id)
        return self._build_holding_stock_payload(item) if item else None

    def get_holding_stock_by_watch_stock(self, watch_stock_id: str) -> dict[str, Any] | None:
        item = self.holding_repository.get_by_linked_watch_stock_id(watch_stock_id)
        return self._build_holding_stock_payload(item) if item else None

    def create_holding_stock_buy(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_holding_buy_payload(payload, creating=True)
        created = self.holding_repository.create_with_buy(normalized)
        if created.get('linked_watch_stock_id'):
            self.update_watch_stock(
                created['linked_watch_stock_id'],
                {
                    'linked_holding_stock_id': created['id'],
                    'suggested_action': normalized.get('suggested_action') or '',
                },
            )
        return self._build_holding_stock_payload(created)

    def append_holding_stock_buy(self, holding_stock_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        normalized = self._normalize_holding_buy_payload(payload, creating=False)
        updated = self.holding_repository.append_buy(holding_stock_id, normalized)
        return self._build_holding_stock_payload(updated) if updated else None

    def convert_watch_stock_to_holding_buy(self, watch_stock_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        watch_stock = self.get_watch_stock(watch_stock_id)
        if not watch_stock:
            raise ValueError('关注股票不存在')
        linked_holding_id = (watch_stock.get('linked_holding_stock_id') or '').strip()
        normalized = self._normalize_holding_buy_payload(
            {
                **payload,
                'linked_watch_stock_id': watch_stock_id,
                'stock_code': payload.get('stock_code') or watch_stock.get('stock_code') or '',
                'stock_name': payload.get('stock_name') or watch_stock.get('stock_name') or '',
                'market': payload.get('market') or watch_stock.get('market') or '',
                'industry': payload.get('industry') or watch_stock.get('industry') or '',
                'asset_type': payload.get('asset_type') or watch_stock.get('asset_type') or '',
                'current_price': payload.get('current_price', watch_stock.get('current_price')),
            },
            creating=not bool(linked_holding_id),
        )
        if linked_holding_id:
            updated = self.holding_repository.append_buy(linked_holding_id, normalized)
            if not updated:
                raise ValueError('关联持仓不存在')
            self.update_watch_stock(watch_stock_id, {'linked_holding_stock_id': linked_holding_id})
            return self._build_holding_stock_payload(updated)
        created = self.holding_repository.create_with_buy(normalized)
        self.update_watch_stock(watch_stock_id, {'linked_holding_stock_id': created['id']})
        return self._build_holding_stock_payload(created)

    def _build_holding_stock_payload(self, item: dict[str, Any] | None) -> dict[str, Any]:
        if not item:
            return {}
        lots = self.holding_repository.list_lots(item['id'], limit=10)
        trades = self.holding_repository.list_trades(item['id'], limit=10)
        payload = dict(item)
        payload['lots'] = lots
        payload['trades'] = trades
        payload['lot_count'] = len(lots)
        payload['display_market'] = item.get('market') or 'A股'
        return payload

    def _normalize_holding_buy_payload(self, payload: dict[str, Any], *, creating: bool) -> dict[str, Any]:
        normalized = {
            'linked_watch_stock_id': (payload.get('linked_watch_stock_id') or '').strip(),
            'stock_code': (payload.get('stock_code') or '').strip(),
            'stock_name': (payload.get('stock_name') or '').strip(),
            'market': (payload.get('market') or '').strip(),
            'industry': (payload.get('industry') or '').strip(),
            'asset_type': (payload.get('asset_type') or '').strip(),
            'status': (payload.get('status') or '').strip() or 'active',
            'risk_status': (payload.get('risk_status') or '').strip(),
            'suggested_action': (payload.get('suggested_action') or '').strip(),
            'note': (payload.get('note') or '').strip(),
            'current_price': payload.get('current_price'),
            'quantity': payload.get('quantity'),
            'price': payload.get('price'),
            'amount': payload.get('amount'),
            'trade_date': (payload.get('trade_date') or '').strip(),
            'last_review_at': (payload.get('last_review_at') or '').strip(),
            'source_watch_stock_id': (payload.get('source_watch_stock_id') or '').strip(),
        }
        required_fields = ['quantity', 'price', 'trade_date']
        if creating:
            required_fields = ['stock_code', 'stock_name', 'market', 'asset_type', *required_fields]
        self._require_fields(normalized, required_fields)
        return normalized

    def _build_holding_filter_options(self, items: list[dict[str, Any]]) -> dict[str, list[str]]:
        return {
            'markets': sorted({item['market'] for item in items if item.get('market')}),
            'asset_types': sorted({item['asset_type'] for item in items if item.get('asset_type')}),
            'risk_statuses': sorted({item['risk_status'] for item in items if item.get('risk_status')}),
            'suggested_actions': sorted({item['suggested_action'] for item in items if item.get('suggested_action')}),
        }

    def build_holding_buy_form_data(self, *, holding_stock_id: str = '', watch_stock_id: str = '') -> dict[str, Any]:
        if holding_stock_id:
            holding_stock = self.get_holding_stock(holding_stock_id)
            if not holding_stock:
                raise ValueError('持仓不存在')
            return {
                'mode': 'append',
                'holding_stock': holding_stock,
                'watch_stock': None,
            }
        if watch_stock_id:
            watch_stock = self.get_watch_stock(watch_stock_id)
            if not watch_stock:
                raise ValueError('关注股票不存在')
            return {
                'mode': 'from_watch',
                'holding_stock': self.get_holding_stock_by_watch_stock(watch_stock_id),
                'watch_stock': watch_stock,
            }
        return {
            'mode': 'create',
            'holding_stock': None,
            'watch_stock': None,
        }

    def build_holding_buy_record_detail_url(self, holding_stock: dict[str, Any]) -> str:
        return f"/holding-records?holding_stock_id={holding_stock.get('id', '')}"

    def build_holding_action_links(self, holding_stock: dict[str, Any]) -> dict[str, str]:
        holding_stock_id = holding_stock.get('id', '')
        return {
            'reanalysis': f'/holding-reanalysis?holding_stock_id={holding_stock_id}',
            'decision': f'/position-decision?holding_stock_id={holding_stock_id}',
            'review': f'/holding-review?holding_stock_id={holding_stock_id}',
            'records': self.build_holding_buy_record_detail_url(holding_stock),
        }

    def build_holding_records_page_data(self, holding_stock_id: str) -> dict[str, Any]:
        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')

        reanalysis_records = self.build_holding_reanalysis_history_items(holding_stock_id, limit=10)
        decision_records = [
            {
                **record,
                'detail_url': f"/position-decision?holding_stock_id={holding_stock_id}&record_id={record.get('id', '')}",
            }
            for record in self.list_position_decision_records(holding_stock_id, limit=10)
        ]
        review_records = [
            {
                **record,
                'detail_url': f"/holding-review?holding_stock_id={holding_stock_id}&record_id={record.get('id', '')}",
            }
            for record in self.list_holding_review_records(holding_stock_id, limit=10)
        ]

        return {
            'title': '持仓历史记录',
            'description': '统一查看持仓域的二次分析、买卖决策与复盘历史记录。',
            'holding_stock': holding_stock,
            'reanalysis_records': reanalysis_records,
            'decision_records': decision_records,
            'review_records': review_records,
            'summary': {
                'reanalysis_count': len(reanalysis_records),
                'decision_count': len(decision_records),
                'review_count': len(review_records),
                'total_count': len(reanalysis_records) + len(decision_records) + len(review_records),
            },
        }

    def build_holding_review_prefill_result(self, record: dict[str, Any] | None) -> dict[str, Any]:
        if not record:
            return {}
        raw_result = record.get('raw_result_json') or {}
        if isinstance(raw_result, dict) and isinstance(raw_result.get('data'), dict):
            return self._clean_data_for_json(raw_result)
        return self._clean_data_for_json(
            self._normalize_holding_review_result(
                {
                    'trade_date': record.get('trade_date', ''),
                    'review_type': record.get('review_type', 'general'),
                    'period_key': record.get('period_key', ''),
                    'analysis_depth': record.get('analysis_depth', 'standard'),
                    'performance_summary': record.get('performance_summary', ''),
                    'execution_summary': record.get('execution_summary', ''),
                    'risk_summary': record.get('risk_summary', ''),
                    'discipline_summary': record.get('discipline_summary', ''),
                    'next_action_summary': record.get('next_action_summary', ''),
                    'conclusion_tag': record.get('conclusion_tag', ''),
                    'tabs': record.get('tabs_json') or [],
                    'evidence': record.get('evidence_json') or [],
                    'context_snapshot': record.get('context_snapshot_json') or {},
                    'meta': {'role': '交易专家'},
                }
            )
        )

    def build_position_decision_prefill_result(self, record: dict[str, Any] | None) -> dict[str, Any]:
        if not record:
            return {}
        raw_result = record.get('raw_result_json') or {}
        if isinstance(raw_result, dict) and isinstance(raw_result.get('data'), dict):
            return self._clean_data_for_json(raw_result)
        return self._clean_data_for_json(self._normalize_position_decision_result(raw_result if isinstance(raw_result, dict) else {}))

    def build_holding_reanalysis_prefill_result(self, record: dict[str, Any] | None) -> dict[str, Any]:
        if not record:
            return {}
        raw_result = record.get('raw_result_json') or {}
        return self._clean_data_for_json(raw_result if isinstance(raw_result, dict) else {})

    def _clean_data_for_json(self, obj: Any):
        if isinstance(obj, dict):
            return {key: self._clean_data_for_json(value) for key, value in obj.items()}
        if isinstance(obj, list):
            return [self._clean_data_for_json(item) for item in obj]
        if isinstance(obj, tuple):
            return [self._clean_data_for_json(item) for item in obj]
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return obj
        if obj is None or isinstance(obj, (str, int, bool)):
            return obj
        try:
            if math.isnan(obj) or math.isinf(obj):
                return None
        except (TypeError, ValueError):
            pass
        return obj

    def build_default_holding_records_url(self) -> str:
        result = self.holding_repository.list(self.normalize_holding_filters({'page': 1, 'page_size': 1}))
        first_item = result.items[0] if result.items else None
        if not first_item:
            return '/holding-stocks#holding-table'
        return f"/holding-records?holding_stock_id={first_item.get('id', '')}"

    def build_holding_reanalysis_page_data(self, holding_stock_id: str, record_id: str | None = None) -> dict[str, Any]:
        return self.build_focus_holding_reanalysis_page_data(holding_stock_id, record_id)

    def build_holding_reanalysis_context(self, holding_stock_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')

        linked_watch_stock_id = (holding_stock.get('linked_watch_stock_id') or '').strip()
        watch_stock = self.get_watch_stock(linked_watch_stock_id) if linked_watch_stock_id else None
        if watch_stock:
            watch_stock = self._hydrate_watch_stock_market_metrics(watch_stock)

        trade_date = (payload.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        market = self._normalize_lookup_market(holding_stock.get('market') or '')
        stock_code = (holding_stock.get('stock_code') or '').strip()
        snapshot = self.data_facade.build_snapshot(stock_code=stock_code, market=market, trade_date=trade_date)
        watch_stock_history = self.list_stock_analysis_records(linked_watch_stock_id, limit=3) if linked_watch_stock_id else []
        entry_decision_history = self.list_entry_decision_records(linked_watch_stock_id, limit=3) if linked_watch_stock_id else []
        trade_plan_history = self.list_trade_plan_analysis_records(linked_watch_stock_id, limit=1) if linked_watch_stock_id else []
        return {
            'holding_stock': holding_stock,
            'watch_stock': watch_stock,
            'trade_date': trade_date,
            'snapshot': snapshot,
            'watch_stock_history': watch_stock_history,
            'entry_decision_history': entry_decision_history,
            'trade_plan_history': trade_plan_history,
        }

    def list_holding_reanalysis_records(self, holding_stock_id: str, limit: int = 10) -> list[dict[str, Any]]:
        rows = self.stock_analysis_record_repository.list_by_holding_stock(holding_stock_id, limit=limit)
        return [self._format_stock_analysis_record(row) for row in rows]

    def build_holding_reanalysis_record_payload(self, raw_result: dict[str, Any], holding_stock: dict[str, Any], watch_stock: dict[str, Any] | None, request_payload: dict[str, Any]) -> dict[str, Any]:
        data = raw_result.get('data') or {}
        decision = data.get('decision') or {}
        trade_date = (request_payload.get('trade_date') or '').strip() or str(data.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        conclusion_summary = (
            (request_payload.get('conclusion_summary') or '').strip()
            or str(decision.get('summary') or '').strip()
            or str(data.get('logic') or '').strip()
            or str(decision.get('logic') or '').strip()
        )
        return {
            'watch_stock_id': (watch_stock or {}).get('id', ''),
            'holding_stock_id': holding_stock.get('id', ''),
            'analysis_scene': 'holding_reanalysis',
            'stock_code': holding_stock.get('stock_code', ''),
            'stock_name': holding_stock.get('stock_name', ''),
            'market': holding_stock.get('market', ''),
            'trade_date': trade_date,
            'analysis_mode': str(data.get('analysis_mode') or 'agentic').strip(),
            'stance': str(data.get('stance') or decision.get('stance') or 'reanalysis').strip(),
            'time_horizon': str(data.get('time_horizon') or decision.get('time_horizon') or '').strip(),
            'conclusion_summary': conclusion_summary,
            'risk_level': str(decision.get('risk_level') or '').strip(),
            'scores_json': data.get('scores') or {},
            'signals_json': data.get('signals') or [],
            'risks_json': data.get('risks') or [],
            'evidence_json': data.get('evidence') or [],
            'raw_result_json': raw_result,
        }

    def build_holding_reanalysis_api_payload(self, holding_stock_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')
        linked_watch_stock_id = (holding_stock.get('linked_watch_stock_id') or '').strip()
        watch_stock = self.get_watch_stock(linked_watch_stock_id) if linked_watch_stock_id else None
        normalized_payload = dict(payload)
        normalized_payload['holding_stock_id'] = holding_stock_id
        normalized_payload['analysis_scene'] = 'holding_reanalysis'
        normalized_payload['watch_stock_id'] = linked_watch_stock_id
        normalized_payload['stock_code'] = normalized_payload.get('stock_code') or holding_stock.get('stock_code') or ''
        normalized_payload['stock_name'] = normalized_payload.get('stock_name') or holding_stock.get('stock_name') or ''
        normalized_payload['market'] = normalized_payload.get('market') or holding_stock.get('market') or ''
        normalized_payload['reanalysis_context'] = self.build_holding_reanalysis_context(holding_stock_id, normalized_payload)
        if watch_stock:
            normalized_payload['watch_stock_context'] = watch_stock
        return normalized_payload

    def build_holding_reanalysis_tabs(self, raw_result: dict[str, Any]) -> list[dict[str, Any]]:
        data = raw_result.get('data') or {}
        decision = data.get('decision') or {}
        snapshot = data.get('snapshot') or {}
        scores = data.get('scores') or {}
        return [
            {
                'id': 'fundamental-changes',
                'label': '基本面变化',
                'content': str(data.get('logic') or decision.get('logic') or '待补充').strip() or '待补充',
            },
            {
                'id': 'valuation-crowding',
                'label': '估值与交易拥挤度',
                'content': json.dumps({'scores': scores, 'snapshot': snapshot}, ensure_ascii=False, indent=2, default=str),
            },
            {
                'id': 'risk-catalyst',
                'label': '风险与催化',
                'content': json.dumps(data.get('risks') or decision.get('risks') or [], ensure_ascii=False, indent=2, default=str),
            },
            {
                'id': 'market-sentiment',
                'label': '市场情绪',
                'content': json.dumps(data.get('signals') or [], ensure_ascii=False, indent=2, default=str),
            },
            {
                'id': 'adjustment-advice',
                'label': '调整建议',
                'content': str(decision.get('summary') or data.get('logic') or '待补充').strip() or '待补充',
            },
        ]

    def update_holding_stock_from_reanalysis_record(self, record: dict[str, Any]) -> dict[str, Any] | None:
        holding_stock_id = (record.get('holding_stock_id') or '').strip()
        if not holding_stock_id:
            return None
        return self.holding_repository.update(
            holding_stock_id,
            {
                'suggested_action': record.get('conclusion_summary') or '',
                'last_review_at': record.get('trade_date') or '',
            },
        )

    def save_holding_reanalysis_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        holding_stock_id = (payload.get('holding_stock_id') or '').strip()
        if not holding_stock_id:
            raise ValueError('缺少 holding_stock_id')

        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')

        linked_watch_stock_id = (holding_stock.get('linked_watch_stock_id') or '').strip()
        watch_stock = self.get_watch_stock(linked_watch_stock_id) if linked_watch_stock_id else None
        raw_result = payload.get('raw_result') or {}
        if raw_result and not isinstance(raw_result, dict):
            raise ValueError('raw_result 必须是对象')

        if isinstance(raw_result.get('data'), dict):
            raw_result = dict(raw_result)
            raw_result['data'] = dict(raw_result.get('data') or {})
            raw_result['data']['analysis_scene'] = 'holding_reanalysis'
            raw_result['data']['holding_stock_id'] = holding_stock_id
            raw_result['data']['watch_stock_id'] = linked_watch_stock_id
            raw_result['data']['holding_reanalysis_tabs'] = self.build_holding_reanalysis_tabs(raw_result)

        record_payload = self.build_holding_reanalysis_record_payload(raw_result, holding_stock, watch_stock, payload)
        created = self.stock_analysis_record_repository.create(record_payload)
        formatted = self._format_stock_analysis_record(created)
        self.update_holding_stock_from_reanalysis_record(formatted)
        return formatted

    def build_holding_reanalysis_detail_url(self, holding_stock_id: str, record_id: str) -> str:
        return f'/holding-reanalysis?holding_stock_id={holding_stock_id}&record_id={record_id}'

    def build_holding_reanalysis_history_items(self, holding_stock_id: str, limit: int = 10) -> list[dict[str, Any]]:
        return [
            {
                **record,
                'detail_url': self.build_holding_reanalysis_detail_url(holding_stock_id, record.get('id', '')),
            }
            for record in self.list_holding_reanalysis_records(holding_stock_id, limit=limit)
        ]

    def build_holding_reanalysis_summary(self, holding_stock_id: str) -> dict[str, Any]:
        records = self.list_holding_reanalysis_records(holding_stock_id, limit=1)
        latest = records[0] if records else None
        return {
            'count': len(self.list_holding_reanalysis_records(holding_stock_id, limit=100)),
            'latest': latest,
        }

    def get_holding_reanalysis_record(self, record_id: str) -> dict[str, Any] | None:
        record = self.get_stock_analysis_record(record_id)
        if not record:
            return None
        return record if (record.get('analysis_scene') or '') == 'holding_reanalysis' else None

    def build_holding_reanalysis_form_defaults(self, holding_stock_id: str) -> dict[str, Any]:
        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')
        return {
            'stock_code': holding_stock.get('stock_code', ''),
            'market': self._normalize_lookup_market(holding_stock.get('market') or ''),
            'trade_date': datetime.now().strftime('%Y-%m-%d'),
            'analysis_depth': 'standard',
        }

    def build_focus_holding_reanalysis_page_context(self, holding_stock_id: str, record_id: str | None = None) -> dict[str, Any]:
        page_data = self.build_focus_holding_reanalysis_page_data(holding_stock_id, record_id)
        page_data['form_defaults'] = self.build_holding_reanalysis_form_defaults(holding_stock_id)
        page_data['history_items'] = self.build_holding_reanalysis_history_items(holding_stock_id, limit=10)
        return page_data

    def build_holding_reanalysis_page_context(self, holding_stock_id: str, record_id: str | None = None) -> dict[str, Any]:
        return self.build_focus_holding_reanalysis_page_context(holding_stock_id, record_id)

    def build_holding_review_page_data(self, holding_stock_id: str, record_id: str | None = None) -> dict[str, Any]:
        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')

        linked_watch_stock_id = (holding_stock.get('linked_watch_stock_id') or '').strip()
        watch_stock = self.get_watch_stock(linked_watch_stock_id) if linked_watch_stock_id else None
        if watch_stock:
            watch_stock = self._hydrate_watch_stock_market_metrics(watch_stock)

        history_items = self.list_holding_review_records(holding_stock_id, limit=10)
        selected_record = None
        if record_id:
            selected_record = self.get_holding_review_record(record_id)
            if not selected_record or selected_record.get('holding_stock_id') != holding_stock_id:
                raise ValueError('持仓复盘记录不存在')
        elif history_items:
            selected_record = history_items[0]

        latest_position_decision = self.list_position_decision_records(holding_stock_id, limit=1)[0] if self.list_position_decision_records(holding_stock_id, limit=1) else None
        latest_reanalysis = self.list_holding_reanalysis_records(holding_stock_id, limit=1)[0] if self.list_holding_reanalysis_records(holding_stock_id, limit=1) else None
        return {
            'page_mode': 'holding_review',
            'page_title': '持仓复盘',
            'page_description': '基于当前持仓、成交轨迹、原始决策与报表市场数据，由交易专家生成结构化持仓复盘草案。',
            'holding_stock': holding_stock,
            'watch_stock': watch_stock,
            'display_market': holding_stock.get('market') or 'A股',
            'selected_record': ({**selected_record, 'raw_result_json': self.build_holding_review_prefill_result(selected_record)} if selected_record else None),
            'history_items': history_items,
            'latest_position_decision': latest_position_decision,
            'latest_reanalysis': latest_reanalysis,
            'form_defaults': self.build_holding_review_form_defaults(holding_stock_id, selected_record),
        }

    def build_position_decision_page_data(self, holding_stock_id: str, record_id: str | None = None) -> dict[str, Any]:
        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')

        linked_watch_stock_id = (holding_stock.get('linked_watch_stock_id') or '').strip()
        watch_stock = self.get_watch_stock(linked_watch_stock_id) if linked_watch_stock_id else None
        if watch_stock:
            watch_stock = self._hydrate_watch_stock_market_metrics(watch_stock)

        history_items = self.list_position_decision_records(holding_stock_id, limit=10)
        selected_record = None
        if record_id:
            selected_record = self.get_position_decision_record(record_id)
            if not selected_record or selected_record.get('holding_stock_id') != holding_stock_id:
                raise ValueError('买卖决策记录不存在')
        elif history_items:
            selected_record = history_items[0]

        latest_trade_plan = self.list_trade_plan_analysis_records(linked_watch_stock_id, limit=1)[0] if linked_watch_stock_id and self.list_trade_plan_analysis_records(linked_watch_stock_id, limit=1) else None
        latest_reanalysis = self.list_holding_reanalysis_records(holding_stock_id, limit=1)[0] if self.list_holding_reanalysis_records(holding_stock_id, limit=1) else None
        return {
            'page_mode': 'position_decision',
            'page_title': '买卖决策',
            'page_description': '基于当前持仓、财报数据、历史成交与持仓计划，由股票分析师生成真实可执行的买卖决策草案。',
            'holding_stock': holding_stock,
            'watch_stock': watch_stock,
            'display_market': holding_stock.get('market') or 'A股',
            'selected_record': ({**selected_record, 'raw_result_json': self.build_position_decision_prefill_result(selected_record)} if selected_record else None),
            'history_items': history_items,
            'latest_trade_plan': latest_trade_plan,
            'latest_reanalysis': latest_reanalysis,
            'form_defaults': self.build_position_decision_form_defaults(holding_stock_id, selected_record),
        }

    def build_holding_review_form_defaults(self, holding_stock_id: str, selected_record: dict[str, Any] | None = None) -> dict[str, Any]:
        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')
        return {
            'trade_date': (selected_record or {}).get('trade_date') or datetime.now().strftime('%Y-%m-%d'),
            'review_type': (selected_record or {}).get('review_type') or 'general',
            'period_key': (selected_record or {}).get('period_key') or '',
            'analysis_depth': (selected_record or {}).get('analysis_depth') or 'standard',
        }

    def build_position_decision_form_defaults(self, holding_stock_id: str, selected_record: dict[str, Any] | None = None) -> dict[str, Any]:
        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')
        return {
            'trade_date': (selected_record or {}).get('trade_date') or datetime.now().strftime('%Y-%m-%d'),
            'analysis_depth': (selected_record or {}).get('analysis_depth') or 'standard',
        }

    def build_holding_review_context(self, holding_stock_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')

        linked_watch_stock_id = (holding_stock.get('linked_watch_stock_id') or '').strip()
        watch_stock = self.get_watch_stock(linked_watch_stock_id) if linked_watch_stock_id else None
        if watch_stock:
            watch_stock = self._hydrate_watch_stock_market_metrics(watch_stock)

        request_payload = self.build_holding_review_request(payload)
        stock_code = (holding_stock.get('stock_code') or '').strip()
        market = self._normalize_lookup_market(holding_stock.get('market') or '')
        snapshot = self.data_facade.build_snapshot(stock_code=stock_code, market=market, trade_date=request_payload['trade_date'])
        trades = self.get_holding_stock_trades(holding_stock_id, limit=30)
        lots = self.get_holding_stock_lots(holding_stock_id, limit=30)
        entry_decision_records = self.list_entry_decision_records(linked_watch_stock_id, limit=3) if linked_watch_stock_id else []
        reanalysis_records = self.list_holding_reanalysis_records(holding_stock_id, limit=3)
        position_decision_records = self.list_position_decision_records(holding_stock_id, limit=3)
        recent_trade_steps = [
            {
                'trade_type': item.get('trade_type', ''),
                'trade_date': item.get('trade_date', ''),
                'price': item.get('price'),
                'quantity': item.get('quantity'),
                'amount': item.get('amount'),
                'note': item.get('note', ''),
            }
            for item in trades[:3]
        ]
        return {
            'holding_stock': holding_stock,
            'watch_stock': watch_stock,
            'request': request_payload,
            'trade_history_context': {
                'trades': trades,
                'lots': lots,
                'recent_trade_steps': recent_trade_steps,
            },
            'entry_context': {
                'latest_entry_decision': entry_decision_records[0] if entry_decision_records else None,
                'entry_decision_history': entry_decision_records,
            },
            'reanalysis_context': {
                'latest_reanalysis': reanalysis_records[0] if reanalysis_records else None,
                'reanalysis_history': reanalysis_records,
            },
            'position_decision_context': {
                'latest_position_decision': position_decision_records[0] if position_decision_records else None,
                'position_decision_history': position_decision_records,
            },
            'financial_context': {
                'company_profile': self._simplify_records(snapshot.get('company_profile'), limit=5),
                'financial_indicators': self._simplify_records(snapshot.get('financial_indicators'), limit=10),
                'reports': snapshot.get('reports') or {},
            },
            'market_context': {
                'technical': snapshot.get('technical') or {},
                'sentiment': snapshot.get('sentiment') or {},
                'market_context': snapshot.get('market_context') or {},
                'news': self._simplify_records(snapshot.get('news'), limit=5),
            },
            'review_focus_context': {
                'suggested_action': holding_stock.get('suggested_action') or '',
                'unrealized_pnl': holding_stock.get('unrealized_pnl'),
                'unrealized_pnl_pct': holding_stock.get('unrealized_pnl_pct'),
                'quantity': holding_stock.get('quantity'),
                'last_review_at': holding_stock.get('last_review_at') or '',
            },
            'data_source': 'holding_snapshot',
        }

    def build_position_decision_context(self, holding_stock_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')

        linked_watch_stock_id = (holding_stock.get('linked_watch_stock_id') or '').strip()
        watch_stock = self.get_watch_stock(linked_watch_stock_id) if linked_watch_stock_id else None
        if watch_stock:
            watch_stock = self._hydrate_watch_stock_market_metrics(watch_stock)

        request_payload = self.build_position_decision_request(payload)
        stock_code = (holding_stock.get('stock_code') or '').strip()
        market = self._normalize_lookup_market(holding_stock.get('market') or '')
        snapshot = self.data_facade.build_snapshot(stock_code=stock_code, market=market, trade_date=request_payload['trade_date'])
        trades = self.get_holding_stock_trades(holding_stock_id, limit=30)
        lots = self.get_holding_stock_lots(holding_stock_id, limit=30)
        trade_plan_records = self.list_trade_plan_analysis_records(linked_watch_stock_id, limit=3) if linked_watch_stock_id else []
        stock_analysis_records = self.list_stock_analysis_records(watch_stock_id=linked_watch_stock_id, limit=3) if linked_watch_stock_id else []
        entry_decision_records = self.list_entry_decision_records(linked_watch_stock_id, limit=3) if linked_watch_stock_id else []
        return {
            'holding_stock': holding_stock,
            'watch_stock': watch_stock,
            'request': request_payload,
            'financial_context': {
                'company_profile': self._simplify_records(snapshot.get('company_profile'), limit=5),
                'financial_indicators': self._simplify_records(snapshot.get('financial_indicators'), limit=10),
                'reports': snapshot.get('reports') or {},
            },
            'trade_history_context': {
                'trades': trades,
                'lots': lots,
                'market_snapshot': {
                    'technical': snapshot.get('technical') or {},
                    'sentiment': snapshot.get('sentiment') or {},
                    'market_context': snapshot.get('market_context') or {},
                    'news': self._simplify_records(snapshot.get('news'), limit=5),
                },
            },
            'holding_plan_context': {
                'latest_trade_plan': trade_plan_records[0] if trade_plan_records else None,
                'trade_plan_history': trade_plan_records,
            },
            'supporting_context': {
                'stock_analysis_history': stock_analysis_records,
                'entry_decision_history': entry_decision_records,
            },
            'data_source': 'holding_snapshot',
        }

    def build_holding_review_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            'trade_date': (payload.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d'),
            'review_type': (payload.get('review_type') or 'general').strip() or 'general',
            'period_key': (payload.get('period_key') or '').strip(),
            'analysis_depth': (payload.get('analysis_depth') or 'standard').strip() or 'standard',
            'client_id': (payload.get('client_id') or '').strip() or None,
        }

    def build_position_decision_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            'trade_date': (payload.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d'),
            'analysis_depth': (payload.get('analysis_depth') or 'standard').strip() or 'standard',
            'client_id': (payload.get('client_id') or '').strip() or None,
        }

    def build_holding_review_record_payload(self, raw_result: dict[str, Any], holding_stock: dict[str, Any], watch_stock: dict[str, Any] | None, request_payload: dict[str, Any]) -> dict[str, Any]:
        normalized_result = self._normalize_holding_review_result(raw_result)
        data = dict(normalized_result.get('data') or {})
        trade_date = (request_payload.get('trade_date') or '').strip() or str(data.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        review_type = (request_payload.get('review_type') or '').strip() or str(data.get('review_type') or 'general').strip()
        period_key = (request_payload.get('period_key') or '').strip() or str(data.get('period_key') or '').strip()
        analysis_depth = (request_payload.get('analysis_depth') or '').strip() or str(data.get('analysis_depth') or 'standard').strip()
        data['holding_stock_id'] = holding_stock.get('id', '')
        data['watch_stock_id'] = (watch_stock or {}).get('id', '')
        data['stock_code'] = holding_stock.get('stock_code', '')
        data['stock_name'] = holding_stock.get('stock_name', '')
        data['market'] = holding_stock.get('market', '')
        data['trade_date'] = trade_date
        data['review_type'] = review_type
        data['period_key'] = period_key
        data['analysis_depth'] = analysis_depth
        normalized_result = {**normalized_result, 'data': data}
        return {
            'holding_stock_id': holding_stock.get('id', ''),
            'watch_stock_id': (watch_stock or {}).get('id', ''),
            'stock_code': holding_stock.get('stock_code', ''),
            'stock_name': holding_stock.get('stock_name', ''),
            'market': holding_stock.get('market', ''),
            'trade_date': trade_date,
            'review_type': review_type,
            'period_key': period_key,
            'analysis_depth': analysis_depth,
            'performance_summary': str(data.get('performance_summary') or '').strip(),
            'execution_summary': str(data.get('execution_summary') or '').strip(),
            'risk_summary': str(data.get('risk_summary') or '').strip(),
            'discipline_summary': str(data.get('discipline_summary') or '').strip(),
            'next_action_summary': str(data.get('next_action_summary') or '').strip(),
            'conclusion_tag': str(data.get('conclusion_tag') or '').strip(),
            'tabs_json': data.get('tabs') or [],
            'evidence_json': data.get('evidence') or [],
            'context_snapshot_json': data.get('context_snapshot') or {},
            'raw_result_json': normalized_result,
        }

    def build_position_decision_record_payload(self, raw_result: dict[str, Any], holding_stock: dict[str, Any], watch_stock: dict[str, Any] | None, request_payload: dict[str, Any]) -> dict[str, Any]:
        normalized_result = self._normalize_position_decision_result(raw_result)
        data = dict(normalized_result.get('data') or {})
        decision = data.get('decision') or {}
        tabs = data.get('tabs') or []
        summary_map = {str(item.get('title') or ''): str(item.get('summary') or '').strip() for item in tabs if isinstance(item, dict)}
        trade_date = (request_payload.get('trade_date') or '').strip() or str(data.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        analysis_depth = (request_payload.get('analysis_depth') or '').strip() or str(data.get('analysis_depth') or 'standard').strip()
        data['holding_stock_id'] = holding_stock.get('id', '')
        data['watch_stock_id'] = (watch_stock or {}).get('id', '')
        data['stock_code'] = holding_stock.get('stock_code', '')
        data['stock_name'] = holding_stock.get('stock_name', '')
        data['market'] = holding_stock.get('market', '')
        data['trade_date'] = trade_date
        data['analysis_depth'] = analysis_depth
        normalized_result = {**normalized_result, 'data': data}
        return {
            'holding_stock_id': holding_stock.get('id', ''),
            'watch_stock_id': (watch_stock or {}).get('id', ''),
            'stock_code': holding_stock.get('stock_code', ''),
            'stock_name': holding_stock.get('stock_name', ''),
            'market': holding_stock.get('market', ''),
            'trade_date': trade_date,
            'analysis_depth': analysis_depth,
            'decision_type': str(decision.get('action') or '').strip(),
            'decision_status': str(decision.get('status') or '').strip(),
            'conclusion_summary': str(decision.get('summary') or data.get('conclusion_summary') or '').strip(),
            'trigger_summary': summary_map.get('触发条件', ''),
            'reason_summary': summary_map.get('核心理由', ''),
            'execution_summary': summary_map.get('执行注意事项', ''),
            'risk_summary': summary_map.get('风险分析', ''),
            'confidence': str(decision.get('confidence') or '').strip(),
            'tabs_json': tabs,
            'evidence_json': data.get('evidence') or [],
            'raw_result_json': normalized_result,
        }
    def _normalize_position_decision_result(self, raw_result: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(raw_result, dict):
            return {'success': True, 'data': {}}
        if isinstance(raw_result.get('data'), dict):
            return raw_result

        tabs = raw_result.get('tabs') if isinstance(raw_result.get('tabs'), list) else []
        recommended_action = str(raw_result.get('recommended_action') or 'watch').strip().lower() or 'watch'
        decision_status = str(raw_result.get('decision_status') or self._map_position_decision_status(recommended_action)).strip()
        confidence = str(raw_result.get('confidence') or 'medium').strip() or 'medium'
        conclusion_summary = str(raw_result.get('conclusion_summary') or '').strip()
        evidence = []
        for tab in tabs:
            if not isinstance(tab, dict):
                continue
            for item in tab.get('evidence', []) if isinstance(tab.get('evidence'), list) else []:
                text = str(item).strip()
                if text:
                    evidence.append({'tab': str(tab.get('title') or ''), 'detail': text})
        return {
            'success': True,
            'data': {
                'holding_stock_id': '',
                'watch_stock_id': '',
                'stock_code': '',
                'stock_name': '',
                'market': '',
                'trade_date': str(raw_result.get('trade_date') or '').strip(),
                'analysis_depth': str(raw_result.get('analysis_depth') or 'standard').strip() or 'standard',
                'decision': {
                    'action': recommended_action,
                    'status': decision_status,
                    'confidence': confidence,
                    'summary': conclusion_summary,
                },
                'conclusion_summary': conclusion_summary,
                'tabs': tabs,
                'evidence': evidence,
                'meta': raw_result.get('meta') if isinstance(raw_result.get('meta'), dict) else {'role': '股票分析师'},
                'context_snapshot': raw_result.get('context_snapshot') if isinstance(raw_result.get('context_snapshot'), dict) else {},
            },
        }

    def _normalize_holding_review_result(self, raw_result: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(raw_result, dict):
            return {'success': True, 'data': {}}
        if isinstance(raw_result.get('data'), dict):
            return raw_result

        tabs = raw_result.get('tabs') if isinstance(raw_result.get('tabs'), list) else []
        evidence = []
        for tab in tabs:
            if not isinstance(tab, dict):
                continue
            for item in tab.get('evidence', []) if isinstance(tab.get('evidence'), list) else []:
                text = str(item).strip()
                if text:
                    evidence.append({'tab': str(tab.get('title') or ''), 'detail': text})
        return {
            'success': True,
            'data': {
                'holding_stock_id': '',
                'watch_stock_id': '',
                'stock_code': '',
                'stock_name': '',
                'market': '',
                'trade_date': str(raw_result.get('trade_date') or '').strip(),
                'review_type': str(raw_result.get('review_type') or 'general').strip() or 'general',
                'period_key': str(raw_result.get('period_key') or '').strip(),
                'analysis_depth': str(raw_result.get('analysis_depth') or 'standard').strip() or 'standard',
                'performance_summary': str(raw_result.get('performance_summary') or '').strip(),
                'execution_summary': str(raw_result.get('execution_summary') or '').strip(),
                'risk_summary': str(raw_result.get('risk_summary') or '').strip(),
                'discipline_summary': str(raw_result.get('discipline_summary') or '').strip(),
                'next_action_summary': str(raw_result.get('next_action_summary') or '').strip(),
                'conclusion_tag': str(raw_result.get('conclusion_tag') or '').strip(),
                'tabs': tabs,
                'evidence': evidence,
                'meta': raw_result.get('meta') if isinstance(raw_result.get('meta'), dict) else {'role': '交易专家'},
                'context_snapshot': raw_result.get('context_snapshot') if isinstance(raw_result.get('context_snapshot'), dict) else {},
            },
        }

    def _map_position_decision_status(self, action: str) -> str:
        mapping = {
            'buy': 'buy_candidate',
            'reduce': 'reduce_candidate',
            'sell': 'sell_candidate',
            'watch': 'observe',
            'hold': 'observe',
        }
        return mapping.get(action, 'observe')


    def save_holding_review_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        holding_stock_id = (payload.get('holding_stock_id') or '').strip()
        if not holding_stock_id:
            raise ValueError('缺少 holding_stock_id')
        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')
        linked_watch_stock_id = (holding_stock.get('linked_watch_stock_id') or '').strip()
        watch_stock = self.get_watch_stock(linked_watch_stock_id) if linked_watch_stock_id else None
        raw_result = payload.get('raw_result') or {}
        if raw_result and not isinstance(raw_result, dict):
            raise ValueError('raw_result 必须是对象')
        record_payload = self.build_holding_review_record_payload(raw_result, holding_stock, watch_stock, payload)
        created = self.holding_review_record_repository.create(record_payload)
        formatted = self._format_holding_review_record(created)
        self.holding_repository.update(
            holding_stock_id,
            {
                'suggested_action': formatted.get('next_action_summary') or holding_stock.get('suggested_action') or '',
                'last_review_at': formatted.get('trade_date') or '',
            },
        )
        return formatted

    def save_position_decision_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        holding_stock_id = (payload.get('holding_stock_id') or '').strip()
        if not holding_stock_id:
            raise ValueError('缺少 holding_stock_id')
        holding_stock = self.get_holding_stock(holding_stock_id)
        if not holding_stock:
            raise ValueError('持仓不存在')
        linked_watch_stock_id = (holding_stock.get('linked_watch_stock_id') or '').strip()
        watch_stock = self.get_watch_stock(linked_watch_stock_id) if linked_watch_stock_id else None
        raw_result = payload.get('raw_result') or {}
        if raw_result and not isinstance(raw_result, dict):
            raise ValueError('raw_result 必须是对象')
        record_payload = self.build_position_decision_record_payload(raw_result, holding_stock, watch_stock, payload)
        created = self.position_decision_record_repository.create(record_payload)
        formatted = self._format_position_decision_record(created)
        self.holding_repository.update(
            holding_stock_id,
            {
                'suggested_action': self._map_position_decision_action_to_label(formatted.get('decision_type') or ''),
                'last_review_at': formatted.get('trade_date') or '',
            },
        )
        return formatted

    def list_holding_review_records(self, holding_stock_id: str, limit: int = 10) -> list[dict[str, Any]]:
        rows = self.holding_review_record_repository.list_by_holding_stock(holding_stock_id, limit=limit)
        return [self._format_holding_review_record(row) for row in rows]

    def get_holding_review_record(self, record_id: str) -> dict[str, Any] | None:
        row = self.holding_review_record_repository.get_by_id(record_id)
        return self._format_holding_review_record(row) if row else None

    def list_position_decision_records(self, holding_stock_id: str, limit: int = 10) -> list[dict[str, Any]]:
        rows = self.position_decision_record_repository.list_by_holding_stock(holding_stock_id, limit=limit)
        return [self._format_position_decision_record(row) for row in rows]

    def get_position_decision_record(self, record_id: str) -> dict[str, Any] | None:
        row = self.position_decision_record_repository.get_by_id(record_id)
        return self._format_position_decision_record(row) if row else None

    def list_stock_analysis_records(self, watch_stock_id: str = '', limit: int = 10, holding_stock_id: str = '') -> list[dict[str, Any]]:
        if holding_stock_id:
            rows = self.stock_analysis_record_repository.list_by_holding_stock(holding_stock_id, limit=limit)
            return [self._format_stock_analysis_record(row) for row in rows]
        rows = self.stock_analysis_record_repository.list_by_watch_stock(watch_stock_id, limit=limit)
        return [self._format_stock_analysis_record(row) for row in rows]

    def get_stock_analysis_record(self, record_id: str) -> dict[str, Any] | None:
        row = self.stock_analysis_record_repository.get_by_id(record_id)
        return self._format_stock_analysis_record(row) if row else None

    def save_focus_stock_analysis_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        watch_stock_id = (payload.get('watch_stock_id') or '').strip()
        if not watch_stock_id:
            raise ValueError('缺少 watch_stock_id')

        watch_stock = self.get_watch_stock(watch_stock_id)
        if not watch_stock:
            raise ValueError('关注股票不存在')

        raw_result = payload.get('raw_result') or {}
        if raw_result and not isinstance(raw_result, dict):
            raise ValueError('raw_result 必须是对象')

        record_payload = self.build_stock_analysis_record_payload(raw_result, watch_stock, payload)
        created = self.stock_analysis_record_repository.create(record_payload)
        formatted = self._format_stock_analysis_record(created)
        self.update_watch_stock(
            watch_stock_id,
            {
                'last_conclusion_summary': formatted.get('conclusion_summary') or watch_stock.get('last_conclusion_summary') or '',
                'last_analysis_at': formatted.get('trade_date') or watch_stock.get('last_analysis_at') or '',
            },
        )
        return formatted

    # 兼容旧共享保存入口，真实保存逻辑已拆到 Focus/Holding 显式入口。
    def save_stock_analysis_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        analysis_scene = (payload.get('analysis_scene') or '').strip()
        if analysis_scene == 'holding_reanalysis' or (payload.get('holding_stock_id') or '').strip():
            return self.save_holding_reanalysis_record(payload)
        return self.save_focus_stock_analysis_record(payload)

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

        watch_stock = self._hydrate_watch_stock_market_metrics(watch_stock)
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

    def build_trade_plan_analysis_context(self, watch_stock_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        watch_stock = self.get_watch_stock(watch_stock_id)
        if not watch_stock:
            raise ValueError('关注股票不存在')

        watch_stock = self._hydrate_watch_stock_market_metrics(watch_stock)
        request_payload = self._normalize_trade_plan_request(payload)
        cache_context = self._load_trade_plan_cache_context(watch_stock, request_payload['trade_date'])
        fallback_context = self._build_trade_plan_fallback_context(watch_stock, request_payload['trade_date'])
        data_source = self._resolve_trade_plan_data_source(cache_context)
        return {
            'watch_stock': watch_stock,
            'request': request_payload,
            'template_markdown': self._load_trade_plan_template_markdown(),
            'cache_context': cache_context,
            'fallback_context': fallback_context,
            'data_source': data_source,
            'role_instruction': self.trade_plan_role_instruction,
        }

    def build_trade_plan_response_context(self, trade_plan_context: dict[str, Any]) -> dict[str, Any]:
        watch_stock = trade_plan_context.get('watch_stock') or {}
        request_payload = trade_plan_context.get('request') or {}
        cache_context = trade_plan_context.get('cache_context') or {}
        return {
            'watch_stock_id': watch_stock.get('id', ''),
            'stock_code': watch_stock.get('stock_code', ''),
            'stock_name': watch_stock.get('stock_name', ''),
            'market': watch_stock.get('market', ''),
            'trade_date': request_payload.get('trade_date', ''),
            'plan_type': request_payload.get('plan_type', ''),
            'risk_preference': request_payload.get('risk_preference', ''),
            'analysis_depth': request_payload.get('analysis_depth', 'standard'),
            'template_name': self.trade_plan_template_name,
            'data_source': trade_plan_context.get('data_source', 'fallback_only'),
            'cache_hits': cache_context.get('cache_hits', []),
        }

    def _normalize_trade_plan_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            'trade_date': (payload.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d'),
            'plan_type': (payload.get('plan_type') or '三笔计划').strip() or '三笔计划',
            'risk_preference': (payload.get('risk_preference') or '中高风险').strip() or '中高风险',
            'analysis_depth': (payload.get('analysis_depth') or 'standard').strip() or 'standard',
            'client_id': (payload.get('client_id') or '').strip() or None,
        }

    def _resolve_trade_plan_data_source(self, cache_context: dict[str, Any]) -> str:
        hit_types = set(cache_context.get('hit_types') or [])
        if {'entry_decision', 'stock_analysis'}.issubset(hit_types):
            return 'cache_first'
        if hit_types:
            return 'partial_cache_fallback'
        return 'fallback_only'

    def _load_trade_plan_template_markdown(self) -> str:
        try:
            return self.trade_plan_template_path.read_text(encoding='utf-8').strip()
        except Exception:
            return ''

    def save_result_markdown_cache(self, result_type: str, result: dict[str, Any], watch_stock: dict[str, Any] | None = None) -> str | None:
        cache_payload = self._build_result_cache_payload(result_type, result, watch_stock)
        if not cache_payload:
            return None
        self.trade_plan_cache_dir.mkdir(parents=True, exist_ok=True)
        file_path = self.trade_plan_cache_dir / self._build_trade_plan_cache_filename(
            cache_payload['market'],
            cache_payload['stock_code'],
            cache_payload['stock_name'],
            cache_payload['biz'],
            cache_payload['trade_date'],
        )
        file_path.write_text(cache_payload['markdown'], encoding='utf-8')
        return str(file_path)

    def _build_result_cache_payload(self, result_type: str, result: dict[str, Any], watch_stock: dict[str, Any] | None = None) -> dict[str, str] | None:
        if not isinstance(result, dict) or not result.get('success'):
            return None
        data = result.get('data') or {}
        if not isinstance(data, dict):
            return None
        watch_stock = watch_stock or {}
        market = str(data.get('market') or watch_stock.get('market') or '').strip()
        stock_code = str(data.get('stock_code') or watch_stock.get('stock_code') or '').strip()
        stock_name = str(data.get('stock_name') or watch_stock.get('stock_name') or '').strip()
        trade_date = str(data.get('trade_date') or datetime.now().strftime('%Y-%m-%d')).strip()
        biz = self.trade_plan_cache_biz_markers.get(result_type)
        if not (market and stock_code and stock_name and trade_date and biz):
            return None
        if result_type == 'entry_decision':
            markdown = self._build_entry_decision_cache_markdown(result)
        elif result_type == 'trade_plan':
            markdown = self._build_trade_plan_cache_markdown(result)
        elif result_type in ('stock_analysis', 'holding_reanalysis'):
            markdown = self._build_stock_analysis_cache_markdown(result)
        elif result_type == 'position_decision':
            markdown = self._build_position_decision_cache_markdown(result)
        elif result_type == 'holding_review':
            markdown = self._build_holding_review_cache_markdown(result)
        else:
            return None
        markdown = markdown.strip()
        if not markdown:
            return None
        return {
            'market': market,
            'stock_code': stock_code,
            'stock_name': stock_name,
            'trade_date': trade_date,
            'biz': biz,
            'markdown': markdown,
        }

    def _build_trade_plan_cache_filename(self, market: str, stock_code: str, stock_name: str, biz: str, trade_date: str) -> str:
        normalized_date = re.sub(r'[^0-9]', '', trade_date or '') or datetime.now().strftime('%Y%m%d')
        safe_market = self._sanitize_trade_plan_cache_part(market)
        safe_stock_code = self._sanitize_trade_plan_cache_part(stock_code)
        safe_stock_name = self._sanitize_trade_plan_cache_part(stock_name)
        safe_biz = self._sanitize_trade_plan_cache_part(biz)
        return f'{safe_market}_{safe_stock_code}_{safe_stock_name}_{safe_biz}_{normalized_date}_.md'

    def find_daily_result_cache(self, *, market: str, stock_code: str, stock_name: str, trade_date: str, result_type: str) -> dict[str, Any]:
        biz = self.trade_plan_cache_biz_markers.get(result_type)
        if not biz or not self.trade_plan_cache_dir.exists():
            return {'hit': False, 'result_source': 'live'}
        file_name = self._build_trade_plan_cache_filename(market, stock_code, stock_name, biz, trade_date)
        file_path = self.trade_plan_cache_dir / file_name
        if not file_path.exists() or not file_path.is_file():
            return {'hit': False, 'result_source': 'live'}
        markdown = self._read_text_file(file_path)
        return {
            'hit': bool(markdown),
            'result_source': 'cache' if markdown else 'live',
            'file_path': str(file_path),
            'file_name': file_name,
            'markdown': markdown,
            'biz': biz,
        }

    def build_cached_stock_analysis_result(self, *, market: str, stock_code: str, stock_name: str, trade_date: str) -> dict[str, Any] | None:
        cache = self.find_daily_result_cache(
            market=market,
            stock_code=stock_code,
            stock_name=stock_name,
            trade_date=trade_date,
            result_type='stock_analysis',
        )
        if not cache.get('hit'):
            return None
        markdown = str(cache.get('markdown') or '').strip()
        return {
            'success': True,
            'data': {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'market': market,
                'trade_date': trade_date,
                'analysis_mode': 'agentic',
                'decision': {
                    'summary': '复用当天股票分析缓存。',
                    'logic': markdown,
                    'risk_level': 'medium',
                },
                'scores': {
                    'technical': 0,
                    'fundamental': 0,
                    'sentiment': 0,
                    'composite': 0,
                },
                'signals': [],
                'risks': [],
                'evidence': [],
                'stance': 'cache',
                'logic': markdown,
                'position_suggestion': None,
                'time_horizon': '',
                'meta': {
                    'result_source': 'cache',
                    'cache_file': cache.get('file_name') or '',
                },
                'snapshot': {},
                'cached_markdown': markdown,
                'result_source': 'cache',
                'cache_file': cache.get('file_name') or '',
            },
        }

    def build_cached_entry_decision_result(self, *, watch_stock: dict[str, Any], trade_date: str) -> dict[str, Any] | None:
        market = str(watch_stock.get('market') or '').strip()
        stock_code = str(watch_stock.get('stock_code') or '').strip()
        stock_name = str(watch_stock.get('stock_name') or '').strip()
        cache = self.find_daily_result_cache(
            market=market,
            stock_code=stock_code,
            stock_name=stock_name,
            trade_date=trade_date,
            result_type='entry_decision',
        )
        if not cache.get('hit'):
            return None
        markdown = str(cache.get('markdown') or '').strip()
        summary_fields = {
            'current_stage': watch_stock.get('current_stage') or '待确认',
            'current_price_zone': watch_stock.get('current_price_zone') or '待确认',
            'suggested_action': watch_stock.get('suggested_action') or '继续观察',
            'execution_summary': watch_stock.get('last_conclusion_summary') or '复用当天进场策略缓存。',
        }
        return {
            'success': True,
            'data': {
                'watch_stock_id': watch_stock.get('id', ''),
                'stock_code': stock_code,
                'stock_name': stock_name,
                'market': market,
                'trade_date': trade_date,
                'basic_info': {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'market': market,
                    'industry': watch_stock.get('industry') or '',
                    'asset_type': watch_stock.get('asset_type') or '',
                    'current_price': watch_stock.get('current_price'),
                    'pe': watch_stock.get('pe'),
                    'investment_horizon': '',
                },
                'macro_analysis': {},
                'asset_classification': {},
                'value_stage_analysis': {
                    'current_stage': summary_fields['current_stage'],
                },
                'price_zone_analysis': {
                    'price_zone': summary_fields['current_price_zone'],
                },
                'buy_plan_analysis': {
                    'suggested_action': summary_fields['suggested_action'],
                },
                'risk_control_analysis': {
                    'conclusion_summary': summary_fields['execution_summary'],
                },
                'decision_card': summary_fields,
                'entry_decision_summary_markdown': markdown,
                'entry_decision_summary_template': '进场决策模板_空白实战版',
                'snapshot': {},
                'manual_inputs': {},
                'meta': {
                    'status': 'completed',
                    'current_role': 'summary',
                    'completed_roles': [],
                    'timeline': [],
                    'duration_ms': 0,
                    'errors': [],
                    'result_source': 'cache',
                    'cache_file': cache.get('file_name') or '',
                },
                'result_source': 'cache',
                'cache_file': cache.get('file_name') or '',
            },
        }

    def annotate_result_source(self, result: dict[str, Any], *, result_source: str, cache_file: str = '') -> dict[str, Any]:
        if not isinstance(result, dict):
            return result
        data = result.get('data')
        if not isinstance(data, dict):
            return result
        meta = data.get('meta')
        if not isinstance(meta, dict):
            meta = {}
            data['meta'] = meta
        meta['result_source'] = result_source
        if cache_file:
            meta['cache_file'] = cache_file
        data['result_source'] = result_source
        if cache_file:
            data['cache_file'] = cache_file
        return result

    def build_entry_decision_cached_response_payload(self, *, watch_stock: dict[str, Any], result: dict[str, Any], trade_date: str, client_id: str) -> dict[str, Any]:
        return {
            'status': 'completed',
            'task_mode': 'cache',
            'client_id': client_id,
            'final_result': result,
            'entry_decision_context': {
                'watch_stock_id': watch_stock.get('id', ''),
                'stock_code': watch_stock.get('stock_code', ''),
                'stock_name': watch_stock.get('stock_name', ''),
                'market': watch_stock.get('market', ''),
                'trade_date': trade_date,
                'analysis_depth': 'cache',
                'session_status': 'completed',
                'pending_save_fields': {
                    'current_stage': watch_stock.get('current_stage', ''),
                    'current_price_zone': watch_stock.get('current_price_zone', ''),
                    'suggested_action': watch_stock.get('suggested_action', ''),
                    'last_conclusion_summary': watch_stock.get('last_conclusion_summary', ''),
                },
                'generated_summary_fields': self._build_entry_decision_summary_fields_from_result(result, trade_date),
            },
        }

    def _build_entry_decision_summary_fields_from_result(self, result: dict[str, Any], trade_date: str) -> dict[str, Any]:
        data = result.get('data') or {}
        decision_card = data.get('decision_card') or {}
        risk_control = data.get('risk_control_analysis') or {}
        buy_plan = data.get('buy_plan_analysis') or {}
        value_stage = data.get('value_stage_analysis') or {}
        price_zone = data.get('price_zone_analysis') or {}
        return {
            'current_stage': str(decision_card.get('current_stage') or value_stage.get('current_stage') or '').strip(),
            'current_price_zone': str(decision_card.get('current_price_zone') or price_zone.get('price_zone') or '').strip(),
            'suggested_action': str(decision_card.get('suggested_action') or buy_plan.get('suggested_action') or '').strip(),
            'last_conclusion_summary': str(risk_control.get('conclusion_summary') or decision_card.get('execution_summary') or '').strip(),
            'last_analysis_at': trade_date or data.get('trade_date') or '',
        }

    def _sanitize_trade_plan_cache_part(self, value: str) -> str:
        text = str(value or '').strip()
        if not text:
            return 'unknown'
        return re.sub(r'[\\/:*?"<>|\s]+', '_', text)

    def _build_entry_decision_cache_markdown(self, result: dict[str, Any]) -> str:
        data = result.get('data') or {}
        markdown = str(data.get('entry_decision_summary_markdown') or '').strip()
        if markdown:
            return markdown
        return self._build_generic_result_cache_markdown('entry_decision', result)

    def _build_trade_plan_cache_markdown(self, result: dict[str, Any]) -> str:
        data = result.get('data') or {}
        markdown = str(data.get('trade_plan_markdown') or '').strip()
        if markdown:
            return markdown
        return self._build_generic_result_cache_markdown('trade_plan', result)

    def _build_stock_analysis_cache_markdown(self, result: dict[str, Any]) -> str:
        return self._build_generic_result_cache_markdown('stock_analysis', result)

    def _build_position_decision_cache_markdown(self, result: dict[str, Any]) -> str:
        data = result.get('data') or {}
        decision = data.get('decision') or {}
        tabs = data.get('tabs') or []
        sections = [
            '# 买卖决策',
            '',
            f'- 标的代码：{data.get("stock_code") or "待确认"}',
            f'- 标的名称：{data.get("stock_name") or "待确认"}',
            f'- 市场：{data.get("market") or "待确认"}',
            f'- 交易日期：{data.get("trade_date") or "待确认"}',
            '',
            '## 决策概要',
            '',
            f'- 推荐动作：{decision.get("action") or "待确认"}',
            f'- 置信度：{decision.get("confidence") or "待确认"}',
            f'- 状态：{decision.get("status") or "待确认"}',
            f'- 摘要：{decision.get("summary") or "待确认"}',
        ]
        if isinstance(tabs, list):
            for tab in tabs:
                if not isinstance(tab, dict):
                    continue
                tab_id = str(tab.get('id') or tab.get('title') or '未知').strip()
                tab_summary = str(tab.get('summary') or '待确认').strip()
                sections.extend(['', f'## {tab_id}', '', tab_summary])
                evidence = tab.get('evidence') or []
                if isinstance(evidence, list) and evidence:
                    sections.append('')
                    for idx, detail in enumerate(evidence, 1):
                        text = str(detail).strip()
                        if text:
                            sections.append(f'{idx}. {text}')
        sections.extend(['', '## 原始结果 JSON', '', '```json', json.dumps(result, ensure_ascii=False, indent=2, default=str), '```'])
        return '\n'.join(sections).strip()

    def _build_holding_review_cache_markdown(self, result: dict[str, Any]) -> str:
        data = result.get('data') or {}
        tabs = data.get('tabs') or []
        sections = [
            '# 持仓复盘',
            '',
            f'- 标的代码：{data.get("stock_code") or "待确认"}',
            f'- 标的名称：{data.get("stock_name") or "待确认"}',
            f'- 市场：{data.get("market") or "待确认"}',
            f'- 交易日期：{data.get("trade_date") or "待确认"}',
            f'- 复盘类型：{data.get("review_type") or "待确认"}',
            f'- 结论标签：{data.get("conclusion_tag") or "待确认"}',
            '',
            '## 摘要',
            '',
            f'- 绩效：{data.get("performance_summary") or "待确认"}',
            f'- 执行：{data.get("execution_summary") or "待确认"}',
            f'- 风险：{data.get("risk_summary") or "待确认"}',
            f'- 纪律：{data.get("discipline_summary") or "待确认"}',
            f'- 后续动作：{data.get("next_action_summary") or "待确认"}',
        ]
        if isinstance(tabs, list):
            for tab in tabs:
                if not isinstance(tab, dict):
                    continue
                tab_id = str(tab.get('id') or tab.get('title') or '未知').strip()
                tab_summary = str(tab.get('summary') or '待确认').strip()
                sections.extend(['', f'## {tab_id}', '', tab_summary])
                evidence = tab.get('evidence') or []
                if isinstance(evidence, list) and evidence:
                    sections.append('')
                    for idx, detail in enumerate(evidence, 1):
                        text = str(detail).strip()
                        if text:
                            sections.append(f'{idx}. {text}')
        sections.extend(['', '## 原始结果 JSON', '', '```json', json.dumps(result, ensure_ascii=False, indent=2, default=str), '```'])
        return '\n'.join(sections).strip()


    def _build_generic_result_cache_markdown(self, result_type: str, result: dict[str, Any]) -> str:
        data = result.get('data') or {}
        decision = data.get('decision') or {}
        meta = data.get('meta') or {}
        snapshot = data.get('snapshot') or {}
        title = self.trade_plan_cache_display_labels.get(result_type, result_type)
        sections = [
            f'# {title}',
            '',
            f'- 标的代码：{data.get("stock_code") or "待确认"}',
            f'- 标的名称：{data.get("stock_name") or "待确认"}',
            f'- 市场：{data.get("market") or "待确认"}',
            f'- 交易日期：{data.get("trade_date") or "待确认"}',
        ]
        if result_type == 'stock_analysis':
            scores = data.get('scores') or {}
            sections.extend(
                [
                    '',
                    '## 分析结论',
                    '',
                    f'- 立场：{decision.get("stance") or data.get("stance") or "待确认"}',
                    f'- 时间周期：{data.get("time_horizon") or "待确认"}',
                    f'- 逻辑：{data.get("logic") or decision.get("logic") or "待确认"}',
                    '',
                    '## 风险提示',
                    '',
                ]
            )
            risks = data.get('risks') or []
            if isinstance(risks, list) and risks:
                sections.extend([f'- {str(item).strip()}' for item in risks if str(item).strip()])
            else:
                sections.append('- 待确认')
            sections.extend(
                [
                    '',
                    '## 评分概览',
                    '',
                    f'- technical: {scores.get("technical", 0)}',
                    f'- fundamental: {scores.get("fundamental", 0)}',
                    f'- sentiment: {scores.get("sentiment", 0)}',
                    f'- composite: {scores.get("composite", 0)}',
                ]
            )
        sections.extend(
            [
                '',
                '## 原始结果 JSON',
                '',
                '```json',
                json.dumps(result, ensure_ascii=False, indent=2, default=str),
                '```',
            ]
        )
        if meta or snapshot:
            sections.extend(['', '<!-- metadata retained for cache replay -->'])
        return '\n'.join(sections).strip()

    def _load_trade_plan_cache_context(self, watch_stock: dict[str, Any], trade_date: str) -> dict[str, Any]:
        matched_files = self._find_trade_plan_cache_files(watch_stock, trade_date)
        files = []
        hit_types = []
        entry_decision_markdown = ''
        stock_analysis_markdown = ''
        for path in matched_files:
            content = self._read_text_file(path)
            file_type = self._classify_trade_plan_cache_file(path.name, content)
            files.append({'name': path.name, 'path': str(path), 'type': file_type, 'content': content})
            if file_type == 'entry_decision' and not entry_decision_markdown:
                entry_decision_markdown = content
                hit_types.append(file_type)
            elif file_type == 'stock_analysis' and not stock_analysis_markdown:
                stock_analysis_markdown = content
                hit_types.append(file_type)
        return {
            'cache_dir': str(self.trade_plan_cache_dir),
            'cache_hits': [item['name'] for item in files],
            'files': files,
            'entry_decision_markdown': entry_decision_markdown,
            'stock_analysis_markdown': stock_analysis_markdown,
            'hit_types': list(dict.fromkeys(hit_types)),
        }

    def _find_trade_plan_cache_files(self, watch_stock: dict[str, Any], trade_date: str) -> list[Path]:
        if not self.trade_plan_cache_dir.exists():
            return []
        stock_code = (watch_stock.get('stock_code') or '').strip()
        stock_name = (watch_stock.get('stock_name') or '').strip()
        market = (watch_stock.get('market') or '').strip()
        normalized_date = trade_date.replace('-', '')
        patterns = [
            f'{market}_{stock_code}_{stock_name}_*_{normalized_date}*.md',
            f'{market}_{stock_code}_*_{normalized_date}*.md',
            f'*_{stock_code}_*_{normalized_date}*.md',
        ]
        results: list[Path] = []
        seen: set[str] = set()
        for pattern in patterns:
            for path in sorted(self.trade_plan_cache_dir.glob(pattern)):
                key = str(path)
                if key in seen or not path.is_file():
                    continue
                seen.add(key)
                results.append(path)
        return results

    def _classify_trade_plan_cache_file(self, file_name: str, content: str) -> str:
        lowered_name = file_name.lower()
        for file_type, keywords in self.trade_plan_cache_keywords.items():
            if any(keyword.lower() in lowered_name for keyword in keywords):
                return file_type
        lowered_content = content.lower()
        for file_type, keywords in self.trade_plan_cache_keywords.items():
            if any(keyword.lower() in lowered_content for keyword in keywords):
                return file_type
        return 'unknown'

    def _build_trade_plan_fallback_context(self, watch_stock: dict[str, Any], trade_date: str) -> dict[str, Any]:
        market = self._normalize_lookup_market(watch_stock.get('market') or '')
        stock_code = (watch_stock.get('stock_code') or '').strip()
        try:
            snapshot = self.data_facade.build_snapshot(stock_code=stock_code, market=market, trade_date=trade_date)
        except Exception:
            snapshot = {
                'stock_code': stock_code,
                'market': market,
                'trade_date': trade_date,
                'company_profile': [],
                'market_context': {},
                'news': [],
                'financial_indicators': [],
                'reports': {},
                'technical': {'score': 0, 'summary': {}},
                'sentiment': {},
            }
        company_profile = snapshot.get('company_profile')
        return {
            'snapshot': snapshot,
            'basic_info': {
                'stock_code': watch_stock.get('stock_code', ''),
                'stock_name': watch_stock.get('stock_name', ''),
                'market': watch_stock.get('market', ''),
                'industry': watch_stock.get('industry', ''),
                'asset_type': watch_stock.get('asset_type', ''),
                'current_price': watch_stock.get('current_price'),
                'pe': watch_stock.get('pe'),
                'company_profile': self._simplify_records(company_profile, limit=5),
            },
            'technical': snapshot.get('technical') or {},
            'sentiment': snapshot.get('sentiment') or {},
            'financial_indicators': self._simplify_records(snapshot.get('financial_indicators'), limit=5),
            'reports': snapshot.get('reports') or {},
            'market_context': snapshot.get('market_context') or {},
            'news': self._simplify_records(snapshot.get('news'), limit=5),
        }

    def _simplify_records(self, value: Any, *, limit: int) -> Any:
        if value is None:
            return []
        if isinstance(value, list):
            return value[:limit]
        if hasattr(value, 'head') and hasattr(value, 'to_dict'):
            try:
                return value.head(limit).to_dict('records')
            except Exception:
                return []
        if isinstance(value, dict):
            return value
        return value

    def _read_text_file(self, path: Path) -> str:
        try:
            return path.read_text(encoding='utf-8').strip()
        except Exception:
            return ''

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


    def build_stock_analysis_record_payload(self, raw_result: dict[str, Any], watch_stock: dict[str, Any], request_payload: dict[str, Any]) -> dict[str, Any]:
        data = raw_result.get('data') or {}
        decision = data.get('decision') or {}
        trade_date = (request_payload.get('trade_date') or '').strip() or self._stringify_manual_value(data.get('trade_date')) or datetime.now().strftime('%Y-%m-%d')
        conclusion_summary = (
            (request_payload.get('conclusion_summary') or '').strip()
            or self._stringify_manual_value(decision.get('summary'))
            or self._stringify_manual_value(data.get('logic'))
            or self._stringify_manual_value(decision.get('logic'))
        )
        return {
            'watch_stock_id': watch_stock['id'],
            'stock_code': watch_stock.get('stock_code', ''),
            'stock_name': watch_stock.get('stock_name', ''),
            'market': watch_stock.get('market', ''),
            'trade_date': trade_date,
            'analysis_mode': self._stringify_manual_value(data.get('analysis_mode')) or 'agentic',
            'stance': self._stringify_manual_value(data.get('stance')) or self._stringify_manual_value(decision.get('stance')),
            'time_horizon': self._stringify_manual_value(data.get('time_horizon')) or self._stringify_manual_value(decision.get('time_horizon')),
            'conclusion_summary': conclusion_summary,
            'risk_level': self._stringify_manual_value(decision.get('risk_level')),
            'scores_json': data.get('scores') or {},
            'signals_json': data.get('signals') or [],
            'risks_json': data.get('risks') or [],
            'evidence_json': data.get('evidence') or [],
            'raw_result_json': raw_result,
        }

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
        meta = data.get('meta') or {}
        position_suggestion = decision.get('position_suggestion') or {}
        if not isinstance(position_suggestion, dict):
            position_suggestion = {}

        suggested_action = (request_payload.get('suggested_action') or '').strip() or self._map_ai_decision_action_to_label(decision.get('action'))
        conclusion_summary = (request_payload.get('conclusion_summary') or '').strip() or (decision.get('summary') or '').strip() or (decision.get('logic') or '').strip()
        trade_date = (request_payload.get('trade_date') or '').strip() or (data.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        plan_type = (request_payload.get('plan_type') or '').strip() or '三笔计划'
        risk_preference = (request_payload.get('risk_preference') or '').strip() or '中高风险'
        max_target_position = (request_payload.get('max_target_position') or '').strip() or str(position_suggestion.get('target_position') or '').strip()
        position_limit = (request_payload.get('position_limit') or '').strip() or str(position_suggestion.get('position_limit') or position_suggestion.get('target_position') or '').strip()
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
                'trade_plan_markdown': data.get('trade_plan_markdown') or '',
                'meta': meta,
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
        entry_plan_json = entry_plan_json if isinstance(entry_plan_json, dict) else {}
        raw_result_json = raw_result_json if isinstance(raw_result_json, dict) else {}
        decision = raw_result_json.get('data', {}).get('decision', {}) if isinstance(raw_result_json.get('data'), dict) else {}
        trade_plan_markdown = str(entry_plan_json.get('trade_plan_markdown') or '').strip()
        if not trade_plan_markdown and isinstance(raw_result_json.get('data'), dict):
            trade_plan_markdown = str(raw_result_json.get('data', {}).get('trade_plan_markdown') or '').strip()
        decision_action = str(decision.get('action') or '').strip()
        return {
            **row,
            'entry_plan_json': entry_plan_json,
            'raw_result_json': raw_result_json,
            'decision_action': decision_action,
            'decision_action_label': self._map_ai_decision_action_to_label(decision_action),
            'trade_plan_markdown': trade_plan_markdown,
        }

    def _format_watch_stock_item(self, row: dict[str, Any]) -> dict[str, Any]:
        latest_trade_plan = None
        watch_stock_id = (row.get('id') or '').strip()
        if watch_stock_id:
            records = self.list_trade_plan_analysis_records(watch_stock_id, limit=1)
            latest_trade_plan = records[0] if records else None
        return {
            **row,
            'last_trade_plan_at': (latest_trade_plan or {}).get('trade_date', ''),
            'trade_plan_status': '已有计划' if latest_trade_plan else '',
            'trade_plan_action': (latest_trade_plan or {}).get('decision_action_label', ''),
            'trade_plan_record_id': (latest_trade_plan or {}).get('id', ''),
            'last_risk_level': (latest_trade_plan or {}).get('risk_level', ''),
            'last_plan_type': (latest_trade_plan or {}).get('plan_type', ''),
            'last_risk_preference': (latest_trade_plan or {}).get('risk_preference', ''),
            'last_trade_plan_markdown': (latest_trade_plan or {}).get('trade_plan_markdown', ''),
        }

    def _format_stock_analysis_record(self, row: dict[str, Any]) -> dict[str, Any]:
        scores_json = row.get('scores_json') or {}
        signals_json = row.get('signals_json') or []
        risks_json = row.get('risks_json') or []
        evidence_json = row.get('evidence_json') or []
        raw_result_json = row.get('raw_result_json') or {}
        if isinstance(scores_json, str):
            scores_json = self._safe_json_loads(scores_json)
        if isinstance(raw_result_json, str):
            raw_result_json = self._safe_json_loads(raw_result_json)
        return {
            **row,
            'analysis_scene': (row.get('analysis_scene') or '').strip(),
            'holding_stock_id': (row.get('holding_stock_id') or '').strip(),
            'scores_json': scores_json if isinstance(scores_json, dict) else {},
            'signals_json': signals_json if isinstance(signals_json, list) else [],
            'risks_json': risks_json if isinstance(risks_json, list) else [],
            'evidence_json': evidence_json if isinstance(evidence_json, list) else [],
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

    def _format_holding_review_record(self, row: dict[str, Any]) -> dict[str, Any]:
        tabs_json = row.get('tabs_json') or []
        evidence_json = row.get('evidence_json') or []
        context_snapshot_json = row.get('context_snapshot_json') or {}
        raw_result_json = row.get('raw_result_json') or {}
        if isinstance(context_snapshot_json, str):
            context_snapshot_json = self._safe_json_loads(context_snapshot_json)
        if isinstance(raw_result_json, str):
            raw_result_json = self._safe_json_loads(raw_result_json)
        return {
            **row,
            'tabs_json': tabs_json if isinstance(tabs_json, list) else [],
            'evidence_json': evidence_json if isinstance(evidence_json, list) else [],
            'context_snapshot_json': context_snapshot_json if isinstance(context_snapshot_json, dict) else {},
            'raw_result_json': raw_result_json if isinstance(raw_result_json, dict) else {},
            'review_type_label': self._map_review_type_label(row.get('review_type') or ''),
            'conclusion_tag_label': self._map_holding_review_tag_label(row.get('conclusion_tag') or ''),
        }

    def _format_position_decision_record(self, row: dict[str, Any]) -> dict[str, Any]:
        tabs_json = row.get('tabs_json') or []
        evidence_json = row.get('evidence_json') or []
        raw_result_json = row.get('raw_result_json') or {}
        if isinstance(raw_result_json, str):
            raw_result_json = self._safe_json_loads(raw_result_json)
        return {
            **row,
            'tabs_json': tabs_json if isinstance(tabs_json, list) else [],
            'evidence_json': evidence_json if isinstance(evidence_json, list) else [],
            'raw_result_json': raw_result_json if isinstance(raw_result_json, dict) else {},
            'decision_type_label': self._map_position_decision_action_to_label(row.get('decision_type') or ''),
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
            'linked_holding_stock_id': (payload.get('linked_holding_stock_id') or '').strip(),
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

    def _map_position_decision_action_to_label(self, action: str | None) -> str:
        normalized = (action or '').strip().lower()
        mapping = {
            'buy': '适合买入',
            'reduce': '适合减仓',
            'sell': '适合卖出',
            'watch': '继续观察',
            'hold': '继续观察',
        }
        return mapping.get(normalized, action or '')

    def _map_review_type_label(self, review_type: str | None) -> str:
        normalized = (review_type or '').strip().lower()
        mapping = {
            'general': '通用复盘',
            'weekly': '周复盘',
            'monthly': '月复盘',
            'quarterly': '季度复盘',
        }
        return mapping.get(normalized, review_type or '')

    def _map_holding_review_tag_label(self, tag: str | None) -> str:
        normalized = (tag or '').strip().lower()
        mapping = {
            'logic_ok': '逻辑仍成立',
            'need_recheck': '需要重新核查',
            'execution_issue': '执行存在问题',
            'risk_rising': '风险上升',
            'prepare_reduce': '准备减仓',
            'prepare_sell': '准备卖出',
        }
        return mapping.get(normalized, tag or '')

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
