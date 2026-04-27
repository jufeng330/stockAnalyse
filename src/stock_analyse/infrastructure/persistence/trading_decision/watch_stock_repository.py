from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from stock_analyse.infrastructure.persistence.trading_decision.schema_manager import TradingDecisionSchemaManager
from stock_analyse.infrastructure.persistence.trading_decision.sqlite_connection import TradingDecisionSQLiteConnection


DEFAULT_STATUS = 'watching'
ARCHIVED_STATUS = 'archived'


@dataclass(frozen=True)
class WatchStockListResult:
    items: list[dict[str, Any]]
    summary: dict[str, int]
    pagination: dict[str, int]


class WatchStockRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.connection_factory = TradingDecisionSQLiteConnection(db_path)
        self.schema_manager = TradingDecisionSchemaManager(self.connection_factory)
        self.schema_manager.ensure_schema()

    def list(self, filters: dict[str, Any]) -> WatchStockListResult:
        page = max(int(filters.get('page') or 1), 1)
        page_size = max(min(int(filters.get('page_size') or 20), 100), 1)
        where_sql, parameters = self._build_where(filters, default_status=DEFAULT_STATUS)
        count_sql = f'SELECT COUNT(*) AS total FROM watch_stocks {where_sql}'
        offset = (page - 1) * page_size

        with self.connection_factory.connect() as connection:
            total = int(connection.execute(count_sql, parameters).fetchone()['total'])
            rows = connection.execute(
                f'''
                SELECT *
                FROM watch_stocks
                {where_sql}
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ? OFFSET ?
                ''',
                [*parameters, page_size, offset],
            ).fetchall()

        items = [self._row_to_dict(row) for row in rows]
        return WatchStockListResult(
            items=items,
            summary=self.get_summary_counts(),
            pagination={
                'page': page,
                'page_size': page_size,
                'total': total,
            },
        )

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now().isoformat(timespec='seconds')
        watch_stock = {
            'id': payload.get('id') or self._new_id(),
            'stock_code': (payload.get('stock_code') or '').strip(),
            'stock_name': (payload.get('stock_name') or '').strip(),
            'market': (payload.get('market') or '').strip(),
            'industry': (payload.get('industry') or '').strip(),
            'asset_type': (payload.get('asset_type') or '').strip(),
            'source': (payload.get('source') or '').strip(),
            'note': (payload.get('note') or '').strip(),
            'status': payload.get('status') or DEFAULT_STATUS,
            'current_price': self._to_float(payload.get('current_price')),
            'pe': self._to_float(payload.get('pe')),
            'current_stage': (payload.get('current_stage') or '').strip(),
            'current_price_zone': (payload.get('current_price_zone') or '').strip(),
            'suggested_action': (payload.get('suggested_action') or '').strip(),
            'last_conclusion_summary': (payload.get('last_conclusion_summary') or '').strip(),
            'last_analysis_at': (payload.get('last_analysis_at') or '').strip(),
            'created_at': now,
            'updated_at': now,
        }

        with self.connection_factory.connect() as connection:
            connection.execute(
                '''
                INSERT INTO watch_stocks (
                    id, stock_code, stock_name, market, industry, asset_type, source, note,
                    status, current_price, pe, current_stage, current_price_zone,
                    suggested_action, last_conclusion_summary, last_analysis_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    watch_stock['id'],
                    watch_stock['stock_code'],
                    watch_stock['stock_name'],
                    watch_stock['market'],
                    watch_stock['industry'],
                    watch_stock['asset_type'],
                    watch_stock['source'],
                    watch_stock['note'],
                    watch_stock['status'],
                    watch_stock['current_price'],
                    watch_stock['pe'],
                    watch_stock['current_stage'],
                    watch_stock['current_price_zone'],
                    watch_stock['suggested_action'],
                    watch_stock['last_conclusion_summary'],
                    watch_stock['last_analysis_at'],
                    watch_stock['created_at'],
                    watch_stock['updated_at'],
                ],
            )
            connection.commit()
        return watch_stock

    def get_by_id(self, watch_stock_id: str) -> dict[str, Any] | None:
        with self.connection_factory.connect() as connection:
            row = connection.execute(
                'SELECT * FROM watch_stocks WHERE id = ?',
                [watch_stock_id],
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def update(self, watch_stock_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        existing = self.get_by_id(watch_stock_id)
        if not existing:
            return None

        updated = {
            **existing,
            'stock_code': (payload.get('stock_code', existing['stock_code']) or '').strip(),
            'stock_name': (payload.get('stock_name', existing['stock_name']) or '').strip(),
            'market': (payload.get('market', existing['market']) or '').strip(),
            'industry': (payload.get('industry', existing['industry']) or '').strip(),
            'asset_type': (payload.get('asset_type', existing['asset_type']) or '').strip(),
            'source': (payload.get('source', existing['source']) or '').strip(),
            'note': (payload.get('note', existing['note']) or '').strip(),
            'current_price': self._to_float(payload.get('current_price', existing['current_price'])),
            'pe': self._to_float(payload.get('pe', existing['pe'])),
            'current_stage': (payload.get('current_stage', existing['current_stage']) or '').strip(),
            'current_price_zone': (payload.get('current_price_zone', existing['current_price_zone']) or '').strip(),
            'suggested_action': (payload.get('suggested_action', existing['suggested_action']) or '').strip(),
            'last_conclusion_summary': (payload.get('last_conclusion_summary', existing['last_conclusion_summary']) or '').strip(),
            'last_analysis_at': (payload.get('last_analysis_at', existing['last_analysis_at']) or '').strip(),
            'updated_at': datetime.now().isoformat(timespec='seconds'),
        }

        with self.connection_factory.connect() as connection:
            connection.execute(
                '''
                UPDATE watch_stocks
                SET stock_code = ?, stock_name = ?, market = ?, industry = ?, asset_type = ?,
                    source = ?, note = ?, current_price = ?, pe = ?, current_stage = ?,
                    current_price_zone = ?, suggested_action = ?, last_conclusion_summary = ?,
                    last_analysis_at = ?, updated_at = ?
                WHERE id = ?
                ''',
                [
                    updated['stock_code'],
                    updated['stock_name'],
                    updated['market'],
                    updated['industry'],
                    updated['asset_type'],
                    updated['source'],
                    updated['note'],
                    updated['current_price'],
                    updated['pe'],
                    updated['current_stage'],
                    updated['current_price_zone'],
                    updated['suggested_action'],
                    updated['last_conclusion_summary'],
                    updated['last_analysis_at'],
                    updated['updated_at'],
                    watch_stock_id,
                ],
            )
            connection.commit()
        return self.get_by_id(watch_stock_id)

    def archive(self, watch_stock_id: str) -> dict[str, Any] | None:
        existing = self.get_by_id(watch_stock_id)
        if not existing:
            return None
        updated_at = datetime.now().isoformat(timespec='seconds')
        with self.connection_factory.connect() as connection:
            connection.execute(
                'UPDATE watch_stocks SET status = ?, updated_at = ? WHERE id = ?',
                [ARCHIVED_STATUS, updated_at, watch_stock_id],
            )
            connection.commit()
        return self.get_by_id(watch_stock_id)

    def get_summary_counts(self) -> dict[str, int]:
        with self.connection_factory.connect() as connection:
            total = int(connection.execute(
                'SELECT COUNT(*) AS value FROM watch_stocks WHERE status != ?',
                [ARCHIVED_STATUS],
            ).fetchone()['value'])
            waiting_decision = int(connection.execute(
                '''
                SELECT COUNT(*) AS value
                FROM watch_stocks
                WHERE status != ? AND (suggested_action != '' OR current_stage != '')
                ''',
                [ARCHIVED_STATUS],
            ).fetchone()['value'])
            completed_analysis = int(connection.execute(
                '''
                SELECT COUNT(*) AS value
                FROM watch_stocks
                WHERE status != ? AND last_analysis_at != ''
                ''',
                [ARCHIVED_STATUS],
            ).fetchone()['value'])
            planned = int(connection.execute(
                '''
                SELECT COUNT(*) AS value
                FROM watch_stocks
                WHERE status != ? AND suggested_action LIKE ?
                ''',
                [ARCHIVED_STATUS, '%计划%'],
            ).fetchone()['value'])
        return {
            'watch_count': total,
            'decision_ready_count': waiting_decision,
            'analysis_completed_count': completed_analysis,
            'planned_count': planned,
        }

    def _build_where(self, filters: dict[str, Any], default_status: str | None = None) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []

        status = (filters.get('status') or '').strip()
        if status:
            clauses.append('status = ?')
            parameters.append(status)
        elif default_status:
            clauses.append('status = ?')
            parameters.append(default_status)

        keyword = (filters.get('keyword') or '').strip()
        if keyword:
            clauses.append('(stock_code LIKE ? OR stock_name LIKE ? OR industry LIKE ? OR note LIKE ?)')
            keyword_like = f'%{keyword}%'
            parameters.extend([keyword_like, keyword_like, keyword_like, keyword_like])

        for field, param_name in (
            ('market', 'market'),
            ('asset_type', 'asset_type'),
            ('current_stage', 'stage'),
            ('current_price_zone', 'price_zone'),
        ):
            value = (filters.get(param_name) or '').strip()
            if value:
                clauses.append(f'{field} = ?')
                parameters.append(value)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''
        return where_sql, parameters

    def _row_to_dict(self, row: Any) -> dict[str, Any]:
        return dict(row)

    def _new_id(self) -> str:
        return f'WS-{uuid4().hex[:12].upper()}'

    def _to_float(self, value: Any) -> float | None:
        if value in (None, ''):
            return None
        return float(value)
