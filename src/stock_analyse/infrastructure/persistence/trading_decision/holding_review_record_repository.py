from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from stock_analyse.infrastructure.persistence.trading_decision.schema_manager import TradingDecisionSchemaManager
from stock_analyse.infrastructure.persistence.trading_decision.sqlite_connection import TradingDecisionSQLiteConnection


class HoldingReviewRecordRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.connection_factory = TradingDecisionSQLiteConnection(db_path)
        self.schema_manager = TradingDecisionSchemaManager(self.connection_factory)
        self.schema_manager.ensure_schema()

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now().isoformat(timespec='seconds')
        record = {
            'id': payload.get('id') or self._new_id(),
            'holding_stock_id': (payload.get('holding_stock_id') or '').strip(),
            'watch_stock_id': (payload.get('watch_stock_id') or '').strip(),
            'stock_code': (payload.get('stock_code') or '').strip(),
            'stock_name': (payload.get('stock_name') or '').strip(),
            'market': (payload.get('market') or '').strip(),
            'trade_date': (payload.get('trade_date') or '').strip(),
            'review_type': (payload.get('review_type') or '').strip(),
            'period_key': (payload.get('period_key') or '').strip(),
            'analysis_depth': (payload.get('analysis_depth') or '').strip(),
            'performance_summary': (payload.get('performance_summary') or '').strip(),
            'execution_summary': (payload.get('execution_summary') or '').strip(),
            'risk_summary': (payload.get('risk_summary') or '').strip(),
            'discipline_summary': (payload.get('discipline_summary') or '').strip(),
            'next_action_summary': (payload.get('next_action_summary') or '').strip(),
            'conclusion_tag': (payload.get('conclusion_tag') or '').strip(),
            'tabs_json': self._json_dumps(payload.get('tabs_json') or []),
            'evidence_json': self._json_dumps(payload.get('evidence_json') or []),
            'context_snapshot_json': self._json_dumps(payload.get('context_snapshot_json') or {}),
            'raw_result_json': self._json_dumps(payload.get('raw_result_json') or {}),
            'created_at': now,
            'updated_at': now,
        }
        with self.connection_factory.connect() as connection:
            connection.execute(
                '''
                INSERT INTO holding_review_records (
                    id, holding_stock_id, watch_stock_id, stock_code, stock_name, market,
                    trade_date, review_type, period_key, analysis_depth,
                    performance_summary, execution_summary, risk_summary, discipline_summary,
                    next_action_summary, conclusion_tag, tabs_json, evidence_json,
                    context_snapshot_json, raw_result_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    record['id'],
                    record['holding_stock_id'],
                    record['watch_stock_id'],
                    record['stock_code'],
                    record['stock_name'],
                    record['market'],
                    record['trade_date'],
                    record['review_type'],
                    record['period_key'],
                    record['analysis_depth'],
                    record['performance_summary'],
                    record['execution_summary'],
                    record['risk_summary'],
                    record['discipline_summary'],
                    record['next_action_summary'],
                    record['conclusion_tag'],
                    record['tabs_json'],
                    record['evidence_json'],
                    record['context_snapshot_json'],
                    record['raw_result_json'],
                    record['created_at'],
                    record['updated_at'],
                ],
            )
            connection.commit()
        return self.get_by_id(record['id']) or record

    def get_by_id(self, record_id: str) -> dict[str, Any] | None:
        with self.connection_factory.connect() as connection:
            row = connection.execute(
                'SELECT * FROM holding_review_records WHERE id = ?',
                [record_id],
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def list_by_holding_stock(self, holding_stock_id: str, limit: int = 10) -> list[dict[str, Any]]:
        with self.connection_factory.connect() as connection:
            rows = connection.execute(
                '''
                SELECT *
                FROM holding_review_records
                WHERE holding_stock_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                ''',
                [holding_stock_id, limit],
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def _row_to_dict(self, row: Any) -> dict[str, Any]:
        data = dict(row)
        return {
            **data,
            'tabs_json': self._json_loads_list(data.get('tabs_json')),
            'evidence_json': self._json_loads_list(data.get('evidence_json')),
            'context_snapshot_json': self._json_loads_dict(data.get('context_snapshot_json')),
            'raw_result_json': self._json_loads_dict(data.get('raw_result_json')),
        }

    def _new_id(self) -> str:
        return f'HRR-{uuid4().hex[:12].upper()}'

    def _json_dumps(self, value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, default=str)

    def _json_loads_dict(self, value: Any) -> dict[str, Any]:
        if value in (None, ''):
            return {}
        if isinstance(value, dict):
            return value
        try:
            loaded = json.loads(value)
        except Exception:
            return {}
        return loaded if isinstance(loaded, dict) else {}

    def _json_loads_list(self, value: Any) -> list[Any]:
        if value in (None, ''):
            return []
        if isinstance(value, list):
            return value
        try:
            loaded = json.loads(value)
        except Exception:
            return []
        return loaded if isinstance(loaded, list) else []
