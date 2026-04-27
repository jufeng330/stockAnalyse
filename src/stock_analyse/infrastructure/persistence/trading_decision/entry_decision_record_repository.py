from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from stock_analyse.infrastructure.persistence.trading_decision.schema_manager import TradingDecisionSchemaManager
from stock_analyse.infrastructure.persistence.trading_decision.sqlite_connection import TradingDecisionSQLiteConnection


class EntryDecisionRecordRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.connection_factory = TradingDecisionSQLiteConnection(db_path)
        self.schema_manager = TradingDecisionSchemaManager(self.connection_factory)
        self.schema_manager.ensure_schema()

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now().isoformat(timespec='seconds')
        record = {
            'id': payload.get('id') or self._new_id(),
            'watch_stock_id': (payload.get('watch_stock_id') or '').strip(),
            'session_id': (payload.get('session_id') or '').strip(),
            'stock_code': (payload.get('stock_code') or '').strip(),
            'stock_name': (payload.get('stock_name') or '').strip(),
            'market': (payload.get('market') or '').strip(),
            'trade_date': (payload.get('trade_date') or '').strip(),
            'current_stage': (payload.get('current_stage') or '').strip(),
            'current_price_zone': (payload.get('current_price_zone') or '').strip(),
            'suggested_action': (payload.get('suggested_action') or '').strip(),
            'suggested_entry_leg': (payload.get('suggested_entry_leg') or '').strip(),
            'conclusion_summary': (payload.get('conclusion_summary') or '').strip(),
            'decision_card_json': self._json_dumps(payload.get('decision_card_json') or {}),
            'full_result_json': self._json_dumps(payload.get('full_result_json') or {}),
            'created_at': now,
            'updated_at': now,
        }
        with self.connection_factory.connect() as connection:
            connection.execute(
                '''
                INSERT INTO entry_decision_records (
                    id, watch_stock_id, session_id, stock_code, stock_name, market,
                    trade_date, current_stage, current_price_zone, suggested_action,
                    suggested_entry_leg, conclusion_summary, decision_card_json,
                    full_result_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    record['id'],
                    record['watch_stock_id'],
                    record['session_id'],
                    record['stock_code'],
                    record['stock_name'],
                    record['market'],
                    record['trade_date'],
                    record['current_stage'],
                    record['current_price_zone'],
                    record['suggested_action'],
                    record['suggested_entry_leg'],
                    record['conclusion_summary'],
                    record['decision_card_json'],
                    record['full_result_json'],
                    record['created_at'],
                    record['updated_at'],
                ],
            )
            connection.commit()
        return self.get_by_id(record['id']) or record

    def get_by_id(self, record_id: str) -> dict[str, Any] | None:
        with self.connection_factory.connect() as connection:
            row = connection.execute(
                'SELECT * FROM entry_decision_records WHERE id = ?',
                [record_id],
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def list_by_watch_stock(self, watch_stock_id: str, limit: int = 10) -> list[dict[str, Any]]:
        with self.connection_factory.connect() as connection:
            rows = connection.execute(
                '''
                SELECT *
                FROM entry_decision_records
                WHERE watch_stock_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                ''',
                [watch_stock_id, limit],
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def _row_to_dict(self, row: Any) -> dict[str, Any]:
        data = dict(row)
        return {
            **data,
            'decision_card_json': self._json_loads(data.get('decision_card_json')),
            'full_result_json': self._json_loads(data.get('full_result_json')),
        }

    def _new_id(self) -> str:
        return f'EDR-{uuid4().hex[:12].upper()}'

    def _json_dumps(self, value: Any) -> str:
        return json.dumps(value, ensure_ascii=False)

    def _json_loads(self, value: Any) -> Any:
        if value in (None, ''):
            return {}
        if isinstance(value, dict):
            return value
        try:
            loaded = json.loads(value)
        except Exception:
            return {}
        return loaded if isinstance(loaded, dict) else {}
