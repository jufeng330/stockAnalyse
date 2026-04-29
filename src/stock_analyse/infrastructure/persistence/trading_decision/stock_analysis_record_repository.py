from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from stock_analyse.infrastructure.persistence.trading_decision.schema_manager import TradingDecisionSchemaManager
from stock_analyse.infrastructure.persistence.trading_decision.sqlite_connection import TradingDecisionSQLiteConnection


class StockAnalysisRecordRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.connection_factory = TradingDecisionSQLiteConnection(db_path)
        self.schema_manager = TradingDecisionSchemaManager(self.connection_factory)
        self.schema_manager.ensure_schema()

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now().isoformat(timespec='seconds')
        record = {
            'id': payload.get('id') or self._new_id(),
            'watch_stock_id': (payload.get('watch_stock_id') or '').strip(),
            'holding_stock_id': (payload.get('holding_stock_id') or '').strip(),
            'analysis_scene': (payload.get('analysis_scene') or '').strip(),
            'stock_code': (payload.get('stock_code') or '').strip(),
            'stock_name': (payload.get('stock_name') or '').strip(),
            'market': (payload.get('market') or '').strip(),
            'trade_date': (payload.get('trade_date') or '').strip(),
            'analysis_mode': (payload.get('analysis_mode') or '').strip(),
            'stance': (payload.get('stance') or '').strip(),
            'time_horizon': (payload.get('time_horizon') or '').strip(),
            'conclusion_summary': (payload.get('conclusion_summary') or '').strip(),
            'risk_level': (payload.get('risk_level') or '').strip(),
            'scores_json': self._json_dumps(payload.get('scores_json') or {}),
            'signals_json': self._json_dumps(payload.get('signals_json') or []),
            'risks_json': self._json_dumps(payload.get('risks_json') or []),
            'evidence_json': self._json_dumps(payload.get('evidence_json') or []),
            'raw_result_json': self._json_dumps(payload.get('raw_result_json') or {}),
            'created_at': now,
            'updated_at': now,
        }
        with self.connection_factory.connect() as connection:
            connection.execute(
                '''
                INSERT INTO stock_analysis_records (
                    id, watch_stock_id, holding_stock_id, analysis_scene, stock_code, stock_name, market,
                    trade_date, analysis_mode, stance, time_horizon,
                    conclusion_summary, risk_level, scores_json, signals_json,
                    risks_json, evidence_json, raw_result_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    record['id'],
                    record['watch_stock_id'],
                    record['holding_stock_id'],
                    record['analysis_scene'],
                    record['stock_code'],
                    record['stock_name'],
                    record['market'],
                    record['trade_date'],
                    record['analysis_mode'],
                    record['stance'],
                    record['time_horizon'],
                    record['conclusion_summary'],
                    record['risk_level'],
                    record['scores_json'],
                    record['signals_json'],
                    record['risks_json'],
                    record['evidence_json'],
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
                'SELECT * FROM stock_analysis_records WHERE id = ?',
                [record_id],
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def list_by_watch_stock(self, watch_stock_id: str, limit: int = 10) -> list[dict[str, Any]]:
        with self.connection_factory.connect() as connection:
            rows = connection.execute(
                '''
                SELECT *
                FROM stock_analysis_records
                WHERE watch_stock_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                ''',
                [watch_stock_id, limit],
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def list_by_holding_stock(self, holding_stock_id: str, limit: int = 10) -> list[dict[str, Any]]:
        with self.connection_factory.connect() as connection:
            rows = connection.execute(
                '''
                SELECT *
                FROM stock_analysis_records
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
            'scores_json': self._json_loads(data.get('scores_json'), default={}),
            'signals_json': self._json_loads(data.get('signals_json'), default=[]),
            'risks_json': self._json_loads(data.get('risks_json'), default=[]),
            'evidence_json': self._json_loads(data.get('evidence_json'), default=[]),
            'raw_result_json': self._json_loads(data.get('raw_result_json'), default={}),
        }

    def _new_id(self) -> str:
        return f'SAR-{uuid4().hex[:12].upper()}'

    def _json_dumps(self, value: Any) -> str:
        return json.dumps(value, ensure_ascii=False)

    def _json_loads(self, value: Any, *, default: Any) -> Any:
        if value in (None, ''):
            return default
        if isinstance(value, type(default)):
            return value
        try:
            loaded = json.loads(value)
        except Exception:
            return default
        return loaded if isinstance(loaded, type(default)) else default
