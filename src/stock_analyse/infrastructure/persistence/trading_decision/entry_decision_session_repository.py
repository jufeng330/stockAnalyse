from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from stock_analyse.infrastructure.persistence.trading_decision.schema_manager import TradingDecisionSchemaManager
from stock_analyse.infrastructure.persistence.trading_decision.sqlite_connection import TradingDecisionSQLiteConnection


class EntryDecisionSessionRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.connection_factory = TradingDecisionSQLiteConnection(db_path)
        self.schema_manager = TradingDecisionSchemaManager(self.connection_factory)
        self.schema_manager.ensure_schema()

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now().isoformat(timespec='seconds')
        session = {
            'id': payload.get('id') or self._new_id(),
            'watch_stock_id': (payload.get('watch_stock_id') or '').strip(),
            'stock_code': (payload.get('stock_code') or '').strip(),
            'trade_date': (payload.get('trade_date') or '').strip(),
            'status': (payload.get('status') or 'running').strip() or 'running',
            'current_role': (payload.get('current_role') or 'macro_analysis').strip() or 'macro_analysis',
            'request_json': self._json_dumps(payload.get('request_json') or {}),
            'manual_inputs_json': self._json_dumps(payload.get('manual_inputs_json') or {}),
            'auto_context_json': self._json_dumps(payload.get('auto_context_json') or {}),
            'role_outputs_json': self._json_dumps(payload.get('role_outputs_json') or {}),
            'missing_fields_json': self._json_dumps(payload.get('missing_fields_json') or []),
            'pause_prompt': (payload.get('pause_prompt') or '').strip(),
            'final_result_json': self._json_dumps(payload.get('final_result_json') or {}),
            'created_at': now,
            'updated_at': now,
        }
        with self.connection_factory.connect() as connection:
            connection.execute(
                '''
                INSERT INTO entry_decision_sessions (
                    id, watch_stock_id, stock_code, trade_date, status, current_role,
                    request_json, manual_inputs_json, auto_context_json, role_outputs_json,
                    missing_fields_json, pause_prompt, final_result_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    session['id'],
                    session['watch_stock_id'],
                    session['stock_code'],
                    session['trade_date'],
                    session['status'],
                    session['current_role'],
                    session['request_json'],
                    session['manual_inputs_json'],
                    session['auto_context_json'],
                    session['role_outputs_json'],
                    session['missing_fields_json'],
                    session['pause_prompt'],
                    session['final_result_json'],
                    session['created_at'],
                    session['updated_at'],
                ],
            )
            connection.commit()
        return self.get_by_id(session['id']) or session

    def update(self, session_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        existing = self.get_by_id(session_id)
        if not existing:
            return None
        updated = {
            **existing,
            'stock_code': (payload.get('stock_code', existing['stock_code']) or '').strip(),
            'trade_date': (payload.get('trade_date', existing['trade_date']) or '').strip(),
            'status': (payload.get('status', existing['status']) or '').strip() or existing['status'],
            'current_role': (payload.get('current_role', existing['current_role']) or '').strip() or existing['current_role'],
            'request_json': self._json_dumps(payload.get('request_json', existing.get('request_json') or {})),
            'manual_inputs_json': self._json_dumps(payload.get('manual_inputs_json', existing.get('manual_inputs_json') or {})),
            'auto_context_json': self._json_dumps(payload.get('auto_context_json', existing.get('auto_context_json') or {})),
            'role_outputs_json': self._json_dumps(payload.get('role_outputs_json', existing.get('role_outputs_json') or {})),
            'missing_fields_json': self._json_dumps(payload.get('missing_fields_json', existing.get('missing_fields_json') or [])),
            'pause_prompt': (payload.get('pause_prompt', existing.get('pause_prompt')) or '').strip(),
            'final_result_json': self._json_dumps(payload.get('final_result_json', existing.get('final_result_json') or {})),
            'updated_at': datetime.now().isoformat(timespec='seconds'),
        }
        with self.connection_factory.connect() as connection:
            connection.execute(
                '''
                UPDATE entry_decision_sessions
                SET stock_code = ?, trade_date = ?, status = ?, current_role = ?,
                    request_json = ?, manual_inputs_json = ?, auto_context_json = ?,
                    role_outputs_json = ?, missing_fields_json = ?, pause_prompt = ?,
                    final_result_json = ?, updated_at = ?
                WHERE id = ?
                ''',
                [
                    updated['stock_code'],
                    updated['trade_date'],
                    updated['status'],
                    updated['current_role'],
                    updated['request_json'],
                    updated['manual_inputs_json'],
                    updated['auto_context_json'],
                    updated['role_outputs_json'],
                    updated['missing_fields_json'],
                    updated['pause_prompt'],
                    updated['final_result_json'],
                    updated['updated_at'],
                    session_id,
                ],
            )
            connection.commit()
        return self.get_by_id(session_id)

    def get_by_id(self, session_id: str) -> dict[str, Any] | None:
        with self.connection_factory.connect() as connection:
            row = connection.execute(
                'SELECT * FROM entry_decision_sessions WHERE id = ?',
                [session_id],
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def find_latest_active_by_watch_stock(self, watch_stock_id: str) -> dict[str, Any] | None:
        with self.connection_factory.connect() as connection:
            row = connection.execute(
                '''
                SELECT *
                FROM entry_decision_sessions
                WHERE watch_stock_id = ? AND status IN ('running', 'paused')
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 1
                ''',
                [watch_stock_id],
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def _row_to_dict(self, row: Any) -> dict[str, Any]:
        data = dict(row)
        return {
            **data,
            'request_json': self._json_loads(data.get('request_json')),
            'manual_inputs_json': self._json_loads(data.get('manual_inputs_json')),
            'auto_context_json': self._json_loads(data.get('auto_context_json')),
            'role_outputs_json': self._json_loads(data.get('role_outputs_json')),
            'missing_fields_json': self._json_loads(data.get('missing_fields_json'), default=[]),
            'final_result_json': self._json_loads(data.get('final_result_json')),
        }

    def _new_id(self) -> str:
        return f'EDS-{uuid4().hex[:12].upper()}'

    def _json_dumps(self, value: Any) -> str:
        return json.dumps(value, ensure_ascii=False)

    def _json_loads(self, value: Any, default: Any | None = None) -> Any:
        if value in (None, ''):
            return {} if default is None else default
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except Exception:
            return {} if default is None else default
