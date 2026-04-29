from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from stock_analyse.infrastructure.persistence.trading_decision.schema_manager import TradingDecisionSchemaManager
from stock_analyse.infrastructure.persistence.trading_decision.sqlite_connection import TradingDecisionSQLiteConnection


class TradePlanAnalysisRecordRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.connection_factory = TradingDecisionSQLiteConnection(db_path)
        self.schema_manager = TradingDecisionSchemaManager(self.connection_factory)
        self.schema_manager.ensure_schema()

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now().isoformat(timespec='seconds')
        record = {
            'id': payload.get('id') or self._new_id(),
            'watch_stock_id': (payload.get('watch_stock_id') or '').strip(),
            'stock_code': (payload.get('stock_code') or '').strip(),
            'stock_name': (payload.get('stock_name') or '').strip(),
            'market': (payload.get('market') or '').strip(),
            'trade_date': (payload.get('trade_date') or '').strip(),
            'plan_type': (payload.get('plan_type') or '').strip(),
            'risk_preference': (payload.get('risk_preference') or '').strip(),
            'risk_level': (payload.get('risk_level') or '').strip(),
            'suggested_action': (payload.get('suggested_action') or '').strip(),
            'conclusion_summary': (payload.get('conclusion_summary') or '').strip(),
            'max_target_position': (payload.get('max_target_position') or '').strip(),
            'position_limit': (payload.get('position_limit') or '').strip(),
            'entry_plan_json': self._json_dumps(payload.get('entry_plan_json') or {}),
            'add_position_rules': (payload.get('add_position_rules') or '').strip(),
            'reduce_position_rules': (payload.get('reduce_position_rules') or '').strip(),
            'sell_rules': (payload.get('sell_rules') or '').strip(),
            'risk_notes': (payload.get('risk_notes') or '').strip(),
            'raw_result_json': self._json_dumps(payload.get('raw_result_json') or {}),
            'created_at': now,
            'updated_at': now,
        }
        with self.connection_factory.connect() as connection:
            connection.execute(
                '''
                INSERT INTO trade_plan_analysis_records (
                    id, watch_stock_id, stock_code, stock_name, market, trade_date,
                    plan_type, risk_preference, risk_level, suggested_action,
                    conclusion_summary, max_target_position, position_limit,
                    entry_plan_json, add_position_rules, reduce_position_rules,
                    sell_rules, risk_notes, raw_result_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    record['id'],
                    record['watch_stock_id'],
                    record['stock_code'],
                    record['stock_name'],
                    record['market'],
                    record['trade_date'],
                    record['plan_type'],
                    record['risk_preference'],
                    record['risk_level'],
                    record['suggested_action'],
                    record['conclusion_summary'],
                    record['max_target_position'],
                    record['position_limit'],
                    record['entry_plan_json'],
                    record['add_position_rules'],
                    record['reduce_position_rules'],
                    record['sell_rules'],
                    record['risk_notes'],
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
                'SELECT * FROM trade_plan_analysis_records WHERE id = ?',
                [record_id],
            ).fetchone()
        return self._format_row(dict(row)) if row else None

    def list_by_watch_stock(self, watch_stock_id: str, limit: int = 10) -> list[dict[str, Any]]:
        with self.connection_factory.connect() as connection:
            rows = connection.execute(
                '''
                SELECT *
                FROM trade_plan_analysis_records
                WHERE watch_stock_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                ''',
                [watch_stock_id, limit],
            ).fetchall()
        return [self._format_row(dict(row)) for row in rows]

    def _format_row(self, row: dict[str, Any]) -> dict[str, Any]:
        entry_plan_json = row.get('entry_plan_json') or {}
        raw_result_json = row.get('raw_result_json') or {}
        if isinstance(entry_plan_json, str):
            try:
                entry_plan_json = json.loads(entry_plan_json)
            except Exception:
                entry_plan_json = {}
        if isinstance(raw_result_json, str):
            try:
                raw_result_json = json.loads(raw_result_json)
            except Exception:
                raw_result_json = {}
        entry_plan_json = entry_plan_json if isinstance(entry_plan_json, dict) else {}
        raw_result_json = raw_result_json if isinstance(raw_result_json, dict) else {}
        data = raw_result_json.get('data') if isinstance(raw_result_json.get('data'), dict) else {}
        decision = data.get('decision') if isinstance(data.get('decision'), dict) else {}
        trade_plan_markdown = str(entry_plan_json.get('trade_plan_markdown') or data.get('trade_plan_markdown') or '').strip()
        decision_action = str(decision.get('action') or '').strip()
        risks_json = decision.get('risks') if isinstance(decision.get('risks'), list) else []
        position_suggestion_json = decision.get('position_suggestion') if isinstance(decision.get('position_suggestion'), dict) else {}
        return {
            **row,
            'entry_plan_json': entry_plan_json,
            'raw_result_json': raw_result_json,
            'decision_action': decision_action,
            'decision_logic': str(decision.get('logic') or '').strip(),
            'decision_action_label': self._map_action_label(decision_action),
            'time_horizon': str(decision.get('time_horizon') or '').strip(),
            'trade_plan_markdown': trade_plan_markdown,
            'risks_json': risks_json,
            'position_suggestion_json': position_suggestion_json,
        }

    def _new_id(self) -> str:
        return f'TPA-{uuid4().hex[:12].upper()}'

    def _json_dumps(self, value: Any) -> str:
        return json.dumps(value, ensure_ascii=False)

    def _map_action_label(self, action: str | None) -> str:
        normalized = (action or '').strip().lower()
        mapping = {'buy': '适合买入', 'hold': '继续观察', 'watch': '继续观察', 'sell': '不适合买入'}
        return mapping.get(normalized, action or '')
