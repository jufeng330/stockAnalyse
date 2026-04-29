from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from stock_analyse.infrastructure.persistence.trading_decision.schema_manager import TradingDecisionSchemaManager
from stock_analyse.infrastructure.persistence.trading_decision.sqlite_connection import TradingDecisionSQLiteConnection


class HoldingStockTradeRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.connection_factory = TradingDecisionSQLiteConnection(db_path)
        self.schema_manager = TradingDecisionSchemaManager(self.connection_factory)
        self.schema_manager.ensure_schema()

    def create(self, payload: dict[str, Any], *, connection=None) -> dict[str, Any]:
        now = datetime.now().isoformat(timespec='seconds')
        record = {
            'id': payload.get('id') or self._new_id(),
            'holding_stock_id': (payload.get('holding_stock_id') or '').strip(),
            'trade_type': (payload.get('trade_type') or '').strip(),
            'trade_date': (payload.get('trade_date') or '').strip(),
            'quantity': self._to_float(payload.get('quantity')) or 0.0,
            'price': self._to_float(payload.get('price')) or 0.0,
            'amount': self._to_float(payload.get('amount')) or 0.0,
            'note': (payload.get('note') or '').strip(),
            'source_watch_stock_id': (payload.get('source_watch_stock_id') or '').strip(),
            'created_at': now,
            'updated_at': now,
        }
        close_connection = connection is None
        active_connection = connection or self.connection_factory.connect()
        try:
            active_connection.execute(
                '''
                INSERT INTO holding_stock_trades (
                    id, holding_stock_id, trade_type, trade_date, quantity, price, amount,
                    note, source_watch_stock_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    record['id'],
                    record['holding_stock_id'],
                    record['trade_type'],
                    record['trade_date'],
                    record['quantity'],
                    record['price'],
                    record['amount'],
                    record['note'],
                    record['source_watch_stock_id'],
                    record['created_at'],
                    record['updated_at'],
                ],
            )
            if close_connection:
                active_connection.commit()
        finally:
            if close_connection:
                active_connection.close()
        return self.get_by_id(record['id']) or record

    def get_by_id(self, trade_id: str) -> dict[str, Any] | None:
        with self.connection_factory.connect() as connection:
            row = connection.execute(
                'SELECT * FROM holding_stock_trades WHERE id = ?',
                [trade_id],
            ).fetchone()
        return dict(row) if row else None

    def list_by_holding_stock(self, holding_stock_id: str, limit: int = 20) -> list[dict[str, Any]]:
        with self.connection_factory.connect() as connection:
            rows = connection.execute(
                '''
                SELECT *
                FROM holding_stock_trades
                WHERE holding_stock_id = ?
                ORDER BY trade_date DESC, created_at DESC, id DESC
                LIMIT ?
                ''',
                [holding_stock_id, limit],
            ).fetchall()
        return [dict(row) for row in rows]

    def _new_id(self) -> str:
        return f'HST-{uuid4().hex[:12].upper()}'

    def _to_float(self, value: Any) -> float | None:
        if value in (None, ''):
            return None
        return float(value)
