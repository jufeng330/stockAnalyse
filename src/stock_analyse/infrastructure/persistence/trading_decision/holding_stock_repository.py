from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from stock_analyse.infrastructure.persistence.trading_decision.holding_stock_lot_repository import HoldingStockLotRepository
from stock_analyse.infrastructure.persistence.trading_decision.holding_stock_trade_repository import HoldingStockTradeRepository
from stock_analyse.infrastructure.persistence.trading_decision.schema_manager import TradingDecisionSchemaManager
from stock_analyse.infrastructure.persistence.trading_decision.sqlite_connection import TradingDecisionSQLiteConnection


DEFAULT_STATUS = 'active'


@dataclass(frozen=True)
class HoldingStockListResult:
    items: list[dict[str, Any]]
    summary: dict[str, int]
    pagination: dict[str, int]


class HoldingStockRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.connection_factory = TradingDecisionSQLiteConnection(db_path)
        self.schema_manager = TradingDecisionSchemaManager(self.connection_factory)
        self.schema_manager.ensure_schema()
        self.lot_repository = HoldingStockLotRepository(db_path)
        self.trade_repository = HoldingStockTradeRepository(db_path)

    def list(self, filters: dict[str, Any]) -> HoldingStockListResult:
        page = max(int(filters.get('page') or 1), 1)
        page_size = max(min(int(filters.get('page_size') or 20), 100), 1)
        where_sql, parameters = self._build_where(filters, default_status=DEFAULT_STATUS)
        count_sql = f'SELECT COUNT(*) AS total FROM holding_stocks {where_sql}'
        offset = (page - 1) * page_size

        with self.connection_factory.connect() as connection:
            total = int(connection.execute(count_sql, parameters).fetchone()['total'])
            rows = connection.execute(
                f'''
                SELECT *
                FROM holding_stocks
                {where_sql}
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ? OFFSET ?
                ''',
                [*parameters, page_size, offset],
            ).fetchall()

        items = [self._row_to_dict(row) for row in rows]
        return HoldingStockListResult(
            items=items,
            summary=self.get_summary_counts(),
            pagination={
                'page': page,
                'page_size': page_size,
                'total': total,
            },
        )

    def create_with_buy(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now().isoformat(timespec='seconds')
        quantity = self._to_float(payload.get('quantity')) or 0.0
        price = self._to_float(payload.get('price')) or 0.0
        amount = self._normalize_amount(quantity=quantity, price=price, amount=payload.get('amount'))
        trade_date = (payload.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        holding_stock = {
            'id': payload.get('id') or self._new_id(),
            'linked_watch_stock_id': (payload.get('linked_watch_stock_id') or '').strip(),
            'stock_code': (payload.get('stock_code') or '').strip(),
            'stock_name': (payload.get('stock_name') or '').strip(),
            'market': (payload.get('market') or '').strip(),
            'industry': (payload.get('industry') or '').strip(),
            'asset_type': (payload.get('asset_type') or '').strip(),
            'status': (payload.get('status') or DEFAULT_STATUS).strip() or DEFAULT_STATUS,
            'risk_status': (payload.get('risk_status') or '').strip(),
            'suggested_action': (payload.get('suggested_action') or '').strip(),
            'note': (payload.get('note') or '').strip(),
            'current_price': self._to_float(payload.get('current_price')),
            'average_cost': round(amount / quantity, 4) if quantity else 0.0,
            'quantity': quantity,
            'market_value': round((self._to_float(payload.get('current_price')) or price or 0.0) * quantity, 4),
            'total_cost': amount,
            'total_buy_amount': amount,
            'total_sell_amount': 0.0,
            'unrealized_pnl': round(((self._to_float(payload.get('current_price')) or price or 0.0) * quantity) - amount, 4),
            'unrealized_pnl_pct': round((((self._to_float(payload.get('current_price')) or price or 0.0) * quantity) - amount) / amount * 100, 4) if amount else 0.0,
            'latest_buy_at': trade_date,
            'last_review_at': (payload.get('last_review_at') or '').strip(),
            'created_at': now,
            'updated_at': now,
        }
        trade_payload = {
            'holding_stock_id': holding_stock['id'],
            'trade_type': 'buy',
            'trade_date': trade_date,
            'quantity': quantity,
            'price': price,
            'amount': amount,
            'note': holding_stock['note'],
            'source_watch_stock_id': holding_stock['linked_watch_stock_id'],
        }
        with self.connection_factory.connect() as connection:
            connection.execute(
                '''
                INSERT INTO holding_stocks (
                    id, linked_watch_stock_id, stock_code, stock_name, market, industry, asset_type,
                    status, risk_status, suggested_action, note, current_price, average_cost,
                    quantity, market_value, total_cost, total_buy_amount, total_sell_amount,
                    unrealized_pnl, unrealized_pnl_pct, latest_buy_at, last_review_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    holding_stock['id'],
                    holding_stock['linked_watch_stock_id'],
                    holding_stock['stock_code'],
                    holding_stock['stock_name'],
                    holding_stock['market'],
                    holding_stock['industry'],
                    holding_stock['asset_type'],
                    holding_stock['status'],
                    holding_stock['risk_status'],
                    holding_stock['suggested_action'],
                    holding_stock['note'],
                    holding_stock['current_price'],
                    holding_stock['average_cost'],
                    holding_stock['quantity'],
                    holding_stock['market_value'],
                    holding_stock['total_cost'],
                    holding_stock['total_buy_amount'],
                    holding_stock['total_sell_amount'],
                    holding_stock['unrealized_pnl'],
                    holding_stock['unrealized_pnl_pct'],
                    holding_stock['latest_buy_at'],
                    holding_stock['last_review_at'],
                    holding_stock['created_at'],
                    holding_stock['updated_at'],
                ],
            )
            created_trade = self.trade_repository.create(trade_payload, connection=connection)
            self.lot_repository.create(
                {
                    'holding_stock_id': holding_stock['id'],
                    'trade_id': created_trade['id'],
                    'buy_date': trade_date,
                    'quantity': quantity,
                    'price': price,
                    'amount': amount,
                    'note': holding_stock['note'],
                },
                connection=connection,
            )
            connection.commit()
        return self.get_by_id(holding_stock['id']) or holding_stock

    def append_buy(self, holding_stock_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        existing = self.get_by_id(holding_stock_id)
        if not existing:
            return None
        quantity = self._to_float(payload.get('quantity')) or 0.0
        price = self._to_float(payload.get('price')) or 0.0
        amount = self._normalize_amount(quantity=quantity, price=price, amount=payload.get('amount'))
        trade_date = (payload.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        note = (payload.get('note') or '').strip()
        source_watch_stock_id = (payload.get('source_watch_stock_id') or existing.get('linked_watch_stock_id') or '').strip()

        with self.connection_factory.connect() as connection:
            created_trade = self.trade_repository.create(
                {
                    'holding_stock_id': holding_stock_id,
                    'trade_type': 'buy',
                    'trade_date': trade_date,
                    'quantity': quantity,
                    'price': price,
                    'amount': amount,
                    'note': note,
                    'source_watch_stock_id': source_watch_stock_id,
                },
                connection=connection,
            )
            self.lot_repository.create(
                {
                    'holding_stock_id': holding_stock_id,
                    'trade_id': created_trade['id'],
                    'buy_date': trade_date,
                    'quantity': quantity,
                    'price': price,
                    'amount': amount,
                    'note': note,
                },
                connection=connection,
            )
            refreshed = self._refresh_summary_fields(
                holding_stock_id,
                connection=connection,
                current_price=self._to_float(payload.get('current_price')),
                risk_status=(payload.get('risk_status') or '').strip(),
                suggested_action=(payload.get('suggested_action') or '').strip(),
                note=note,
                last_review_at=(payload.get('last_review_at') or '').strip(),
            )
            connection.commit()
        return refreshed

    def get_by_id(self, holding_stock_id: str) -> dict[str, Any] | None:
        with self.connection_factory.connect() as connection:
            row = connection.execute(
                'SELECT * FROM holding_stocks WHERE id = ?',
                [holding_stock_id],
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_by_linked_watch_stock_id(self, watch_stock_id: str) -> dict[str, Any] | None:
        with self.connection_factory.connect() as connection:
            row = connection.execute(
                'SELECT * FROM holding_stocks WHERE linked_watch_stock_id = ? ORDER BY updated_at DESC LIMIT 1',
                [watch_stock_id],
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def update(self, holding_stock_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        existing = self.get_by_id(holding_stock_id)
        if not existing:
            return None
        updated = {
            **existing,
            **payload,
            'updated_at': datetime.now().isoformat(timespec='seconds'),
        }
        with self.connection_factory.connect() as connection:
            connection.execute(
                '''
                UPDATE holding_stocks
                SET linked_watch_stock_id = ?, stock_code = ?, stock_name = ?, market = ?, industry = ?, asset_type = ?,
                    status = ?, risk_status = ?, suggested_action = ?, note = ?, current_price = ?, average_cost = ?,
                    quantity = ?, market_value = ?, total_cost = ?, total_buy_amount = ?, total_sell_amount = ?,
                    unrealized_pnl = ?, unrealized_pnl_pct = ?, latest_buy_at = ?, last_review_at = ?, updated_at = ?
                WHERE id = ?
                ''',
                [
                    updated['linked_watch_stock_id'],
                    updated['stock_code'],
                    updated['stock_name'],
                    updated['market'],
                    updated['industry'],
                    updated['asset_type'],
                    updated['status'],
                    updated['risk_status'],
                    updated['suggested_action'],
                    updated['note'],
                    updated['current_price'],
                    updated['average_cost'],
                    updated['quantity'],
                    updated['market_value'],
                    updated['total_cost'],
                    updated['total_buy_amount'],
                    updated['total_sell_amount'],
                    updated['unrealized_pnl'],
                    updated['unrealized_pnl_pct'],
                    updated['latest_buy_at'],
                    updated['last_review_at'],
                    updated['updated_at'],
                    holding_stock_id,
                ],
            )
            connection.commit()
        return self.get_by_id(holding_stock_id)

    def list_lots(self, holding_stock_id: str, limit: int = 20) -> list[dict[str, Any]]:
        return self.lot_repository.list_by_holding_stock(holding_stock_id, limit=limit)

    def list_trades(self, holding_stock_id: str, limit: int = 20) -> list[dict[str, Any]]:
        return self.trade_repository.list_by_holding_stock(holding_stock_id, limit=limit)

    def get_summary_counts(self) -> dict[str, int]:
        with self.connection_factory.connect() as connection:
            total = int(connection.execute(
                'SELECT COUNT(*) AS value FROM holding_stocks WHERE status = ?',
                [DEFAULT_STATUS],
            ).fetchone()['value'])
            review_ready = int(connection.execute(
                "SELECT COUNT(*) AS value FROM holding_stocks WHERE status = ? AND last_review_at != ''",
                [DEFAULT_STATUS],
            ).fetchone()['value'])
            risky = int(connection.execute(
                "SELECT COUNT(*) AS value FROM holding_stocks WHERE status = ? AND risk_status != ''",
                [DEFAULT_STATUS],
            ).fetchone()['value'])
            suggested = int(connection.execute(
                "SELECT COUNT(*) AS value FROM holding_stocks WHERE status = ? AND suggested_action != ''",
                [DEFAULT_STATUS],
            ).fetchone()['value'])
        return {
            'holding_count': total,
            'reviewed_count': review_ready,
            'risk_flag_count': risky,
            'suggested_action_count': suggested,
        }

    def _refresh_summary_fields(
        self,
        holding_stock_id: str,
        *,
        connection,
        current_price: float | None = None,
        risk_status: str = '',
        suggested_action: str = '',
        note: str = '',
        last_review_at: str = '',
    ) -> dict[str, Any] | None:
        summary_row = connection.execute(
            '''
            SELECT
                COALESCE(SUM(CASE WHEN trade_type = 'buy' THEN quantity ELSE 0 END), 0) AS total_buy_quantity,
                COALESCE(SUM(CASE WHEN trade_type = 'buy' THEN amount ELSE 0 END), 0) AS total_buy_amount,
                COALESCE(SUM(CASE WHEN trade_type IN ('sell', 'reduce') THEN amount ELSE 0 END), 0) AS total_sell_amount,
                COUNT(CASE WHEN trade_type = 'buy' THEN 1 END) AS buy_trade_count,
                COALESCE(MAX(CASE WHEN trade_type = 'buy' THEN trade_date ELSE '' END), '') AS latest_buy_at
            FROM holding_stock_trades
            WHERE holding_stock_id = ?
            ''',
            [holding_stock_id],
        ).fetchone()
        existing = connection.execute(
            'SELECT * FROM holding_stocks WHERE id = ?',
            [holding_stock_id],
        ).fetchone()
        if not existing:
            return None
        total_quantity = float(summary_row['total_buy_quantity'] or 0.0)
        total_buy_amount = float(summary_row['total_buy_amount'] or 0.0)
        total_sell_amount = float(summary_row['total_sell_amount'] or 0.0)
        average_cost = round(total_buy_amount / total_quantity, 4) if total_quantity else 0.0
        effective_current_price = current_price if current_price is not None else self._to_float(existing['current_price'])
        market_value = round((effective_current_price or 0.0) * total_quantity, 4)
        unrealized_pnl = round(market_value - total_buy_amount, 4)
        unrealized_pnl_pct = round(unrealized_pnl / total_buy_amount * 100, 4) if total_buy_amount else 0.0
        updated_at = datetime.now().isoformat(timespec='seconds')
        connection.execute(
            '''
            UPDATE holding_stocks
            SET current_price = ?, average_cost = ?, quantity = ?, market_value = ?, total_cost = ?,
                total_buy_amount = ?, total_sell_amount = ?, unrealized_pnl = ?, unrealized_pnl_pct = ?,
                latest_buy_at = ?, risk_status = ?, suggested_action = ?, note = ?, last_review_at = ?, updated_at = ?
            WHERE id = ?
            ''',
            [
                effective_current_price,
                average_cost,
                total_quantity,
                market_value,
                total_buy_amount,
                total_buy_amount,
                total_sell_amount,
                unrealized_pnl,
                unrealized_pnl_pct,
                summary_row['latest_buy_at'] or '',
                risk_status or existing['risk_status'],
                suggested_action or existing['suggested_action'],
                note or existing['note'],
                last_review_at or existing['last_review_at'],
                updated_at,
                holding_stock_id,
            ],
        )
        refreshed = connection.execute('SELECT * FROM holding_stocks WHERE id = ?', [holding_stock_id]).fetchone()
        return self._row_to_dict(refreshed) if refreshed else None

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
            ('risk_status', 'risk_status'),
            ('suggested_action', 'suggested_action'),
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
        return f'HS-{uuid4().hex[:12].upper()}'

    def _to_float(self, value: Any) -> float | None:
        if value in (None, ''):
            return None
        return float(value)

    def _normalize_amount(self, *, quantity: float, price: float, amount: Any) -> float:
        parsed_amount = self._to_float(amount)
        if parsed_amount is not None:
            return parsed_amount
        return round(quantity * price, 4)
