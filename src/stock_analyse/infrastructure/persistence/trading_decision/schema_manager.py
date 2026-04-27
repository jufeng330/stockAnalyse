from __future__ import annotations

from stock_analyse.infrastructure.persistence.trading_decision.sqlite_connection import TradingDecisionSQLiteConnection


class TradingDecisionSchemaManager:
    def __init__(self, connection_factory: TradingDecisionSQLiteConnection) -> None:
        self.connection_factory = connection_factory

    def ensure_schema(self) -> None:
        with self.connection_factory.connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS watch_stocks (
                    id TEXT PRIMARY KEY,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    market TEXT NOT NULL,
                    industry TEXT NOT NULL DEFAULT '',
                    asset_type TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT '',
                    note TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    current_price REAL,
                    pe REAL,
                    current_stage TEXT NOT NULL DEFAULT '',
                    current_price_zone TEXT NOT NULL DEFAULT '',
                    suggested_action TEXT NOT NULL DEFAULT '',
                    last_conclusion_summary TEXT NOT NULL DEFAULT '',
                    last_analysis_at TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_plan_analysis_records (
                    id TEXT PRIMARY KEY,
                    watch_stock_id TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    market TEXT NOT NULL,
                    trade_date TEXT NOT NULL DEFAULT '',
                    plan_type TEXT NOT NULL DEFAULT '',
                    risk_preference TEXT NOT NULL DEFAULT '',
                    risk_level TEXT NOT NULL DEFAULT '',
                    suggested_action TEXT NOT NULL DEFAULT '',
                    conclusion_summary TEXT NOT NULL DEFAULT '',
                    max_target_position TEXT NOT NULL DEFAULT '',
                    position_limit TEXT NOT NULL DEFAULT '',
                    entry_plan_json TEXT NOT NULL DEFAULT '{}',
                    add_position_rules TEXT NOT NULL DEFAULT '',
                    reduce_position_rules TEXT NOT NULL DEFAULT '',
                    sell_rules TEXT NOT NULL DEFAULT '',
                    risk_notes TEXT NOT NULL DEFAULT '',
                    raw_result_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS entry_decision_sessions (
                    id TEXT PRIMARY KEY,
                    watch_stock_id TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'running',
                    current_role TEXT NOT NULL DEFAULT 'macro_analysis',
                    request_json TEXT NOT NULL DEFAULT '{}',
                    manual_inputs_json TEXT NOT NULL DEFAULT '{}',
                    auto_context_json TEXT NOT NULL DEFAULT '{}',
                    role_outputs_json TEXT NOT NULL DEFAULT '{}',
                    missing_fields_json TEXT NOT NULL DEFAULT '[]',
                    pause_prompt TEXT NOT NULL DEFAULT '',
                    final_result_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS entry_decision_records (
                    id TEXT PRIMARY KEY,
                    watch_stock_id TEXT NOT NULL,
                    session_id TEXT NOT NULL DEFAULT '',
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    market TEXT NOT NULL,
                    trade_date TEXT NOT NULL DEFAULT '',
                    current_stage TEXT NOT NULL DEFAULT '',
                    current_price_zone TEXT NOT NULL DEFAULT '',
                    suggested_action TEXT NOT NULL DEFAULT '',
                    suggested_entry_leg TEXT NOT NULL DEFAULT '',
                    conclusion_summary TEXT NOT NULL DEFAULT '',
                    decision_card_json TEXT NOT NULL DEFAULT '{}',
                    full_result_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_watch_stocks_status_updated ON watch_stocks(status, updated_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_watch_stocks_market_status ON watch_stocks(market, status)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_watch_stocks_stage_zone ON watch_stocks(current_stage, current_price_zone)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_trade_plan_watch_created ON trade_plan_analysis_records(watch_stock_id, created_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_trade_plan_stock_created ON trade_plan_analysis_records(stock_code, created_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_entry_session_watch_updated ON entry_decision_sessions(watch_stock_id, updated_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_entry_session_status_updated ON entry_decision_sessions(status, updated_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_entry_record_watch_created ON entry_decision_records(watch_stock_id, created_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_entry_record_stock_created ON entry_decision_records(stock_code, created_at DESC)'
            )
            connection.commit()
