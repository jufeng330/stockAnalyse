"""交易决策持久化结构管理器。

负责初始化 AI 主链路所依赖的 SQLite 表结构与索引，包括关注、持仓、会话和分析结果记录。
"""

from __future__ import annotations

from stock_analyse.infrastructure.persistence.trading_decision.sqlite_connection import TradingDecisionSQLiteConnection


class TradingDecisionSchemaManager:
    """管理交易决策数据库表结构。

    负责在应用启动或仓储初始化时补齐主链路需要的表、字段和索引。
    """

    def __init__(self, connection_factory: TradingDecisionSQLiteConnection) -> None:
        """保存数据库连接工厂以便后续建表。"""
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
                    linked_holding_stock_id TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._ensure_column(connection, 'watch_stocks', 'linked_holding_stock_id', "TEXT NOT NULL DEFAULT ''")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS holding_stocks (
                    id TEXT PRIMARY KEY,
                    linked_watch_stock_id TEXT NOT NULL DEFAULT '',
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    market TEXT NOT NULL,
                    industry TEXT NOT NULL DEFAULT '',
                    asset_type TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'active',
                    risk_status TEXT NOT NULL DEFAULT '',
                    suggested_action TEXT NOT NULL DEFAULT '',
                    note TEXT NOT NULL DEFAULT '',
                    current_price REAL,
                    average_cost REAL NOT NULL DEFAULT 0,
                    quantity REAL NOT NULL DEFAULT 0,
                    market_value REAL NOT NULL DEFAULT 0,
                    total_cost REAL NOT NULL DEFAULT 0,
                    total_buy_amount REAL NOT NULL DEFAULT 0,
                    total_sell_amount REAL NOT NULL DEFAULT 0,
                    unrealized_pnl REAL NOT NULL DEFAULT 0,
                    unrealized_pnl_pct REAL NOT NULL DEFAULT 0,
                    latest_buy_at TEXT NOT NULL DEFAULT '',
                    last_review_at TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS holding_stock_lots (
                    id TEXT PRIMARY KEY,
                    holding_stock_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL DEFAULT '',
                    buy_date TEXT NOT NULL DEFAULT '',
                    quantity REAL NOT NULL DEFAULT 0,
                    price REAL NOT NULL DEFAULT 0,
                    amount REAL NOT NULL DEFAULT 0,
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS holding_stock_trades (
                    id TEXT PRIMARY KEY,
                    holding_stock_id TEXT NOT NULL,
                    trade_type TEXT NOT NULL,
                    trade_date TEXT NOT NULL DEFAULT '',
                    quantity REAL NOT NULL DEFAULT 0,
                    price REAL NOT NULL DEFAULT 0,
                    amount REAL NOT NULL DEFAULT 0,
                    note TEXT NOT NULL DEFAULT '',
                    source_watch_stock_id TEXT NOT NULL DEFAULT '',
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
                'CREATE INDEX IF NOT EXISTS idx_watch_stocks_linked_holding ON watch_stocks(linked_holding_stock_id)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_holding_stocks_status_updated ON holding_stocks(status, updated_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_holding_stocks_code_updated ON holding_stocks(stock_code, updated_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_holding_stocks_linked_watch ON holding_stocks(linked_watch_stock_id)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_holding_lots_holding_buy_date ON holding_stock_lots(holding_stock_id, buy_date DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_holding_trades_holding_trade_date ON holding_stock_trades(holding_stock_id, trade_date DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_holding_trades_source_watch ON holding_stock_trades(source_watch_stock_id, trade_date DESC)'
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
                """
                CREATE TABLE IF NOT EXISTS stock_analysis_records (
                    id TEXT PRIMARY KEY,
                    watch_stock_id TEXT NOT NULL,
                    holding_stock_id TEXT NOT NULL DEFAULT '',
                    analysis_scene TEXT NOT NULL DEFAULT '',
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    market TEXT NOT NULL,
                    trade_date TEXT NOT NULL DEFAULT '',
                    analysis_mode TEXT NOT NULL DEFAULT '',
                    stance TEXT NOT NULL DEFAULT '',
                    time_horizon TEXT NOT NULL DEFAULT '',
                    conclusion_summary TEXT NOT NULL DEFAULT '',
                    risk_level TEXT NOT NULL DEFAULT '',
                    scores_json TEXT NOT NULL DEFAULT '{}',
                    signals_json TEXT NOT NULL DEFAULT '[]',
                    risks_json TEXT NOT NULL DEFAULT '[]',
                    evidence_json TEXT NOT NULL DEFAULT '[]',
                    raw_result_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._ensure_column(connection, 'stock_analysis_records', 'holding_stock_id', "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, 'stock_analysis_records', 'analysis_scene', "TEXT NOT NULL DEFAULT ''")
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_entry_record_stock_created ON entry_decision_records(stock_code, created_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_stock_analysis_record_watch_created ON stock_analysis_records(watch_stock_id, created_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_stock_analysis_record_holding_created ON stock_analysis_records(holding_stock_id, created_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_stock_analysis_record_stock_created ON stock_analysis_records(stock_code, created_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_stock_analysis_record_scene_created ON stock_analysis_records(analysis_scene, created_at DESC)'
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS position_decision_records (
                    id TEXT PRIMARY KEY,
                    holding_stock_id TEXT NOT NULL,
                    watch_stock_id TEXT NOT NULL DEFAULT '',
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    market TEXT NOT NULL,
                    trade_date TEXT NOT NULL DEFAULT '',
                    analysis_depth TEXT NOT NULL DEFAULT '',
                    decision_type TEXT NOT NULL DEFAULT '',
                    decision_status TEXT NOT NULL DEFAULT '',
                    conclusion_summary TEXT NOT NULL DEFAULT '',
                    trigger_summary TEXT NOT NULL DEFAULT '',
                    reason_summary TEXT NOT NULL DEFAULT '',
                    execution_summary TEXT NOT NULL DEFAULT '',
                    risk_summary TEXT NOT NULL DEFAULT '',
                    confidence TEXT NOT NULL DEFAULT '',
                    tabs_json TEXT NOT NULL DEFAULT '[]',
                    evidence_json TEXT NOT NULL DEFAULT '[]',
                    raw_result_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_position_decision_holding_created ON position_decision_records(holding_stock_id, created_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_position_decision_watch_created ON position_decision_records(watch_stock_id, created_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_position_decision_type_created ON position_decision_records(decision_type, created_at DESC)'
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS holding_review_records (
                    id TEXT PRIMARY KEY,
                    holding_stock_id TEXT NOT NULL,
                    watch_stock_id TEXT NOT NULL DEFAULT '',
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    market TEXT NOT NULL,
                    trade_date TEXT NOT NULL DEFAULT '',
                    review_type TEXT NOT NULL DEFAULT '',
                    period_key TEXT NOT NULL DEFAULT '',
                    analysis_depth TEXT NOT NULL DEFAULT '',
                    performance_summary TEXT NOT NULL DEFAULT '',
                    execution_summary TEXT NOT NULL DEFAULT '',
                    risk_summary TEXT NOT NULL DEFAULT '',
                    discipline_summary TEXT NOT NULL DEFAULT '',
                    next_action_summary TEXT NOT NULL DEFAULT '',
                    conclusion_tag TEXT NOT NULL DEFAULT '',
                    tabs_json TEXT NOT NULL DEFAULT '[]',
                    evidence_json TEXT NOT NULL DEFAULT '[]',
                    context_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    raw_result_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_holding_review_holding_created ON holding_review_records(holding_stock_id, created_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_holding_review_watch_created ON holding_review_records(watch_stock_id, created_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_holding_review_type_created ON holding_review_records(review_type, created_at DESC)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_holding_review_tag_created ON holding_review_records(conclusion_tag, created_at DESC)'
            )
            connection.commit()

    def _ensure_column(self, connection, table_name: str, column_name: str, definition: str) -> None:
        existing_columns = {
            row['name']
            for row in connection.execute(f'PRAGMA table_info({table_name})').fetchall()
        }
        if column_name in existing_columns:
            return
        connection.execute(f'ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}')
