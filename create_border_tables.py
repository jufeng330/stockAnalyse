from sqlalchemy import create_engine, text

engine = create_engine('mysql+pymysql://root:aloo.1234-qwer@192.168.1.12:3306/stock_info')

queries = [
    """
    CREATE TABLE IF NOT EXISTS stock_border_SH (
        code VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100),
        latest_price DOUBLE,
        change_rate DOUBLE,
        change_amount DOUBLE,
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        prev_close DOUBLE,
        volume DOUBLE,
        turnover DOUBLE,
        turnover_rate DOUBLE,
        pe_dynamic DOUBLE,
        total_market_value DOUBLE,
        pb DOUBLE,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_border_H (
        code VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100),
        latest_price DOUBLE,
        change_rate DOUBLE,
        change_amount DOUBLE,
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        prev_close DOUBLE,
        volume DOUBLE,
        turnover DOUBLE,
        turnover_rate DOUBLE,
        pe_dynamic DOUBLE,
        total_market_value DOUBLE,
        pb DOUBLE,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_border_usa (
        code VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100),
        latest_price DOUBLE,
        change_rate DOUBLE,
        change_amount DOUBLE,
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        prev_close DOUBLE,
        volume DOUBLE,
        turnover DOUBLE,
        turnover_rate DOUBLE,
        pe_dynamic DOUBLE,
        total_market_value DOUBLE,
        pb DOUBLE,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS financial_indicate_H (
        code VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100),
        latest_price DOUBLE,
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        prev_close DOUBLE,
        volume DOUBLE,
        turnover DOUBLE,
        turnover_rate DOUBLE,
        lot_size DOUBLE,
        amplitude DOUBLE,
        avg_price DOUBLE,
        volume_ratio DOUBLE,
        week52_high DOUBLE,
        week52_low DOUBLE,
        historical_high DOUBLE,
        historical_low DOUBLE,
        issued_shares DOUBLE,
        circulating_shares DOUBLE,
        total_market_value DOUBLE,
        circulating_market_value DOUBLE,
        net_assets DOUBLE,
        net_profit DOUBLE,
        eps DOUBLE,
        naps DOUBLE,
        yield_rate DOUBLE,
        pe DOUBLE,
        pb DOUBLE,
        pe_ttm DOUBLE,
        dividend_ttm DOUBLE,
        dividend_ratio_ttm DOUBLE,
        dividend_lfy DOUBLE,
        dividend_lfy_ratio DOUBLE,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS financial_indicate_usa (
        code VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100),
        latest_price DOUBLE,
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        prev_close DOUBLE,
        volume DOUBLE,
        turnover DOUBLE,
        turnover_rate DOUBLE,
        lot_size DOUBLE,
        amplitude DOUBLE,
        avg_price DOUBLE,
        volume_ratio DOUBLE,
        week52_high DOUBLE,
        week52_low DOUBLE,
        historical_high DOUBLE,
        historical_low DOUBLE,
        issued_shares DOUBLE,
        circulating_shares DOUBLE,
        total_market_value DOUBLE,
        circulating_market_value DOUBLE,
        net_assets DOUBLE,
        net_profit DOUBLE,
        eps DOUBLE,
        naps DOUBLE,
        yield_rate DOUBLE,
        pe DOUBLE,
        pb DOUBLE,
        pe_ttm DOUBLE,
        dividend_ttm DOUBLE,
        dividend_ratio_ttm DOUBLE,
        dividend_lfy DOUBLE,
        dividend_lfy_ratio DOUBLE,
        pre_price DOUBLE,
        pre_volume DOUBLE,
        pre_turnover DOUBLE,
        pre_change_amount DOUBLE,
        pre_change_rate DOUBLE,
        post_price DOUBLE,
        post_volume DOUBLE,
        post_turnover DOUBLE,
        post_change_amount DOUBLE,
        post_change_rate DOUBLE,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """
]

with engine.connect() as con:
    for q in queries:
        con.execute(text(q))
    con.commit()
    print("Tables created successfully.")
