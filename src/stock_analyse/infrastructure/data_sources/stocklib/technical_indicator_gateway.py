from __future__ import annotations

from stocklib.stock_ak_indicator import stockAKIndicator


class TechnicalIndicatorGateway:
    def __init__(self) -> None:
        self.indicator = stockAKIndicator()

    def get_history_data(self, market: str, symbol: str, start_date: str, end_date: str):
        return self.indicator.stock_day_data_code(symbol, market, start_date, end_date)

    def calculate_ma(self, df):
        return self.indicator.strategy_mac(df)

    def calculate_macd(self, df):
        return self.indicator.strategy_macd(df)

    def calculate_rsi(self, df, period: int = 14):
        return self.indicator.strategy_rsi(df, period=period)

    def calculate_bollinger(self, df):
        return self.indicator.strategy_bollinger(df)
