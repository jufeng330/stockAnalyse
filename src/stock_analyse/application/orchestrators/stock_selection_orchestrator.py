from __future__ import annotations

from scanner.stock_select_strategy import StockSelectStrategy
from scanner.top_stock_scanner import TopStockScanner


class StockSelectionOrchestrator:
    def calculate_score(self, market: str, symbol: str):
        scanner = TopStockScanner(market=market)
        return scanner.analyzer.analyze_stock_safe({'代码': symbol, 'market': market, '股票代码': symbol})

    def batch_analyze(self, market: str, min_score: int = 30, strategy_type: int = 1):
        scanner = TopStockScanner(max_workers=20, market=market, strategy_type=strategy_type)
        return scanner.scan_high_score_stocks(batch_size=20, type=strategy_type, strategy_filter='avg')

    def select_candidates(self, market: str, strategy_type: int = 1, strategy_filter: str = 'avg'):
        scanner = TopStockScanner(max_workers=20, market=market, strategy_type=strategy_type)
        df_stocks_data = scanner.get_all_stocks()
        selector = StockSelectStrategy(market=market, strategy_type=strategy_type)
        return selector.select_stock(df_stocks_data, strategy_type=strategy_type, strategy_filter=strategy_filter)
