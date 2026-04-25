from __future__ import annotations

from typing import Any

from stock_analyse.application.use_cases import select_stock_strategy as select_stock_strategy_use_case


class StockSelectionOrchestrator:
    def __init__(
        self,
        *,
        full_market_scan_workflow: Any | None = None,
        backtest_workflow: Any | None = None,
        technical_analysis_workflow: Any | None = None,
    ) -> None:
        self._full_market_scan_workflow = full_market_scan_workflow
        self._backtest_workflow = backtest_workflow
        self._technical_analysis_workflow = technical_analysis_workflow

    def _get_full_market_scan_workflow(self):
        if self._full_market_scan_workflow is None:
            from stock_analyse.application.workflows.full_market_scan_workflow import FullMarketScanWorkflow

            self._full_market_scan_workflow = FullMarketScanWorkflow()
        return self._full_market_scan_workflow

    def _get_backtest_workflow(self):
        if self._backtest_workflow is None:
            from stock_analyse.application.workflows.backtest_stocks_workflow import BacktestStocksWorkflow

            self._backtest_workflow = BacktestStocksWorkflow()
        return self._backtest_workflow

    def _get_technical_analysis_workflow(self):
        if self._technical_analysis_workflow is None:
            from stock_analyse.application.workflows.technical_analysis_workflow import TechnicalAnalysisWorkflow

            self._technical_analysis_workflow = TechnicalAnalysisWorkflow()
        return self._technical_analysis_workflow

    def calculate_score(self, market: str, symbol: str):
        return self._get_technical_analysis_workflow().analyze_stock(
            {'代码': symbol, 'market': market, '股票代码': symbol},
            market=market,
        )

    def batch_analyze(self, market: str, min_score: int = 30, strategy_type: int = 1):
        _, high_score_stocks = self._get_full_market_scan_workflow().run(
            market=market,
            strategy_type=strategy_type,
            batch_size=20,
            strategy_filter='avg',
            min_score=min_score,
        )
        return high_score_stocks

    def run_web_selection(self, market: str, strategy_type: int = 1):
        file_utils, high_score_stocks = self._get_full_market_scan_workflow().run(
            market=market,
            strategy_type=strategy_type,
            batch_size=20,
            strategy_filter='avg',
        )
        file_utils.save_results_by_price(high_score_stocks)
        return file_utils, high_score_stocks

    def run_full_market_scan(self, market: str, strategy_type: int = 1, batch_size: int = 20, strategy_filter: str = 'avg'):
        return self._get_full_market_scan_workflow().run(
            market=market,
            strategy_type=strategy_type,
            batch_size=batch_size,
            strategy_filter=strategy_filter,
        )

    def backtest_stocks(self, market: str, high_score_stocks: list[dict], analysis_date: str):
        return self._get_backtest_workflow().run(
            market=market,
            high_score_stocks=high_score_stocks,
            analysis_date=analysis_date,
        )

    def select_candidates(self, market: str, strategy_type: int = 1, strategy_filter: str = 'avg'):
        file_utils, _ = self._get_full_market_scan_workflow().build_runtime(
            market=market,
            strategy_type=strategy_type,
        )
        df_stocks_data = self._get_full_market_scan_workflow().get_all_stocks(market=market)
        _ = file_utils
        return select_stock_strategy_use_case.execute(
            df_stock=df_stocks_data,
            market=market,
            strategy_type=strategy_type,
            strategy_filter=strategy_filter,
        )

    def get_strategy_name(self, strategy_type: int | str) -> str:
        return select_stock_strategy_use_case.get_strategy_name(strategy_type)

    def create_strategy_selector(self, market: str, strategy_type: int = 1):
        from stock_analyse.domain.strategies.stock_select_strategy import StockSelectStrategy

        return StockSelectStrategy(market=market, strategy_type=strategy_type)

    def run_strategy_selection(self, df_stock, market: str, strategy_type: int = 1, strategy_filter: str = 'avg'):
        return select_stock_strategy_use_case.execute(
            df_stock=df_stock,
            market=market,
            strategy_type=strategy_type,
            strategy_filter=strategy_filter,
        )
