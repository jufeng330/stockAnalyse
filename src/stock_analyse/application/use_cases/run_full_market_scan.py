from __future__ import annotations

from stock_analyse.application.orchestrators.stock_selection_orchestrator import StockSelectionOrchestrator


def _strategy_types_for_market(market: str) -> list[int]:
    return [1, 2, 3, 4, 5, 6] if market == 'SH' else [4, 6]


def execute(
    markets: list[str] | None = None,
    batch_size: int = 20,
    strategy_filter: str = 'avg',
    analysis_date: str = '2025-06-06',
    orchestrator: StockSelectionOrchestrator | None = None,
) -> dict:
    orchestrator = orchestrator or StockSelectionOrchestrator()
    markets = markets or ['SH', 'H', 'usa']
    summaries = []

    for market in markets:
        for strategy_type in _strategy_types_for_market(market):
            file_utils, high_score_stocks = orchestrator.run_full_market_scan(
                market=market,
                strategy_type=strategy_type,
                batch_size=batch_size,
                strategy_filter=strategy_filter,
            )
            if not high_score_stocks:
                summaries.append(
                    {
                        'market': market,
                        'strategy_type': strategy_type,
                        'qualified': 0,
                        'stats': '未找到得分大于等于85分的股票。',
                    }
                )
                continue

            file_utils.save_results_by_price(high_score_stocks)
            df_high_score_stocks, stats = orchestrator.backtest_stocks(
                market=market,
                high_score_stocks=high_score_stocks,
                analysis_date=analysis_date,
            )
            file_utils.create_middle_file('回测结果', df_high_score_stocks)
            file_utils.create_text_file('回测结果_统计', stats)
            summaries.append(
                {
                    'market': market,
                    'strategy_type': strategy_type,
                    'qualified': len(high_score_stocks),
                    'stats': stats,
                }
            )

    return {
        'success': True,
        'data': {
            'markets': markets,
            'summaries': summaries,
        },
        'message': '全市场扫描完成',
    }
