from __future__ import annotations

import pandas as pd

from stock_analyse.application.orchestrators.stock_selection_orchestrator import StockSelectionOrchestrator


def execute(market: str, strategy_code: str, orchestrator: StockSelectionOrchestrator | None = None) -> dict:
    orchestrator = orchestrator or StockSelectionOrchestrator()
    strategy_type = int(strategy_code)
    file_utils, high_score_stocks = orchestrator.run_web_selection(market=market, strategy_type=strategy_type)

    if not high_score_stocks:
        high_score_stocks_text = '未找到得分大于等于85分的股票。'
    elif isinstance(high_score_stocks, list):
        high_score_stocks_text = pd.DataFrame(high_score_stocks).to_markdown()
    else:
        high_score_stocks_text = high_score_stocks.to_markdown()

    summary = file_utils.read_text_file('summary.txt')
    all_results = file_utils.read_text_file('temp_results.txt')
    return {
        'success': True,
        'high_score_text': high_score_stocks_text,
        'summary_text': summary,
        'all_results': all_results,
        'message': f'股票 {market}_{strategy_type} 分析完成',
    }
