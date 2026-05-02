from __future__ import annotations

from stock_analyse.application.dto.stock_ai_analysis_state import StockAIAnalysisState
from stock_analyse.application.graphs.trading_decision.focus_stock_analysis_graph import FocusStockAnalysisGraph
from stock_analyse.application.graphs.trading_decision.holding_reanalysis_graph import HoldingReanalysisGraph


def _normalize_scene(value: str | None) -> str:
    return 'holding_reanalysis' if str(value or '').strip() == 'holding_reanalysis' else 'stock_analysis'


class StockAnalysisGraph:
    """兼容保留的股票分析共享 graph。

    当前仅作为旧导入入口，内部根据场景转调 FocusStockAnalysisGraph 或 HoldingReanalysisGraph。
    """

    def __init__(self) -> None:
        self._focus_graph = FocusStockAnalysisGraph()
        self._holding_graph = HoldingReanalysisGraph()

    def run(self, **context) -> StockAIAnalysisState:
        scene = _normalize_scene(context.get('analysis_scene'))
        if scene == 'holding_reanalysis':
            return self._holding_graph.run(**context)
        return self._focus_graph.run(**context)


def run_stock_analysis_graph(**kwargs) -> StockAIAnalysisState:
    return StockAnalysisGraph().run(**kwargs)


def run_focus_stock_analysis_graph(**kwargs) -> StockAIAnalysisState:
    return FocusStockAnalysisGraph().run(**kwargs)


def run_holding_reanalysis_graph(**kwargs) -> StockAIAnalysisState:
    return HoldingReanalysisGraph().run(**kwargs)
