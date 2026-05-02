from __future__ import annotations

from stock_analyse.application.orchestrators.focus_stock_ai_analysis_orchestrator import FocusStockAIAnalysisOrchestrator
from stock_analyse.application.orchestrators.holding_stock_reanalysis_orchestrator import HoldingStockReanalysisOrchestrator


def normalize_stock_ai_scene(value: str | None) -> str:
    return 'holding_reanalysis' if str(value or '').strip() == 'holding_reanalysis' else 'stock_analysis'


class StockAIAnalysisOrchestrator:
    """兼容保留的股票 AI 分析共享编排器。

    当前仅作为旧入口，根据场景分发到 FocusStockAIAnalysisOrchestrator 或 HoldingStockReanalysisOrchestrator。
    """

    def __init__(self) -> None:
        self._focus_orchestrator = FocusStockAIAnalysisOrchestrator()
        self._holding_orchestrator = HoldingStockReanalysisOrchestrator()

    def run(self, *, analysis_scene: str | None = None, **kwargs):
        scene = normalize_stock_ai_scene(analysis_scene)
        if scene == 'holding_reanalysis':
            return self._holding_orchestrator.run(**kwargs)
        return self._focus_orchestrator.run(**kwargs)


def build_stock_ai_orchestrator(analysis_scene: str | None = None):
    scene = normalize_stock_ai_scene(analysis_scene)
    if scene == 'holding_reanalysis':
        return HoldingStockReanalysisOrchestrator()
    return FocusStockAIAnalysisOrchestrator()
