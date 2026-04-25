from __future__ import annotations

from stock_analyse.application.agents.base_agent import BaseStockAnalysisAgent


class BullResearcher(BaseStockAnalysisAgent):
    def __init__(self, **kwargs) -> None:
        super().__init__(
            role_name='bull_researcher',
            instruction='请仅基于 analyst_outputs 中的证据，为看多立场构建最强论据，强调上涨驱动、估值修复或短期催化。',
            **kwargs,
        )
