from __future__ import annotations

from stock_analyse.application.agents.base_agent import BaseStockAnalysisAgent


class BearResearcher(BaseStockAnalysisAgent):
    def __init__(self, **kwargs) -> None:
        super().__init__(
            role_name='bear_researcher',
            instruction='请仅基于 analyst_outputs 中的证据，为看空立场构建最强论据，强调回撤风险、估值压力、兑现压力或消息不确定性。',
            **kwargs,
        )
