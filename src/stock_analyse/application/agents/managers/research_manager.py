from __future__ import annotations

from stock_analyse.application.agents.base_agent import BaseStockAnalysisAgent


class ResearchManager(BaseStockAnalysisAgent):
    def __init__(self, **kwargs) -> None:
        super().__init__(
            role_name='research_manager',
            instruction='请综合多空研究观点，判断哪一方证据更强、分歧点在哪里，并给出中立研究结论。',
            **kwargs,
        )
