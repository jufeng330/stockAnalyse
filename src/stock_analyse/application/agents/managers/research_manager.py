from __future__ import annotations

from stock_analyse.application.agents.base_agent import BaseStockAnalysisAgent


class ResearchManager(BaseStockAnalysisAgent):
    """研究经理角色。

    用于股票分析与持仓二次分析流程的研究汇总阶段，负责比较多空证据强弱并输出中性结论，作为交易员决策输入。
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            role_name='research_manager',
            instruction='请综合多空研究观点，判断哪一方证据更强、分歧点在哪里，并给出中立研究结论。',
            **kwargs,
        )
