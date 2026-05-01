from __future__ import annotations

from stock_analyse.application.agents.base_agent import BaseStockAnalysisAgent


class BearResearcher(BaseStockAnalysisAgent):
    """空头研究员角色。

    用于股票分析与持仓二次分析流程的多空对照阶段，只基于已有证据构建看空论据，供后续研究经理仲裁。
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            role_name='bear_researcher',
            instruction='请仅基于 analyst_outputs 中的证据，为看空立场构建最强论据，强调回撤风险、估值压力、兑现压力或消息不确定性。',
            **kwargs,
        )
