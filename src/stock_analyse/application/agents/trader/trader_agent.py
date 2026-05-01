from __future__ import annotations

from stock_analyse.application.agents.base_agent import BaseStockAnalysisAgent


class TraderAgent(BaseStockAnalysisAgent):
    """交易员角色。

    用于股票分析与持仓二次分析流程的最终决策阶段，综合研究经理结论和风控边界输出 action、摘要与仓位建议。
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            role_name='trader_agent',
            instruction='请基于研究经理结论和风险经理约束，输出最终交易建议，包括 action、summary、仓位建议、主要证据和风险提醒。',
            **kwargs,
        )
