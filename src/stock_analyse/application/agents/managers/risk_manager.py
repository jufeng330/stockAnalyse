from __future__ import annotations

from stock_analyse.application.agents.base_agent import BaseStockAnalysisAgent


class RiskManager(BaseStockAnalysisAgent):
    """风控经理角色。

    用于股票分析与持仓二次分析流程的风险约束阶段，负责评估仓位、波动与证伪风险，并给出风险等级和执行边界。
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            role_name='risk_manager',
            instruction='请评估仓位风险、波动风险、消息不确定性和基本面证伪风险，并给出风险等级与仓位约束。',
            **kwargs,
        )
