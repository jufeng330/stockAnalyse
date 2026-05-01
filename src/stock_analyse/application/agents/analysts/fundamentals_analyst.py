from __future__ import annotations

from stock_analyse.application.agents.base_agent import BaseStockAnalysisAgent


class FundamentalsAnalyst(BaseStockAnalysisAgent):
    """基本面分析师角色。

    用于股票分析与持仓二次分析流程的基础研究阶段，负责评估盈利质量、成长性、估值与中期基本面风险。
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            role_name='fundamentals_analyst',
            instruction='请基于公司资料、主营业务、财务指标和财报摘要，评估盈利质量、成长性、估值合理性与中期基本面风险。',
            **kwargs,
        )
