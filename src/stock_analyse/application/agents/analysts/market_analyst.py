from __future__ import annotations

from stock_analyse.application.agents.base_agent import BaseStockAnalysisAgent


class MarketAnalyst(BaseStockAnalysisAgent):
    """市场分析师角色。

    用于股票分析与持仓二次分析流程的首轮分析阶段，负责判断短线趋势、关键价位和市场风格是否支持交易假设。
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            role_name='market_analyst',
            instruction='请基于市场快照、技术指标、价格波动和板块信息，判断短期趋势、支撑压力位、潜在催化与交易风险。',
            **kwargs,
        )
