from __future__ import annotations

from stock_analyse.application.agents.base_agent import BaseStockAnalysisAgent


class NewsAnalyst(BaseStockAnalysisAgent):
    """消息面分析师角色。

    用于股票分析与持仓二次分析流程的事件扫描阶段，负责整理短期催化、利空与舆情方向，辅助判断消息驱动是否可靠。
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(
            role_name='news_analyst',
            instruction='请基于近期香港/美股/A股相关新闻与情绪数据，判断短期催化、利空、舆情方向和消息可信度。',
            **kwargs,
        )
