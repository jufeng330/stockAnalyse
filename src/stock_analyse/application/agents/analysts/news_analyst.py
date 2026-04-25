from __future__ import annotations

from stock_analyse.application.agents.base_agent import BaseStockAnalysisAgent


class NewsAnalyst(BaseStockAnalysisAgent):
    def __init__(self, **kwargs) -> None:
        super().__init__(
            role_name='news_analyst',
            instruction='请基于近期香港/美股/A股相关新闻与情绪数据，判断短期催化、利空、舆情方向和消息可信度。',
            **kwargs,
        )
