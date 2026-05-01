from __future__ import annotations

from typing import Any

from stock_analyse.application.agents.trade_plan_analysis import (
    TradePlanAnalysisAgent,
    TradePlanAnalysisInput,
    TradePlanAnalysisOutput,
)


class TradePlanAnalysisGraph:
    """持仓计划分析 graph 封装。

    用于关注股票的持仓计划分析场景，负责把页面/服务层上下文归一化为 agent 可消费的结构化输入。
    """

    def __init__(self, *, agent: TradePlanAnalysisAgent | None = None) -> None:
        self.agent = agent or TradePlanAnalysisAgent()

    def run(
        self,
        *,
        context: dict[str, Any],
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> TradePlanAnalysisOutput:
        agent_input = TradePlanAnalysisInput.model_validate(
            {
                'template_markdown': context.get('template_markdown') or '',
                'watch_stock': context.get('watch_stock') or {},
                'request': context.get('request') or {},
                'cache_context': context.get('cache_context') or {},
                'fallback_context': context.get('fallback_context') or {},
                'data_source': context.get('data_source') or 'fallback_only',
            }
        )
        return self.agent.run(
            data=agent_input,
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
            system_prompt=system_prompt,
        )


def run_trade_plan_analysis_graph(**kwargs) -> TradePlanAnalysisOutput:
    return TradePlanAnalysisGraph().run(**kwargs)
