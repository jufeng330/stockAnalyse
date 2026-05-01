from __future__ import annotations

from typing import Any

from stock_analyse.application.agents.position_decision import PositionDecisionAgent, PositionDecisionInput, PositionDecisionOutput


class PositionDecisionGraph:
    """买卖决策 graph 封装。

    用于持仓股票的买卖决策场景，负责把持仓、财务与计划上下文整理成结构化 agent 输入。
    """

    def __init__(self, *, agent: PositionDecisionAgent | None = None) -> None:
        self.agent = agent or PositionDecisionAgent()

    def run(
        self,
        *,
        context: dict[str, Any],
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> PositionDecisionOutput:
        agent_input = PositionDecisionInput.model_validate(
            {
                'holding_stock': context.get('holding_stock') or {},
                'watch_stock': context.get('watch_stock') or {},
                'request': context.get('request') or {},
                'financial_context': context.get('financial_context') or {},
                'trade_history_context': context.get('trade_history_context') or {},
                'holding_plan_context': context.get('holding_plan_context') or {},
                'supporting_context': context.get('supporting_context') or {},
                'data_source': context.get('data_source') or 'holding_snapshot',
            }
        )
        return self.agent.run(
            data=agent_input,
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
            system_prompt=system_prompt,
        )


def run_position_decision_graph(**kwargs) -> PositionDecisionOutput:
    return PositionDecisionGraph().run(**kwargs)
