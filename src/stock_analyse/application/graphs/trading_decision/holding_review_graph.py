from __future__ import annotations

from typing import Any

from stock_analyse.application.agents.holding_review import HoldingReviewAgent, HoldingReviewInput, HoldingReviewOutput


class HoldingReviewGraph:
    def __init__(self, *, agent: HoldingReviewAgent | None = None) -> None:
        self.agent = agent or HoldingReviewAgent()

    def run(
        self,
        *,
        context: dict[str, Any],
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> HoldingReviewOutput:
        agent_input = HoldingReviewInput.model_validate(
            {
                'holding_stock': context.get('holding_stock') or {},
                'watch_stock': context.get('watch_stock') or {},
                'request': context.get('request') or {},
                'trade_history_context': context.get('trade_history_context') or {},
                'entry_context': context.get('entry_context') or {},
                'reanalysis_context': context.get('reanalysis_context') or {},
                'position_decision_context': context.get('position_decision_context') or {},
                'financial_context': context.get('financial_context') or {},
                'market_context': context.get('market_context') or {},
                'review_focus_context': context.get('review_focus_context') or {},
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


def run_holding_review_graph(**kwargs) -> HoldingReviewOutput:
    return HoldingReviewGraph().run(**kwargs)
