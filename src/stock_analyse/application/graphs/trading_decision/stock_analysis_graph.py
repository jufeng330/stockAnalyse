from __future__ import annotations

from typing import Any

from stock_analyse.application.agents.stock_analysis import StockAnalysisAgent
from stock_analyse.application.agents.stock_analysis.models import StockAnalysisInput
from stock_analyse.application.dto.stock_ai_analysis_state import StockAIAnalysisState


class StockAnalysisGraph:
    """股票分析共享 graph。

    用于 `/api/analyze_stock_ai` 背后的普通股票分析与持仓二次分析场景，负责把请求上下文归一化为共享状态机输入。
    """

    def __init__(self, *, agent: StockAnalysisAgent | None = None) -> None:
        self.agent = agent or StockAnalysisAgent()

    def run(self, *, callbacks: dict[str, Any] | None = None, **context: Any) -> StockAIAnalysisState:
        agent_input = StockAnalysisInput.model_validate(
            {
                'stock_code': context.get('stock_code'),
                'market': context.get('market'),
                'trade_date': context.get('trade_date'),
                'start_date_str': context.get('start_date_str'),
                'end_date_str': context.get('end_date_str'),
                'analysis_depth': context.get('analysis_depth') or 'standard',
                'include_technical': context.get('include_technical', True),
                'include_sentiment': context.get('include_sentiment', True),
                'llm_provider': context.get('llm_provider'),
                'llm_model': context.get('llm_model'),
                'api_code': context.get('api_code'),
                'system_prompt': context.get('system_prompt'),
                'analysis_scene': context.get('analysis_scene') or 'stock_analysis',
            }
        )
        state = StockAIAnalysisState(
            request={
                'stock_code': agent_input.stock_code,
                'market': agent_input.market,
                'trade_date': agent_input.trade_date,
                'start_date_str': agent_input.start_date_str,
                'end_date_str': agent_input.end_date_str,
                'analysis_depth': agent_input.analysis_depth,
                'include_technical': agent_input.include_technical,
                'include_sentiment': agent_input.include_sentiment,
                'llm_provider': agent_input.llm_provider,
                'llm_model': agent_input.llm_model,
                'analysis_scene': agent_input.analysis_scene,
            }
        )
        return self.agent.run(data=agent_input, state=state, callbacks=callbacks)


def run_stock_analysis_graph(**kwargs) -> StockAIAnalysisState:
    return StockAnalysisGraph().run(**kwargs)
