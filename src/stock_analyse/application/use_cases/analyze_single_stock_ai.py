from __future__ import annotations

from stock_analyse.application.dto.stock_ai_analysis_response import build_error_response, build_success_response
from stock_analyse.application.orchestrators.stock_ai_analysis_orchestrator import StockAIAnalysisOrchestrator



def execute(
    stock_code: str,
    market: str,
    trade_date: str | None = None,
    start_date_str: str | None = None,
    end_date_str: str | None = None,
    analysis_depth: str = 'standard',
    include_technical: bool = True,
    include_sentiment: bool = True,
    llm_provider: str | None = None,
    llm_model: str | None = None,
    api_code: str | None = None,
    system_prompt: str | None = None,
    callbacks: dict | None = None,
    orchestrator: StockAIAnalysisOrchestrator | None = None,
) -> dict:
    try:
        orchestrator = orchestrator or StockAIAnalysisOrchestrator()
        state = orchestrator.run(
            stock_code=stock_code,
            market=market,
            trade_date=trade_date,
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            analysis_depth=analysis_depth,
            include_technical=include_technical,
            include_sentiment=include_sentiment,
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
            system_prompt=system_prompt,
            callbacks=callbacks,
        )
        decision = state.get('decision', {})
        technical = state.get('stock_snapshot', {}).get('technical', {})
        sentiment = state.get('stock_snapshot', {}).get('sentiment', {})
        fundamental = state.get('final_state', {}).get('analyst_outputs', {}).get('fundamentals', {})
        return build_success_response(
            message='AI个股分析完成',
            data={
                'stock_code': stock_code,
                'market': market,
                'trade_date': state.get('stock_snapshot', {}).get('trade_date'),
                'analysis_mode': 'agentic',
                'decision': decision,
                'final_state': state.get('final_state', {}),
                'scores': {
                    'technical': technical.get('score', 0),
                    'fundamental': round(float(fundamental.get('confidence', 0) or 0) * 100, 2),
                    'sentiment': sentiment.get('sentiment_score', 0),
                    'composite': decision.get('scores', {}).get('composite', 0),
                },
                'signals': decision.get('signals', []),
                'risks': decision.get('risks', []),
                'evidence': decision.get('evidence', []),
                'stance': decision.get('stance', ''),
                'logic': decision.get('logic', ''),
                'position_suggestion': decision.get('position_suggestion'),
                'time_horizon': decision.get('time_horizon', ''),
                'meta': state.get('meta', {}),
                'snapshot': state.get('stock_snapshot', {}),
            },
        )
    except Exception as exc:
        return build_error_response(message='AI个股分析失败', error=str(exc))
