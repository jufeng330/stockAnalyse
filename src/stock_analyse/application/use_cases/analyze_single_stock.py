from __future__ import annotations

from stock_analyse.application.orchestrators.stock_analysis_orchestrator import StockAnalysisOrchestrator



def execute(stock_code: str, market: str, start_date_str: str, end_date_str: str, selected_strategies: list[str], system_prompt: str, message_format: str, ai_platform: str, ai_model: str, api_code: str, callbacks=None, orchestrator: StockAnalysisOrchestrator | None = None) -> dict:
    try:
        orchestrator = orchestrator or StockAnalysisOrchestrator()
        data = orchestrator.run(
            stock_code=stock_code,
            market=market,
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            selected_strategies=selected_strategies,
            system_prompt=system_prompt,
            message_format=message_format,
            ai_platform=ai_platform,
            ai_model=ai_model,
            api_code=api_code,
            callbacks=callbacks,
        )
        return {
            'success': True,
            **data,
        }
    except Exception as exc:
        return {
            'success': False,
            'error': str(exc),
            'message': '服务器内部错误',
        }
