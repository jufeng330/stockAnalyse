from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from stock_analyse.infrastructure.llm.client import StockAiClient


class StockAiAdapter:
    def __init__(self, system_prompt: str, prompt_template: str, ai_platform: str, ai_model: str, api_code: str) -> None:
        self.analysis_client = StockAiClient(
            system_prompt=system_prompt,
            prompt_template=prompt_template,
            ai_platform=ai_platform,
            model=ai_model,
            api_token=api_code,
        )
        self.report_client = StockAiClient(
            system_prompt=system_prompt,
            prompt_template=prompt_template,
            ai_platform=ai_platform,
            model=ai_model,
            api_token=api_code,
        )

    def analyze(self, market: str, symbol: str, start_date: str, end_date: str) -> tuple[str, str, str]:
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_analysis = executor.submit(self.analysis_client.analyze_indicator, market=market, symbol=symbol, start_date=start_date, end_date=end_date)
            future_report = executor.submit(self.report_client.analyze_report, market=market, symbol=symbol)
            future_summary = executor.submit(self.analysis_client.get_summary, market=market, symbol=symbol)
            return future_summary.result(), future_analysis.result(), future_report.result()
