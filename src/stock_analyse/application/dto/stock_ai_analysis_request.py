from __future__ import annotations

from dataclasses import dataclass


@dataclass
class StockAIAnalysisRequest:
    stock_code: str
    market: str
    trade_date: str | None = None
    start_date_str: str | None = None
    end_date_str: str | None = None
    analysis_depth: str = 'standard'
    include_technical: bool = True
    include_sentiment: bool = True
    llm_provider: str | None = None
    llm_model: str | None = None
    api_code: str | None = None
    system_prompt: str | None = None

    def to_dict(self) -> dict:
        return {
            'stock_code': self.stock_code,
            'market': self.market,
            'trade_date': self.trade_date,
            'start_date_str': self.start_date_str,
            'end_date_str': self.end_date_str,
            'analysis_depth': self.analysis_depth,
            'include_technical': self.include_technical,
            'include_sentiment': self.include_sentiment,
            'llm_provider': self.llm_provider,
            'llm_model': self.llm_model,
            'api_code': self.api_code,
            'system_prompt': self.system_prompt,
        }
