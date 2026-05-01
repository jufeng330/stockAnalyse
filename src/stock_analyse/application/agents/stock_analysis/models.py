from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


class StockAnalysisInput(BaseModel):
    model_config = ConfigDict(extra='allow')

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
    analysis_scene: Literal['stock_analysis', 'holding_reanalysis'] = 'stock_analysis'
