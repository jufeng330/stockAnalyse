from __future__ import annotations

import pandas as pd

from stock_analyse.domain.strategies.selection_strategy_service import SelectionStrategyService


def get_strategy_name(strategy_type: int | str, service: SelectionStrategyService | None = None) -> str:
    service = service or SelectionStrategyService()
    return service.get_strategy_name(strategy_type)


def execute(
    df_stock: pd.DataFrame,
    market: str,
    strategy_type: int = 1,
    strategy_filter: str = 'avg',
    service: SelectionStrategyService | None = None,
) -> pd.DataFrame:
    service = service or SelectionStrategyService()
    return service.select(df_stock, market=market, strategy_type=strategy_type, strategy_filter=strategy_filter)
