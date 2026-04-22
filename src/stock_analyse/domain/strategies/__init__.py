from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_type: int
    strategy_filter: str = 'avg'


class StockSelectionStrategy(Protocol):
    def select(self, df_stock, strategy_type: int, strategy_filter: str = 'avg'):
        ...
