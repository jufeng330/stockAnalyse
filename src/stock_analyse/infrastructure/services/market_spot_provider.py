from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class MarketSpotProvider(ABC):
    @abstractmethod
    def supports_market(self, market: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_stock_spot(self, market: str) -> pd.DataFrame:
        raise NotImplementedError
