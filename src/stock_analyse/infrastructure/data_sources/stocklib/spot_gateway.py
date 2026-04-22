from __future__ import annotations

from stocklib.stock_border import stockBorderInfo


class SpotGateway:
    def get_market_spot(self, market: str):
        border = stockBorderInfo(market=market)
        return border.get_stock_spot()
