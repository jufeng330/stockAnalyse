from __future__ import annotations

from typing import Iterable

from futu import AccumulateFilter, FinancialFilter, FinancialQuarter, Market, SimpleFilter, SortDir, StockField


class FutuStockFilterMapper:
    SUPPORTED_MARKETS = {'H', 'HK', 'usa'}

    @classmethod
    def supports_market(cls, market: str) -> bool:
        return str(market or '').strip() in cls.SUPPORTED_MARKETS

    @classmethod
    def to_futu_market(cls, market: str):
        normalized = str(market or '').strip()
        if normalized in {'H', 'HK'}:
            return Market.HK
        if normalized == 'usa':
            return Market.US
        raise ValueError(f'Unsupported market for Futu stock filter: {market}')

    @classmethod
    def build_filters(cls, strategy_type: int | str, market: str, config: dict | None) -> list:
        strategy_code = int(strategy_type)
        normalized_market = str(market or '').strip()
        filters = []
        spot = ((config or {}).get('filters') or {}).get('spot') or {}
        financial = ((config or {}).get('filters') or {}).get('financial') or {}
        risk = ((config or {}).get('filters') or {}).get('risk') or {}
        dividend = ((config or {}).get('filters') or {}).get('dividend') or {}

        cls._extend(filters, cls._build_strategy_common(strategy_code, normalized_market, spot, financial, risk, dividend))
        return filters

    @classmethod
    def _build_strategy_common(cls, strategy_code: int, market: str, spot: dict, financial: dict, risk: dict, dividend: dict) -> list:
        filters: list = []
        market_cap_min = cls._number(spot.get('market_cap_min'))
        pe_dynamic_max = cls._number(spot.get('pe_dynamic_max'))
        pb_max = cls._number(spot.get('pb_max'))
        revenue_total_min = cls._number(spot.get('revenue_total_min'))
        debt_ratio_max = cls._number(risk.get('debt_ratio_max'))
        profit_min = cls._number(financial.get('profit_min'))
        profit_growth_min = cls._number(financial.get('profit_growth_min'))
        revenue_growth_min = cls._number(financial.get('revenue_growth_min'))
        roe_min = cls._number(financial.get('roe_min'))
        dividend_yield_min = cls._number(dividend.get('dividend_yield_min'))

        if strategy_code in {1, 2, 3, 5, 7} and market_cap_min is not None:
            filters.append(cls._simple(StockField.MARKET_VAL, filter_min=market_cap_min))
        if strategy_code in {1, 2, 3, 5, 6, 7} and pe_dynamic_max is not None:
            filters.append(cls._simple(StockField.PE_TTM, filter_max=pe_dynamic_max))
        if strategy_code in {2, 5} and pb_max is not None:
            filters.append(cls._simple(StockField.PB_RATE, filter_max=pb_max))
        if strategy_code in {2, 3, 1} and debt_ratio_max is not None:
            filters.append(cls._financial(market, StockField.DEBT_ASSET_RATE, filter_max=debt_ratio_max))
        if strategy_code in {2, 4} and revenue_total_min is not None:
            filters.append(cls._financial(market, StockField.SUM_OF_BUSINESS, filter_min=revenue_total_min))
        if strategy_code in {2, 4, 6} and profit_min is not None and profit_min > 0:
            filters.append(cls._financial(market, StockField.NET_PROFIT, filter_min=profit_min))
        if strategy_code in {2, 4, 6} and profit_growth_min is not None and profit_growth_min > 0:
            filters.append(cls._financial(market, StockField.NET_PROFIX_GROWTH, filter_min=cls._percent_to_futu(profit_growth_min)))
        if strategy_code in {2, 4, 6} and revenue_growth_min is not None and revenue_growth_min > 0:
            filters.append(cls._financial(market, StockField.SUM_OF_BUSINESS_GROWTH, filter_min=cls._percent_to_futu(revenue_growth_min)))
        if strategy_code == 5 and roe_min is not None:
            filters.append(cls._financial(market, StockField.RETURN_ON_EQUITY_RATE, filter_min=roe_min))
        if strategy_code == 1 and dividend_yield_min is not None:
            filters.append(cls._simple(StockField.DIVIDEND_RATIO if hasattr(StockField, 'DIVIDEND_RATIO') else StockField.NONE, filter_min=cls._percent_to_futu(dividend_yield_min), allow_none=True))
        return filters

    @staticmethod
    def _extend(target: list, filters: Iterable) -> None:
        for item in filters:
            if item is not None:
                target.append(item)

    @staticmethod
    def _number(value):
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _percent_to_futu(value: float) -> float:
        return float(value) * 100 if abs(float(value)) <= 1 else float(value)

    @classmethod
    def _simple(cls, stock_field, *, filter_min=None, filter_max=None, allow_none: bool = False):
        if stock_field == StockField.NONE and not allow_none:
            return None
        if stock_field == StockField.NONE:
            return None
        item = SimpleFilter()
        item.stock_field = stock_field
        item.is_no_filter = False
        item.sort = SortDir.NONE
        if filter_min is not None:
            item.filter_min = float(filter_min)
        if filter_max is not None:
            item.filter_max = float(filter_max)
        return item

    @classmethod
    def _financial(cls, market: str, stock_field, *, filter_min=None, filter_max=None):
        item = FinancialFilter()
        item.stock_field = stock_field
        item.is_no_filter = False
        item.sort = SortDir.NONE
        normalized_market = str(market or '').strip()
        item.quarter = FinancialQuarter.ANNUAL if normalized_market in {'H', 'HK'} else FinancialQuarter.MOST_RECENT_QUARTER
        if filter_min is not None:
            item.filter_min = float(filter_min)
        if filter_max is not None:
            item.filter_max = float(filter_max)
        return item
