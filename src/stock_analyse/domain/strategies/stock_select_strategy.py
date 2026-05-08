from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from stock_analyse.domain.strategies.financial_filter_service import FinancialFilterService
from stock_analyse.domain.strategies.selection_strategy_service import SelectionStrategyService
from stock_analyse.infrastructure.persistence.stock_file_utils import StockFileUtils
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
from stock_analyse.domain.services.stock_strategy_service import StockStrategy
from stock_analyse.shared.report_date_utils import ReportDateUtils
from stock_analyse.shared.select_config_loader import load_select_config
from stock_analyse.shared.stock_utils import StockUtils


class StockSelectStrategy:
    """股票筛选策略类，采用策略模式实现不同的股票筛选逻辑"""

    def __init__(self, market: str = 'SH', strategy_type='1'):
        self.market = market
        self.stock = stockBorderInfo(market=self.market)
        self.reportUtils = ReportDateUtils()
        self.stock_strategy = StockStrategy()
        self.stock_utils = StockUtils()
        self.financial_filter_service = FinancialFilterService()
        self.select_config = load_select_config()
        strategy_name = self.get_strategy_name(strategy_type)
        self.file_utils = StockFileUtils(market=self.market, name=strategy_name)
        self.logger = logging.getLogger(__name__)
        self.strategy_name = strategy_name

    def get_strategy_name(self, strategy_type):
        if strategy_type == 1:
            return '高股息选股策略_1'
        elif strategy_type == 2:
            return '优质股筛选策略_2'
        elif strategy_type == 3:
            return '保守型筛选策略_3'
        elif strategy_type == 4:
            return '成长型筛选策略_4'
        elif strategy_type == 5:
            return '价值型筛选策略_5'
        elif strategy_type == 6:
            return '知名股票筛选策略_6'
        return '未知_' + str(strategy_type)

    def select_stock(self, df_stock, strategy_type=1, strategy_filter='continnue') -> pd.DataFrame:
        if strategy_type == 1:
            return self.normal_strategy(df_stock, strategy_filter)
        elif strategy_type == 2:
            return self.quality_strategy(df_stock, strategy_filter)
        elif strategy_type == 3:
            return self.conservative_strategy(df_stock)
        elif strategy_type == 4:
            return self.growth_strategy(df_stock)
        elif strategy_type == 5:
            return self.value_strategy(df_stock)
        elif strategy_type == 6:
            return self.famous_stock_strategy(df_stock)
        return pd.DataFrame()

    def normal_strategy(self, df_stock: Optional[pd.DataFrame] = None, strategy_filter: str = 'avg') -> pd.DataFrame:
        return SelectionStrategyService().normal_strategy(
            df_stock=df_stock,
            market=self.market,
            strategy_filter=strategy_filter,
            selector=self,
        )

    def quality_strategy(self, df_stock: Optional[pd.DataFrame] = None, strategy_filter: str = 'avg') -> pd.DataFrame:
        return SelectionStrategyService().quality_strategy(
            df_stock=df_stock,
            market=self.market,
            strategy_filter=strategy_filter,
            selector=self,
        )

    def conservative_strategy(self, df_stock: Optional[pd.DataFrame] = None, strategy_filter: str = 'avg') -> pd.DataFrame:
        return SelectionStrategyService().conservative_strategy(
            df_stock=df_stock,
            market=self.market,
            strategy_filter=strategy_filter,
            selector=self,
        )

    def growth_strategy(self, df_stock: Optional[pd.DataFrame] = None, strategy_filter: str = 'avg') -> pd.DataFrame:
        return SelectionStrategyService().growth_strategy(
            df_stock=df_stock,
            market=self.market,
            strategy_filter=strategy_filter,
            selector=self,
        )

    def value_strategy(self, df_stock: Optional[pd.DataFrame] = None, strategy_filter: str = 'avg') -> pd.DataFrame:
        return SelectionStrategyService().value_strategy(
            df_stock=df_stock,
            market=self.market,
            strategy_filter=strategy_filter,
            selector=self,
        )

    def famous_stock_strategy(self, df_stock: Optional[pd.DataFrame] = None, strategy_filter: str = 'avg') -> pd.DataFrame:
        return SelectionStrategyService().famous_stock_strategy(
            df_stock=df_stock,
            market=self.market,
            strategy_filter=strategy_filter,
            selector=self,
        )

    def _find_financial_stock_data(
        self,
        date_financial: str,
        df_financial: pd.DataFrame,
        data_type: str = 'continue',
        threshold_1: float = 0.0,
        threshold_2: float = 0.0,
        threshold_3: float = 0.0,
    ) -> set:
        return self.financial_filter_service.find_financial_stock_data(
            date_financial=date_financial,
            df_financial=df_financial,
            stock_strategy=self.stock_strategy,
            market=self.market,
            logger=self.logger,
            data_type=data_type,
            threshold_1=threshold_1,
            threshold_2=threshold_2,
            threshold_3=threshold_3,
        )

    def compute_financial_lrl_ratio(self, df_financial, col_lrl='净利润同比增长率', col_lr='净利润'):
        return self.financial_filter_service.compute_financial_lrl_ratio(
            df_financial=df_financial,
            col_lrl=col_lrl,
            col_lr=col_lr,
        )

    def get_strategy_config(self, strategy_type: int | str) -> dict:
        strategy_key = str(strategy_type)
        defaults = self.select_config.get('defaults', {})
        strategies = self.select_config.get('strategies', {})
        strategy_config = strategies.get(strategy_key, {})
        market_key = 'H' if str(self.market).upper() == 'HK' else str(self.market)
        market_overrides = (strategy_config.get('market_overrides') or {}).get(market_key, {})
        merged = {
            **defaults,
            **strategy_config,
        }
        default_meta = defaults.get('meta', {})
        strategy_meta = strategy_config.get('meta', {})
        merged['meta'] = {
            **default_meta,
            **strategy_meta,
        }
        default_filters = defaults.get('filters', {})
        strategy_filters = strategy_config.get('filters', {})
        override_filters = market_overrides.get('filters', {})
        merged['filters'] = {
            **default_filters,
            **strategy_filters,
        }
        for key, value in strategy_filters.items():
            if isinstance(value, dict) and isinstance(default_filters.get(key), dict):
                merged['filters'][key] = {
                    **default_filters[key],
                    **value,
                }
        for key, value in override_filters.items():
            if isinstance(value, dict) and isinstance(merged['filters'].get(key), dict):
                merged['filters'][key] = {
                    **merged['filters'][key],
                    **value,
                }
            else:
                merged['filters'][key] = value
        if market_overrides:
            for key, value in market_overrides.items():
                if key != 'filters':
                    merged[key] = value
        return merged

    def get_threshold(self, strategy_type: int | str, path: str, default=None):
        current = self.get_strategy_config(strategy_type)
        for part in path.split('.'):
            if not isinstance(current, dict):
                return default
            current = current.get(part)
            if current is None:
                return default
        return current
