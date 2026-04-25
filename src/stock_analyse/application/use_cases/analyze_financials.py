from __future__ import annotations

import logging

import pandas as pd

from stock_analyse.domain.strategies.financial_filter_service import FinancialFilterService
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
from stock_analyse.domain.services.stock_strategy_service import StockStrategy
from stock_analyse.shared.report_date_utils import ReportDateUtils


def _pivot_financial_frame(df_financial_filter: pd.DataFrame) -> pd.DataFrame:
    df_financial_filter_year = df_financial_filter[
        (df_financial_filter['报告期'] > df_financial_filter['报告期'].min())
        & (df_financial_filter['报告期'].astype(str).str.endswith('03-31'))
    ].copy()
    if df_financial_filter_year.empty:
        return pd.DataFrame(columns=['股票代码_'])
    df_pivot = df_financial_filter_year.pivot_table(
        index='股票代码',
        columns='年份',
        values=['净利润', '净利润同比增长率', '营业总收入同比增长率', '净资产收益率', '资产负债率'],
        aggfunc='first',
    ).reset_index()
    df_pivot.columns = ['_'.join(map(str, col)).strip() for col in df_pivot.columns.values]
    sort_columns = [column for column in ['净利润_2025', '净利润_2024'] if column in df_pivot.columns]
    if sort_columns:
        df_pivot = df_pivot.sort_values(by=sort_columns, ascending=False)
    return df_pivot


def execute(
    *,
    market: str,
    strategy_filter: str = 'avg',
    threshold_1: float = 5000 * 10000,
    threshold_2: float = 0.05,
    threshold_3: float = 0.05,
    gateway: stockBorderInfo | None = None,
    filter_service: FinancialFilterService | None = None,
    stock_strategy: StockStrategy | None = None,
    report_utils: ReportDateUtils | None = None,
    logger: logging.Logger | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    gateway = gateway or stockBorderInfo(market=market)
    filter_service = filter_service or FinancialFilterService()
    stock_strategy = stock_strategy or StockStrategy()
    report_utils = report_utils or ReportDateUtils()
    logger = logger or logging.getLogger(__name__)

    date = '20250331'
    df_financial = gateway.get_stock_border_financial_indicator(market=market, date=date)
    date_financial = report_utils.get_report_year_str(days=365 * 3, format='%Y-%m-%d')
    if market == 'H':
        date_financial = report_utils.get_report_year_str(days=365 * 4, format='%Y-%m-%d')

    set_stocks = filter_service.find_financial_stock_data(
        date_financial=date_financial,
        df_financial=df_financial,
        stock_strategy=stock_strategy,
        market=market,
        logger=logger,
        data_type=strategy_filter,
        threshold_1=threshold_1,
        threshold_2=threshold_2,
        threshold_3=threshold_3,
    )
    df_financial_filter = df_financial[df_financial['股票代码'].isin(set_stocks)].copy()
    df_pivot = _pivot_financial_frame(df_financial_filter)
    return df_financial_filter, df_pivot
