from __future__ import annotations

import logging

import pandas as pd

from stock_analyse.domain.strategies.financial_report_filter_service import FinancialReportFilterService
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
from stock_analyse.domain.services.stock_strategy_service import StockStrategy
from stock_analyse.shared.report_date_utils import ReportDateUtils


def _build_pivot(df_report_filter: pd.DataFrame, values: list[str], sort_columns: list[str], ascending: bool) -> pd.DataFrame:
    df_financial_filter_year = df_report_filter[(df_report_filter['报告期'] > df_report_filter['报告期'].min())].copy()
    if df_financial_filter_year.empty:
        return pd.DataFrame(columns=['股票代码_'])
    df_financial_filter_year['年份'] = df_financial_filter_year['报告期'].astype(str).str[:4]
    df_pivot = df_financial_filter_year.pivot_table(
        index='股票代码',
        columns='年份',
        values=values,
        aggfunc='first',
    ).reset_index()
    df_pivot.columns = ['_'.join(map(str, col)).strip() for col in df_pivot.columns.values]
    valid_sort_columns = [column for column in sort_columns if column in df_pivot.columns]
    if valid_sort_columns:
        df_pivot = df_pivot.sort_values(by=valid_sort_columns, ascending=ascending)
    return df_pivot


def _process_stock_zcfz(*, date_str: str, df_report: pd.DataFrame, filter_service: FinancialReportFilterService, stock_strategy: StockStrategy, logger: logging.Logger) -> tuple[pd.DataFrame, pd.DataFrame]:
    metrics_array = [
        ['资产负债率', '资产负债率要求', 85, '<'],
        ['资产-总资产', '总资产要求', 10000 * 10000 * 10, '>'],
        ['资产-总资产同比', '总资产同比要求', 5, '>'],
    ]
    _, set_stocks = filter_service.process_financial_metrics(
        df_financial=df_report,
        date_financial=date_str,
        metrics_array=metrics_array,
        stock_strategy=stock_strategy,
        logger=logger,
    )
    df_report_filter = df_report[df_report['股票代码'].isin(set_stocks)].copy()
    df_pivot = _build_pivot(
        df_report_filter,
        ['资产负债率', '资产-总资产', '资产-总资产同比', '负债-总负债', '负债-总负债同比'],
        ['资产负债率_2025'],
        True,
    )
    return df_report_filter, df_pivot


def _process_stock_lrb(*, date_str: str, df_report: pd.DataFrame, filter_service: FinancialReportFilterService, stock_strategy: StockStrategy, logger: logging.Logger) -> tuple[pd.DataFrame, pd.DataFrame]:
    metrics_array = [
        ['净利润同比 ', '净利润同比p要求', 10, '>'],
        ['营业总收入同比', '营业总收入同比要求', 10, '>'],
        ['净利润', '净利润要求', 10000 * 10000, '>'],
        ['营业利润', '营业利润要求', 10000 * 10000, '>'],
    ]
    _, set_stocks = filter_service.process_financial_metrics(
        df_financial=df_report,
        date_financial=date_str,
        metrics_array=metrics_array,
        stock_strategy=stock_strategy,
        logger=logger,
    )
    df_report_filter = df_report[df_report['股票代码'].isin(set_stocks)].copy()
    df_pivot = _build_pivot(
        df_report_filter,
        ['净利润', '营业利润', '营业总收入', '净利润同比', '营业总收入同比'],
        ['净利润_2025'],
        False,
    )
    return df_report_filter, df_pivot


def _process_stock_xjll(*, date_str: str, df_report: pd.DataFrame, filter_service: FinancialReportFilterService, stock_strategy: StockStrategy, logger: logging.Logger) -> tuple[pd.DataFrame, pd.DataFrame]:
    metrics_array = [
        ['净现金流-同比增长  ', '净现金流率要求', 5, '>'],
        ['经营性现金流-净现金流占比', '净现金流占比要求', 50, '>'],
    ]
    _, set_stocks = filter_service.process_financial_metrics(
        df_financial=df_report,
        date_financial=date_str,
        metrics_array=metrics_array,
        stock_strategy=stock_strategy,
        logger=logger,
    )
    df_report_filter = df_report[df_report['股票代码'].isin(set_stocks)].copy()
    df_pivot = _build_pivot(
        df_report_filter,
        ['净现金流-净现金流', '净现金流-同比增长', '经营性现金流-净现金流占比', '投资性现金流-净现金流占比', '融资性现金流-净现金流占比'],
        ['净现金流-净现金流_2025'],
        False,
    )
    return df_report_filter, df_pivot


def execute(
    *,
    market: str,
    strategy_filter: str = 'avg',
    gateway: stockBorderInfo | None = None,
    filter_service: FinancialReportFilterService | None = None,
    stock_strategy: StockStrategy | None = None,
    report_utils: ReportDateUtils | None = None,
    logger: logging.Logger | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    _ = strategy_filter
    gateway = gateway or stockBorderInfo(market=market)
    filter_service = filter_service or FinancialReportFilterService()
    stock_strategy = stock_strategy or StockStrategy()
    report_utils = report_utils or ReportDateUtils()
    logger = logger or logging.getLogger(__name__)

    stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = gateway.get_stock_border_report(market=market, date='20241231', indicator='年报')
    date_str = report_utils.get_report_year_str(days=365 * 3, format='%Y-%m-%d')
    if market == 'H':
        date_str = report_utils.get_report_year_str(days=365 * 4, format='%Y-%m-%d')

    df_report_filter_zcfz, df_pivot_zcfz = _process_stock_zcfz(
        date_str=date_str,
        df_report=stock_zcfz_em_df,
        filter_service=filter_service,
        stock_strategy=stock_strategy,
        logger=logger,
    )
    df_report_filter_lrb, df_pivot_lrb = _process_stock_lrb(
        date_str=date_str,
        df_report=stock_lrb_em_df,
        filter_service=filter_service,
        stock_strategy=stock_strategy,
        logger=logger,
    )
    df_report_filter_xjll, df_pivot_xjll = _process_stock_xjll(
        date_str=date_str,
        df_report=stock_xjll_em_df,
        filter_service=filter_service,
        stock_strategy=stock_strategy,
        logger=logger,
    )

    stock_code_col = '股票代码_'
    set_stocks = set(df_pivot_zcfz.get(stock_code_col, pd.Series(dtype=str)))
    set_stocks &= set(df_pivot_lrb.get(stock_code_col, pd.Series(dtype=str)))
    set_stocks &= set(df_pivot_xjll.get(stock_code_col, pd.Series(dtype=str)))

    df_report_filter_zcfz = df_report_filter_zcfz[df_report_filter_zcfz['股票代码'].isin(set_stocks)]
    df_report_filter_lrb = df_report_filter_lrb[df_report_filter_lrb['股票代码'].isin(set_stocks)]
    df_report_filter_xjll = df_report_filter_xjll[df_report_filter_xjll['股票代码'].isin(set_stocks)]
    df_pivot_zcfz = df_pivot_zcfz[df_pivot_zcfz[stock_code_col].isin(set_stocks)] if stock_code_col in df_pivot_zcfz.columns else df_pivot_zcfz
    df_pivot_lrb = df_pivot_lrb[df_pivot_lrb[stock_code_col].isin(set_stocks)] if stock_code_col in df_pivot_lrb.columns else df_pivot_lrb
    df_pivot_xjll = df_pivot_xjll[df_pivot_xjll[stock_code_col].isin(set_stocks)] if stock_code_col in df_pivot_xjll.columns else df_pivot_xjll

    return df_report_filter_zcfz, df_report_filter_lrb, df_report_filter_xjll, df_pivot_zcfz, df_pivot_lrb, df_pivot_xjll
