from __future__ import annotations

import pandas as pd

from stock_analyse.domain.services.dcf_valuation_service import stockDCFSimpleModel
from stock_analyse.infrastructure.data_sources.reports.annual_report_client import stockAnnualReport
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo


class ValuationGateway:
    def get_stock_report(self, market: str, symbol: str, years: int = 5):
        report = stockAnnualReport()
        return report.get_stock_report(stock_code=symbol, market=market, years=years)

    @staticmethod
    def _latest_row(df, symbol: str):
        normalized = df.copy()
        if '股票代码' not in normalized.columns:
            normalized['股票代码'] = symbol
        if '报告日' in normalized.columns:
            normalized = normalized.sort_values('报告日', ascending=False)
        return normalized.head(1).copy()

    def calculate_dcf(self, market: str, symbol: str, cashflow_df, profit_df=None, discount_rate: float = 0.1, growth_rate: float = 0.03):
        model = stockDCFSimpleModel(market=market)
        normalized = self._latest_row(cashflow_df, symbol)
        if '经营活动产生的现金流量净额' in normalized.columns and '经营性现金流-现金流量净额' not in normalized.columns:
            normalized['经营性现金流-现金流量净额'] = normalized['经营活动产生的现金流量净额']
        if profit_df is not None and '归属于母公司所有者的净利润' in profit_df.columns:
            growth = profit_df.sort_values('报告日', ascending=False)['归属于母公司所有者的净利润'].pct_change(-1).iloc[0]
            normalized['净利润同比'] = 0 if pd.isna(growth) else growth * 100
        elif '净利润同比' not in normalized.columns:
            normalized['净利润同比'] = 0
        result = model.calculate_dcf(normalized, discount_rate=discount_rate, growth_rate=growth_rate)
        return float(result.iloc[-1]) if hasattr(result, 'iloc') else float(result)

    def calculate_stock_price_range(self, market: str, symbol: str, zcfz, lrb, xjll):
        model = stockDCFSimpleModel(market=market)
        zcfz_normalized = self._latest_row(zcfz, symbol)
        lrb_normalized = self._latest_row(lrb, symbol)
        xjll_normalized = self._latest_row(xjll, symbol)
        if '负债合计' in zcfz_normalized.columns and '负债-总负债' not in zcfz_normalized.columns:
            zcfz_normalized['负债-总负债'] = zcfz_normalized['负债合计']
        if '货币资金' in zcfz_normalized.columns and '资产-货币资金' not in zcfz_normalized.columns:
            zcfz_normalized['资产-货币资金'] = zcfz_normalized['货币资金']
        if '实收资本(或股本)' in zcfz_normalized.columns and '资产-总股本' not in zcfz_normalized.columns:
            zcfz_normalized['资产-总股本'] = zcfz_normalized['实收资本(或股本)']
        if '名称' in zcfz_normalized.columns and '股票简称' not in zcfz_normalized.columns:
            zcfz_normalized['股票简称'] = zcfz_normalized['名称']
        if '名称' not in zcfz_normalized.columns and '股票简称' not in zcfz_normalized.columns:
            zcfz_normalized['股票简称'] = ''
        if '归属于母公司所有者的净利润' in lrb_normalized.columns and '净利润同比' not in lrb_normalized.columns:
            lrb_normalized['净利润同比'] = 0
        if '经营活动产生的现金流量净额' in xjll_normalized.columns and '经营性现金流-现金流量净额' not in xjll_normalized.columns:
            xjll_normalized['经营性现金流-现金流量净额'] = xjll_normalized['经营活动产生的现金流量净额']
        return model.calculate_stock_price_range(zcfz_normalized, lrb_normalized, xjll_normalized)

    def get_stock_spot(self, market: str):
        border = stockBorderInfo(market=market)
        return border.get_stock_spot()
