from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from stock_analyse.application.workflows.dividend_analysis_workflow import StockFenHengAnalyser
from stock_analyse.domain.strategies.financial_filter_service import FinancialFilterService

if TYPE_CHECKING:
    from stock_analyse.domain.strategies.stock_select_strategy import StockSelectStrategy


STRATEGY_NAMES = {
    1: '高股息选股策略_1',
    2: '优质股筛选策略_2',
    3: '保守型筛选策略_3',
    4: '成长型筛选策略_4',
    5: '价值型筛选策略_5',
    6: '知名股票筛选策略_6',
}


class SelectionStrategyService:
    def __init__(self) -> None:
        self.financial_filter_service = FinancialFilterService()

    def _create_selector(self, market: str, strategy_type: int):
        from stock_analyse.domain.strategies.stock_select_strategy import StockSelectStrategy

        return StockSelectStrategy(market=market, strategy_type=strategy_type)

    def get_strategy_name(self, strategy_type: int | str) -> str:
        try:
            strategy_type = int(strategy_type)
        except (TypeError, ValueError):
            return f'未知_{strategy_type}'
        return STRATEGY_NAMES.get(strategy_type, f'未知_{strategy_type}')

    def select(self, df_stock, market: str, strategy_type: int = 1, strategy_filter: str = 'avg'):
        selector = self._create_selector(market=market, strategy_type=strategy_type)
        if strategy_type == 1:
            return self.normal_strategy(
                df_stock=df_stock,
                market=market,
                strategy_filter=strategy_filter,
                selector=selector,
            )
        if strategy_type == 2:
            return self.quality_strategy(
                df_stock=df_stock,
                market=market,
                strategy_filter=strategy_filter,
                selector=selector,
            )
        if strategy_type == 3:
            return self.conservative_strategy(
                df_stock=df_stock,
                market=market,
                strategy_filter=strategy_filter,
                selector=selector,
            )
        if strategy_type == 4:
            return self.growth_strategy(
                df_stock=df_stock,
                market=market,
                strategy_filter=strategy_filter,
                selector=selector,
            )
        if strategy_type == 5:
            return self.value_strategy(
                df_stock=df_stock,
                market=market,
                strategy_filter=strategy_filter,
                selector=selector,
            )
        if strategy_type == 6:
            return self.famous_stock_strategy(
                df_stock=df_stock,
                market=market,
                strategy_filter=strategy_filter,
                selector=selector,
            )
        return selector.select_stock(df_stock, strategy_type=strategy_type, strategy_filter=strategy_filter)

    def normal_strategy(
        self,
        df_stock: pd.DataFrame | None,
        market: str,
        strategy_filter: str = 'avg',
        selector: StockSelectStrategy | None = None,
    ) -> pd.DataFrame:
        selector = selector or self._create_selector(market=market, strategy_type=1)
        date = selector.reportUtils.get_current_report_year_st()

        if df_stock is None:
            df_stock_spot = selector.stock.get_stock_border_info()
        else:
            df_stock_spot = df_stock.copy()

        df_stock_spot['代码'] = df_stock_spot['代码'].astype(str)
        selector.logger.info(f"常规策略 - 初始股票数量：{len(df_stock_spot)}")

        if '总市值' in df_stock_spot.columns:
            df_stock_spot = df_stock_spot[df_stock_spot['总市值'] > 100 * 10000 * 10000]

        if '市盈率-动态' in df_stock_spot.columns:
            df_stock_spot = df_stock_spot[df_stock_spot['市盈率-动态'] < 15]
        else:
            if '平均净资产收益率' in df_stock_spot.columns:
                df_stock_spot = df_stock_spot[df_stock_spot['平均净资产收益率'] > 15]
            if '营业总收入同比增长率' in df_stock_spot.columns:
                df_stock_spot = df_stock_spot[df_stock_spot['营业总收入同比增长率'] > 20]

        if '现金分红-股息率' in df_stock_spot.columns:
            df_stock_spot = df_stock_spot[df_stock_spot['现金分红-股息率'] > 0.03]

        if '资产负债率' in df_stock_spot.columns:
            try:
                df = selector.stock_utils.pd_convert_to_float(df_stock_spot, '资产负债率')
                df_stock_spot['资产负债率_%'] = df['资产负债率'].astype(float) * 100
                df_stock_spot = df_stock_spot[df_stock_spot['资产负债率_%'] < 70]
            except Exception as exc:
                selector.logger.error(f"资产负债率转换错误: {exc}")

        selector.logger.info(f"常规策略 - 资产负债率筛选后股票数量：{len(df_stock_spot)}")

        df_financial = selector.stock.get_stock_border_financial_indicator(
            market=market, date=date, df_stock_spot=df_stock_spot
        )

        date_financial = selector.reportUtils.get_report_year_str(days=365 * 3, format='%Y-%m-%d')
        if market == 'H':
            date_financial = selector.reportUtils.get_report_year_str(days=365 * 4, format='%Y-%m-%d')

        set_stocks = set(df_stock_spot['股票代码'].tolist())
        df_filtered = df_stock_spot[df_stock_spot['股票代码'].isin(set_stocks)]
        df_financial_filter = df_financial[df_financial['股票代码'].isin(set_stocks)]

        selector.file_utils.create_middle_file(file_name='常规策略-股票基本信息', df=df_filtered)
        selector.file_utils.create_middle_file(file_name='常规策略-股票财务信息', df=df_financial_filter)

        selector.logger.info(f"策略{selector.strategy_name} - 最终筛选股票数量：{len(df_filtered)}")
        return df_filtered

    def quality_strategy(
        self,
        df_stock: pd.DataFrame | None,
        market: str,
        strategy_filter: str = 'avg',
        selector: StockSelectStrategy | None = None,
    ) -> pd.DataFrame:
        selector = selector or self._create_selector(market=market, strategy_type=2)
        date = selector.reportUtils.get_current_report_year_st()

        if df_stock is None:
            df_stock = selector.stock.get_stock_spot()
        df_stock = df_stock.copy()
        df_stock['代码'] = df_stock['代码'].astype(str)
        selector.logger.info(f"优质股策略 - 初始股票数量：{len(df_stock)}")

        if '总市值' in df_stock.columns:
            df_stock = df_stock[df_stock['总市值'] > 100 * 10000 * 10000]

        if '市盈率-动态' in df_stock.columns:
            df_stock = df_stock[df_stock['市盈率-动态'] < 15]
        else:
            if '平均净资产收益率' in df_stock.columns:
                df_stock = df_stock[df_stock['平均净资产收益率'] > 15]
            if '营业总收入同比增长率' in df_stock.columns:
                df_stock = df_stock[df_stock['营业总收入同比增长率'] > 20]
            if '净利润同比增长率' in df_stock.columns:
                df_stock = df_stock[df_stock['净利润同比增长率'] > 10]

        if '市净率' in df_stock.columns:
            df_stock = df_stock[df_stock['市净率'] < 5]

        if '资产负债率' in df_stock.columns:
            df_stock = selector.stock_utils.pd_convert_to_float(df_stock, '资产负债率')
            df_stock['资产负债率_%'] = df_stock['资产负债率'] * 100
            df_stock = df_stock[df_stock['资产负债率_%'] < 80]

        selector.logger.info(f"优质股策略 - 资产负债率筛选后股票数量：{len(df_stock)}")

        df_financial = selector.stock.get_stock_border_financial_indicator(
            market=market, date=date, df_stock_spot=df_stock
        )
        date_financial = selector.reportUtils.get_report_year_str(days=365 * 3, format='%Y-%m-%d')

        if '营业总收入' in df_financial.columns:
            df_financial = selector.stock_utils.pd_convert_to_float(df_financial, '营业总收入')
            df_financial = df_financial[df_financial['营业总收入'] > 10 * 10000 * 10000]

        set_stocks = self.find_financial_stock_data(
            date_financial=date_financial,
            df_financial=df_financial,
            stock_strategy=selector.stock_strategy,
            market=market,
            logger=selector.logger,
            data_type=strategy_filter,
            threshold_1=5000 * 10000,
            threshold_2=0.05,
            threshold_3=0.05,
        )

        df_filtered = df_stock[df_stock['股票代码'].isin(set_stocks)]
        df_financial_filter = df_financial[df_financial['股票代码'].isin(set_stocks)]

        selector.file_utils.create_middle_file(file_name='优质股策略-股票基本信息', df=df_filtered)
        selector.file_utils.create_middle_file(file_name='优质股策略-股票财务信息', df=df_financial_filter)

        selector.logger.info(f"优质股策略 - 最终筛选股票数量：{len(df_filtered)}")
        return df_filtered

    def conservative_strategy(
        self,
        df_stock: pd.DataFrame | None,
        market: str,
        strategy_filter: str = 'avg',
        selector: StockSelectStrategy | None = None,
    ) -> pd.DataFrame:
        selector = selector or self._create_selector(market=market, strategy_type=3)

        if df_stock is None:
            df_stock = selector.stock.get_stock_spot()
        df_stock = df_stock.copy()
        df_stock['代码'] = df_stock['代码'].astype(str)
        selector.logger.info(f"保守型策略 - 初始股票数量：{len(df_stock)}")

        if '总市值' in df_stock.columns:
            df_stock = df_stock[df_stock['总市值'] > 500 * 10000 * 10000]

        if '市盈率-动态' in df_stock.columns:
            df_stock = df_stock[df_stock['市盈率-动态'] < 15]

        if '资产负债率' in df_stock.columns:
            df_stock = selector.stock_utils.pd_convert_to_float(df_stock, '资产负债率')
            df_stock['资产负债率_%'] = df_stock['资产负债率'] * 100
            df_stock = df_stock[df_stock['资产负债率_%'] < 60]

        fh_service = StockFenHengAnalyser(market=market)
        df_stock_fh, df_fh_summary = fh_service.get_fh_codes(type=strategy_filter, threshold=0.03)
        if df_fh_summary is not None and len(df_fh_summary) > 0:
            col_fh_code = '代码'
            set_fh = set(df_stock_fh[col_fh_code])
            df_stock = df_stock[df_stock['代码'].isin(set_fh)]
            df_stock = df_stock.merge(
                df_fh_summary[[col_fh_code, '平均股息率']],
                left_on='代码',
                right_on=col_fh_code,
                how='left',
            )

        selector.file_utils.create_middle_file(file_name='常规策略-股票基本信息', df=df_stock)
        selector.file_utils.create_middle_file(file_name='常规策略-股票股息率', df=df_stock_fh)

        selector.logger.info(f"保守型策略 - 最终筛选股票数量：{len(df_stock)}")
        return df_stock

    def growth_strategy(
        self,
        df_stock: pd.DataFrame | None,
        market: str,
        strategy_filter: str = 'avg',
        selector: StockSelectStrategy | None = None,
    ) -> pd.DataFrame:
        selector = selector or self._create_selector(market=market, strategy_type=4)

        if df_stock is None:
            df_stock = selector.stock.get_stock_spot()
        df_stock = df_stock.copy()
        df_stock['代码'] = df_stock['代码'].astype(str)
        selector.logger.info(f"成长型策略 - 初始股票数量：{len(df_stock)}")

        date = selector.reportUtils.get_current_report_year_st()
        df_financial = selector.stock.get_stock_border_financial_indicator(
            market=market, date=date, df_stock_spot=df_stock
        )

        date_financial = selector.reportUtils.get_report_year_str(days=365 * 2, format='%Y-%m-%d')

        if '营业总收入' in df_financial.columns:
            df_financial = selector.stock_utils.pd_convert_to_float(df_financial, '营业总收入')
            df_financial = df_financial[df_financial['营业总收入'] > 10 * 10000 * 10000]

        if market == 'usa':
            threshold_profit = 0
            threshold_profit_growth = 20
            threshold_revenue_growth = 30
        else:
            threshold_profit = 0
            threshold_profit_growth = 0.20
            threshold_revenue_growth = 0.30

        set_stocks = self.find_financial_stock_data(
            date_financial=date_financial,
            df_financial=df_financial,
            stock_strategy=selector.stock_strategy,
            market=market,
            logger=selector.logger,
            data_type='avg',
            threshold_1=threshold_profit,
            threshold_2=threshold_profit_growth,
            threshold_3=threshold_revenue_growth,
        )

        df_filtered = df_stock[df_stock['股票代码'].isin(set_stocks)]

        selector.logger.info(f"成长型策略 - 最终筛选股票数量：{len(df_filtered)}")
        return df_filtered

    def value_strategy(
        self,
        df_stock: pd.DataFrame | None,
        market: str,
        strategy_filter: str = 'avg',
        selector: StockSelectStrategy | None = None,
    ) -> pd.DataFrame:
        selector = selector or self._create_selector(market=market, strategy_type=5)

        if df_stock is None:
            df_stock = selector.stock.get_stock_spot()
        df_stock = df_stock.copy()
        df_stock['代码'] = df_stock['代码'].astype(str)
        selector.logger.info(f"价值型策略 - 初始股票数量：{len(df_stock)}")

        if '总市值' in df_stock.columns:
            df_stock = df_stock[df_stock['总市值'] > 500 * 10000 * 10000]

        if '市盈率-动态' in df_stock.columns:
            df_stock = df_stock[df_stock['市盈率-动态'] < 12]

        if '市净率' in df_stock.columns:
            df_stock = df_stock[df_stock['市净率'] < 1.5]

        date = selector.reportUtils.get_current_report_year_st()
        df_financial = selector.stock.get_stock_border_financial_indicator(
            market=market, date=date, df_stock_spot=df_stock
        )

        date_financial = selector.reportUtils.get_report_year_str(days=365 * 3, format='%Y-%m-%d')

        set_stocks = self.find_financial_stock_data(
            date_financial=date_financial,
            df_financial=df_financial,
            stock_strategy=selector.stock_strategy,
            market=market,
            logger=selector.logger,
            data_type='avg',
            threshold_1=0,
            threshold_2=0,
            threshold_3=0,
        )

        if not df_financial.empty and '净资产收益率(%)' in df_financial.columns:
            df_roe = df_financial[['股票代码', '净资产收益率(%)']].dropna()
            df_roe = selector.stock_utils.pd_convert_to_float(df_roe, '净资产收益率(%)')
            df_roe = df_roe[df_roe['净资产收益率(%)'] > 15]
            set_roe = set(df_roe['股票代码'])
            set_stocks = set_stocks & set_roe

        df_filtered = df_stock[df_stock['股票代码'].isin(set_stocks)]

        selector.logger.info(f"价值型策略 - 最终筛选股票数量：{len(df_filtered)}")
        return df_filtered

    def famous_stock_strategy(
        self,
        df_stock: pd.DataFrame | None,
        market: str,
        strategy_filter: str = 'avg',
        selector: StockSelectStrategy | None = None,
    ) -> pd.DataFrame:
        selector = selector or self._create_selector(market=market, strategy_type=6)

        if df_stock is None:
            df_stock = selector.stock.get_stock_spot()
        df_stock = df_stock.copy()
        df_stock['代码'] = df_stock['代码'].astype(str)
        selector.logger.info(f"价值型策略 - 初始股票数量：{len(df_stock)}")

        if '市盈率-动态' in df_stock.columns:
            df_stock = df_stock[df_stock['市盈率-动态'] < 50]

        df_famous_stock = selector.stock.get_famous_stock_info()
        set_famous_stocks = set(df_famous_stock['股票代码'].tolist())

        date = selector.reportUtils.get_current_report_year_st()
        df_financial = selector.stock.get_stock_border_financial_indicator(
            market=market, date=date, df_stock_spot=df_stock
        )

        date_financial = selector.reportUtils.get_report_year_str(days=365 * 3, format='%Y-%m-%d')

        if market == 'usa':
            threshold_profit = 0
            threshold_profit_growth = 5
            threshold_revenue_growth = 10
        else:
            threshold_profit = 0
            threshold_profit_growth = 0.05
            threshold_revenue_growth = 0.05

        set_stocks = self.find_financial_stock_data(
            date_financial=date_financial,
            df_financial=df_financial,
            stock_strategy=selector.stock_strategy,
            market=market,
            logger=selector.logger,
            data_type='avg',
            threshold_1=threshold_profit,
            threshold_2=threshold_profit_growth,
            threshold_3=threshold_revenue_growth,
        )

        set_stocks = set_stocks & set_famous_stocks
        df_filtered = df_stock[df_stock['股票代码'].isin(set_stocks)]

        selector.logger.info(f"价值型策略 - 最终筛选股票数量：{len(df_filtered)}")
        return df_filtered

    def find_financial_stock_data(
        self,
        *,
        date_financial: str,
        df_financial: pd.DataFrame,
        stock_strategy,
        market: str,
        logger,
        data_type: str = 'continue',
        threshold_1: float = 0.0,
        threshold_2: float = 0.0,
        threshold_3: float = 0.0,
    ) -> set:
        return self.financial_filter_service.find_financial_stock_data(
            date_financial=date_financial,
            df_financial=df_financial,
            stock_strategy=stock_strategy,
            market=market,
            logger=logger,
            data_type=data_type,
            threshold_1=threshold_1,
            threshold_2=threshold_2,
            threshold_3=threshold_3,
        )

    def compute_financial_lrl_ratio(
        self,
        df_financial: pd.DataFrame,
        col_lrl: str = '净利润同比增长率',
        col_lr: str = '净利润',
    ) -> pd.DataFrame:
        return self.financial_filter_service.compute_financial_lrl_ratio(
            df_financial=df_financial,
            col_lrl=col_lrl,
            col_lr=col_lr,
        )
