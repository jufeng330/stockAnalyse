from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from stock_analyse.application.workflows.dividend_analysis_workflow import StockFenHengAnalyser
from stock_analyse.domain.strategies.financial_filter_service import FinancialFilterService
from stock_analyse.infrastructure.services.futu_market_data_provider import FutuMarketDataProvider
from stock_analyse.infrastructure.config.settings import get_settings

if TYPE_CHECKING:
    from stock_analyse.domain.strategies.stock_select_strategy import StockSelectStrategy


STRATEGY_NAMES = {
    1: '高股息选股策略_1',
    2: '优质股筛选策略_2',
    3: '保守型筛选策略_3',
    4: '成长型筛选策略_4',
    5: '价值型筛选策略_5',
    6: '知名股票筛选策略_6',
    7: '深度价值成长策略_7',
}


class SelectionStrategyService:
    def __init__(self) -> None:
        self.financial_filter_service = FinancialFilterService()

    @staticmethod
    def _convert_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        frame = df.copy()
        for column in columns:
            if column not in frame.columns:
                continue
            series = frame[column]
            if pd.api.types.is_numeric_dtype(series):
                continue
            normalized = (
                series.astype(str)
                .str.replace(',', '', regex=False)
                .str.replace('%', '', regex=False)
                .str.strip()
            )
            frame[column] = pd.to_numeric(normalized, errors='coerce')
        return frame

    def _normalize_spot_filter_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._convert_numeric_columns(
            df,
            [
                '总市值',
                '市盈率-动态',
                '平均净资产收益率',
                '营业总收入同比增长率',
                '净利润同比增长率',
                '现金分红-股息率',
                '资产负债率',
                '市净率',
            ],
        )

    def _normalize_financial_filter_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._convert_numeric_columns(
            df,
            [
                '营业总收入',
                '净资产收益率(%)',
            ],
        )
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
        df_stock = self._apply_market_prefilter(
            df_stock=df_stock,
            market=market,
            strategy_type=strategy_type,
            selector=selector,
        )
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
        if strategy_type == 7:
            return self.deep_value_growth_strategy(
                df_stock=df_stock,
                market=market,
                strategy_filter=strategy_filter,
                selector=selector,
            )
        return selector.select_stock(df_stock, strategy_type=strategy_type, strategy_filter=strategy_filter)

    def _apply_market_prefilter(
        self,
        *,
        df_stock: pd.DataFrame | None,
        market: str,
        strategy_type: int,
        selector: StockSelectStrategy,
        allow_local_fallback: bool = True,
    ) -> pd.DataFrame | None:
        if df_stock is not None:
            return df_stock
        normalized_market = get_settings().market_data.normalize_market(market)
        provider = FutuMarketDataProvider(normalized_market)
        if not provider.should_use_as_prefilter(normalized_market):
            return None
        try:
            strategy_config = selector.get_strategy_config(strategy_type)
            return provider.get_filtered_stock_spot(
                market=normalized_market,
                strategy_type=strategy_type,
                strategy_config=strategy_config,
            )
        except Exception as exc:
            # 即使禁止 fallback，如果是由于网络中断引起的，我们也可以记录警告并返回 None，
            # 让上层尝试使用 get_stock_spot() 等其他本地方法获取。
            selector.logger.warning(
                'Futu stock prefilter failed | market=%s | strategy=%s | error=%s',
                normalized_market,
                strategy_type,
                exc,
            )
            if not allow_local_fallback:
                # 检查是否是严重错误。如果是网络断开，有时返回 None 让上层走本地库可能更稳健
                if "Remote end closed connection" in str(exc) or "Connection aborted" in str(exc):
                    return None
                raise RuntimeError(
                    f'Futu stock prefilter failed for market={normalized_market} strategy={strategy_type}: {str(exc)}'
                ) from exc
            return None

    def get_prefilter_candidates_or_raise(
        self,
        *,
        market: str,
        strategy_type: int,
        strategy_filter: str = 'avg',
    ) -> pd.DataFrame:
        selector = self._create_selector(market=market, strategy_type=strategy_type)
        df_stock = self._apply_market_prefilter(
            df_stock=None,
            market=market,
            strategy_type=strategy_type,
            selector=selector,
            allow_local_fallback=False,
        )
        
        # 优化：如果远程预筛选失败（返回 None），则自动尝试获取本地全量行情作为备选
        if df_stock is None:
            selector.logger.info(f'Remote prefilter failed for {market}, falling back to local full market spot...')
            try:
                df_stock = selector.stock.get_stock_spot()
            except Exception as e:
                selector.logger.error(f'Local fallback also failed: {e}')
                
        if df_stock is None or df_stock.empty:
            # 如果本地也拿不到数据，才抛出错误
            raise RuntimeError(f'No candidates (remote or local) for market={market} strategy={strategy_type}')
            
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
        market_cap_min = selector.get_threshold(1, 'filters.spot.market_cap_min', 100 * 10000 * 10000)
        pe_dynamic_max = selector.get_threshold(1, 'filters.spot.pe_dynamic_max', 15)
        fallback_roe_avg_min = selector.get_threshold(1, 'filters.spot.fallback_roe_avg_min', 15)
        fallback_revenue_growth_min = selector.get_threshold(1, 'filters.spot.fallback_revenue_growth_min', 20)
        dividend_mode = selector.get_threshold(1, 'filters.dividend.mode', strategy_filter)
        dividend_yield_min = selector.get_threshold(1, 'filters.dividend.dividend_yield_min', 0.03)
        min_dividend_years = selector.get_threshold(1, 'filters.dividend.min_dividend_years', 3)
        debt_ratio_max = selector.get_threshold(1, 'filters.risk.debt_ratio_max', 70)
        lookback_years = selector.get_threshold(1, 'filters.financial.lookback_years', 3)
        threshold_profit = selector.get_threshold(1, 'filters.financial.profit_min', 0)
        threshold_profit_growth = selector.get_threshold(1, 'filters.financial.profit_growth_min', 0)
        threshold_revenue_growth = selector.get_threshold(1, 'filters.financial.revenue_growth_min', 0)

        if df_stock is None:
            prefetched_df = self._apply_market_prefilter(df_stock=None, market=market, strategy_type=1, selector=selector)
            if prefetched_df is None:
                df_stock_spot = selector.stock.get_stock_spot()
            else:
                df_stock_spot = prefetched_df.copy()
        else:
            df_stock_spot = df_stock.copy()

        df_stock_spot['代码'] = df_stock_spot['代码'].astype(str)
        df_stock_spot = self._normalize_spot_filter_columns(df_stock_spot)
        selector.logger.info(f"常规策略 - 初始股票数量：{len(df_stock_spot)}")

        if '总市值' in df_stock_spot.columns:
            df_stock_spot = df_stock_spot[df_stock_spot['总市值'] > market_cap_min]

        if '市盈率-动态' in df_stock_spot.columns:
            df_stock_spot = df_stock_spot[df_stock_spot['市盈率-动态'] < pe_dynamic_max]
        else:
            if '平均净资产收益率' in df_stock_spot.columns:
                df_stock_spot = df_stock_spot[df_stock_spot['平均净资产收益率'] > fallback_roe_avg_min]
            if '营业总收入同比增长率' in df_stock_spot.columns:
                df_stock_spot = df_stock_spot[df_stock_spot['营业总收入同比增长率'] > fallback_revenue_growth_min]

        if '现金分红-股息率' in df_stock_spot.columns:
            df_stock_spot = df_stock_spot[df_stock_spot['现金分红-股息率'] > dividend_yield_min]

        if '资产负债率' in df_stock_spot.columns:
            try:
                df = selector.stock_utils.pd_convert_to_float(df_stock_spot, '资产负债率')
                df_stock_spot['资产负债率_%'] = df['资产负债率'].astype(float) * 100
                df_stock_spot = df_stock_spot[df_stock_spot['资产负债率_%'] < debt_ratio_max]
            except Exception as exc:
                selector.logger.error(f"资产负债率转换错误: {exc}")

        selector.logger.info(f"常规策略 - 资产负债率筛选后股票数量：{len(df_stock_spot)}")

        df_financial = selector.stock.get_stock_border_financial_indicator(
            market=market, date=date, df_stock_spot=df_stock_spot
        )
        date_financial = selector.reportUtils.get_report_year_str(days=365 * lookback_years, format='%Y-%m-%d')
        set_stocks = set(df_stock_spot['股票代码'].tolist())

        if df_financial is not None and not df_financial.empty:
            df_financial = self._normalize_financial_filter_columns(df_financial)
            set_financial_stocks = self.find_financial_stock_data(
                date_financial=date_financial,
                df_financial=df_financial,
                stock_strategy=selector.stock_strategy,
                market=market,
                logger=selector.logger,
                data_type=strategy_filter,
                threshold_1=threshold_profit,
                threshold_2=threshold_profit_growth,
                threshold_3=threshold_revenue_growth,
            )
            set_stocks = set_stocks & set_financial_stocks

        df_stock_fh = pd.DataFrame()
        df_fh_summary = pd.DataFrame()
        col_fh_code = '代码'
        normalized_market = get_settings().market_data.normalize_market(market)
        if normalized_market in {'SH', 'SZ'}:
            fh_service = StockFenHengAnalyser(market=market)
            df_stock_fh, df_fh_summary = fh_service.get_fh_codes(
                type=dividend_mode,
                min_years=min_dividend_years,
                threshold=dividend_yield_min,
            )
            if df_fh_summary is not None and not df_fh_summary.empty:
                set_fh = set(df_fh_summary[col_fh_code].astype(str))
                set_stocks = set_stocks & set_fh

        df_filtered = df_stock_spot[df_stock_spot['股票代码'].isin(set_stocks)]
        df_financial_filter = df_financial[df_financial['股票代码'].isin(set_stocks)] if df_financial is not None and not df_financial.empty else pd.DataFrame()

        if df_fh_summary is not None and not df_fh_summary.empty:
            df_filtered = df_filtered.merge(
                df_fh_summary[[col_fh_code, '平均股息率']],
                left_on='代码',
                right_on=col_fh_code,
                how='left',
            )
        elif '上一财年股息率' in df_filtered.columns and '平均股息率' not in df_filtered.columns:
            df_filtered['平均股息率'] = df_filtered['上一财年股息率']
        elif '股息率-TTM' in df_filtered.columns and '平均股息率' not in df_filtered.columns:
            df_filtered['平均股息率'] = df_filtered['股息率-TTM']
        elif '现金分红-股息率' in df_filtered.columns and '平均股息率' not in df_filtered.columns:
            df_filtered['平均股息率'] = df_filtered['现金分红-股息率']
        if '平均股息率' in df_filtered.columns:
            dividend_values = pd.to_numeric(df_filtered['平均股息率'], errors='coerce')
            df_filtered = df_filtered[dividend_values >= dividend_yield_min]
            df_filtered = df_filtered.copy()
            df_filtered['平均股息率'] = dividend_values.loc[df_filtered.index]
        elif normalized_market not in {'SH', 'SZ'}:
            selector.logger.info('常规策略 - 股息率字段缺失，跳过非A股股息筛选 | market=%s', normalized_market)

        selector.file_utils.create_middle_file(file_name='常规策略-股票基本信息', df=df_filtered)
        selector.file_utils.create_middle_file(file_name='常规策略-股票财务信息', df=df_financial_filter)
        selector.file_utils.create_middle_file(file_name='常规策略-股票股息率', df=df_stock_fh)

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
        market_cap_min = selector.get_threshold(2, 'filters.spot.market_cap_min', 100 * 10000 * 10000)
        pe_dynamic_max = selector.get_threshold(2, 'filters.spot.pe_dynamic_max', 15)
        fallback_roe_avg_min = selector.get_threshold(2, 'filters.spot.fallback_roe_avg_min', 15)
        fallback_revenue_growth_min = selector.get_threshold(2, 'filters.spot.fallback_revenue_growth_min', 20)
        fallback_net_profit_growth_min = selector.get_threshold(2, 'filters.spot.fallback_net_profit_growth_min', 10)
        pb_max = selector.get_threshold(2, 'filters.spot.pb_max', 5)
        revenue_total_min = selector.get_threshold(2, 'filters.spot.revenue_total_min', 10 * 10000 * 10000)
        debt_ratio_max = selector.get_threshold(2, 'filters.risk.debt_ratio_max', 80)
        lookback_years = selector.get_threshold(2, 'filters.financial.lookback_years', 3)
        threshold_profit = selector.get_threshold(2, 'filters.financial.profit_min', 5000 * 10000)
        threshold_profit_growth = selector.get_threshold(2, 'filters.financial.profit_growth_min', 0.05)
        threshold_revenue_growth = selector.get_threshold(2, 'filters.financial.revenue_growth_min', 0.05)

        if df_stock is None:
            prefetched_df = self._apply_market_prefilter(df_stock=None, market=market, strategy_type=2, selector=selector)
            if prefetched_df is None:
                df_stock = selector.stock.get_stock_spot()
            else:
                df_stock = prefetched_df
        df_stock = df_stock.copy()
        df_stock['代码'] = df_stock['代码'].astype(str)
        df_stock = self._normalize_spot_filter_columns(df_stock)
        selector.logger.info(f"优质股策略 - 初始股票数量：{len(df_stock)}")

        if '总市值' in df_stock.columns:
            df_stock = df_stock[df_stock['总市值'] > market_cap_min]

        if '市盈率-动态' in df_stock.columns:
            df_stock = df_stock[df_stock['市盈率-动态'] < pe_dynamic_max]
        else:
            if '平均净资产收益率' in df_stock.columns:
                df_stock = df_stock[df_stock['平均净资产收益率'] > fallback_roe_avg_min]
            if '营业总收入同比增长率' in df_stock.columns:
                df_stock = df_stock[df_stock['营业总收入同比增长率'] > fallback_revenue_growth_min]
            if '净利润同比增长率' in df_stock.columns:
                df_stock = df_stock[df_stock['净利润同比增长率'] > fallback_net_profit_growth_min]

        if '市净率' in df_stock.columns:
            df_stock = df_stock[df_stock['市净率'] < pb_max]

        if '资产负债率' in df_stock.columns:
            df_stock = selector.stock_utils.pd_convert_to_float(df_stock, '资产负债率')
            df_stock['资产负债率_%'] = df_stock['资产负债率'] * 100
            df_stock = df_stock[df_stock['资产负债率_%'] < debt_ratio_max]

        selector.logger.info(f"优质股策略 - 资产负债率筛选后股票数量：{len(df_stock)}")

        df_financial = selector.stock.get_stock_border_financial_indicator(
            market=market, date=date, df_stock_spot=df_stock
        )
        date_financial = selector.reportUtils.get_report_year_str(days=365 * lookback_years, format='%Y-%m-%d')

        df_financial = self._normalize_financial_filter_columns(df_financial)
        if '营业总收入' in df_financial.columns:
            df_financial = selector.stock_utils.pd_convert_to_float(df_financial, '营业总收入')
            df_financial = df_financial[df_financial['营业总收入'] > revenue_total_min]

        set_stocks = self.find_financial_stock_data(
            date_financial=date_financial,
            df_financial=df_financial,
            stock_strategy=selector.stock_strategy,
            market=market,
            logger=selector.logger,
            data_type=strategy_filter,
            threshold_1=threshold_profit,
            threshold_2=threshold_profit_growth,
            threshold_3=threshold_revenue_growth,
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
        market_cap_min = selector.get_threshold(3, 'filters.spot.market_cap_min', 500 * 10000 * 10000)
        pe_dynamic_max = selector.get_threshold(3, 'filters.spot.pe_dynamic_max', 15)
        debt_ratio_max = selector.get_threshold(3, 'filters.risk.debt_ratio_max', 60)
        dividend_mode = selector.get_threshold(3, 'filters.dividend.mode', strategy_filter)
        dividend_yield_min = selector.get_threshold(3, 'filters.dividend.dividend_yield_min', 0.03)
        min_dividend_years = selector.get_threshold(3, 'filters.dividend.min_dividend_years', 5)

        if df_stock is None:
            prefetched_df = self._apply_market_prefilter(df_stock=None, market=market, strategy_type=3, selector=selector)
            if prefetched_df is None:
                df_stock = selector.stock.get_stock_spot()
            else:
                df_stock = prefetched_df
        df_stock = df_stock.copy()
        df_stock['代码'] = df_stock['代码'].astype(str)
        selector.logger.info(f"保守型策略 - 初始股票数量：{len(df_stock)}")

        if '总市值' in df_stock.columns:
            df_stock = df_stock[df_stock['总市值'] > market_cap_min]

        if '市盈率-动态' in df_stock.columns:
            df_stock = df_stock[df_stock['市盈率-动态'] < pe_dynamic_max]

        if '资产负债率' in df_stock.columns:
            df_stock = selector.stock_utils.pd_convert_to_float(df_stock, '资产负债率')
            df_stock['资产负债率_%'] = df_stock['资产负债率'] * 100
            df_stock = df_stock[df_stock['资产负债率_%'] < debt_ratio_max]

        fh_service = StockFenHengAnalyser(market=market)
        df_stock_fh, df_fh_summary = fh_service.get_fh_codes(
            type=dividend_mode,
            min_years=min_dividend_years,
            threshold=dividend_yield_min,
        )
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
        revenue_total_min = selector.get_threshold(4, 'filters.spot.revenue_total_min', 10 * 10000 * 10000)
        lookback_years = selector.get_threshold(4, 'filters.financial.lookback_years', 2)
        financial_mode = selector.get_threshold(4, 'filters.financial.mode', 'avg')
        threshold_profit = selector.get_threshold(4, 'filters.financial.profit_min', 0)
        threshold_profit_growth = selector.get_threshold(4, 'filters.financial.profit_growth_min', 0.20)
        threshold_revenue_growth = selector.get_threshold(4, 'filters.financial.revenue_growth_min', 0.30)

        if df_stock is None:
            prefetched_df = self._apply_market_prefilter(df_stock=None, market=market, strategy_type=4, selector=selector)
            if prefetched_df is None:
                df_stock = selector.stock.get_stock_spot()
            else:
                df_stock = prefetched_df
        df_stock = df_stock.copy()
        df_stock['代码'] = df_stock['代码'].astype(str)
        selector.logger.info(f"成长型策略 - 初始股票数量：{len(df_stock)}")

        date = selector.reportUtils.get_current_report_year_st()
        df_financial = selector.stock.get_stock_border_financial_indicator(
            market=market, date=date, df_stock_spot=df_stock
        )

        date_financial = selector.reportUtils.get_report_year_str(days=365 * lookback_years, format='%Y-%m-%d')

        df_financial = self._normalize_financial_filter_columns(df_financial)
        if '营业总收入' in df_financial.columns:
            df_financial = selector.stock_utils.pd_convert_to_float(df_financial, '营业总收入')
            df_financial = df_financial[df_financial['营业总收入'] > revenue_total_min]

        set_stocks = self.find_financial_stock_data(
            date_financial=date_financial,
            df_financial=df_financial,
            stock_strategy=selector.stock_strategy,
            market=market,
            logger=selector.logger,
            data_type=financial_mode,
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
        market_cap_min = selector.get_threshold(5, 'filters.spot.market_cap_min', 500 * 10000 * 10000)
        pe_dynamic_max = selector.get_threshold(5, 'filters.spot.pe_dynamic_max', 12)
        pb_max = selector.get_threshold(5, 'filters.spot.pb_max', 1.5)
        financial_mode = selector.get_threshold(5, 'filters.financial.mode', 'avg')
        lookback_years = selector.get_threshold(5, 'filters.financial.lookback_years', 3)
        threshold_profit = selector.get_threshold(5, 'filters.financial.profit_min', 0)
        threshold_profit_growth = selector.get_threshold(5, 'filters.financial.profit_growth_min', 0)
        threshold_revenue_growth = selector.get_threshold(5, 'filters.financial.revenue_growth_min', 0)
        roe_min = selector.get_threshold(5, 'filters.financial.roe_min', 15)

        if df_stock is None:
            prefetched_df = self._apply_market_prefilter(df_stock=None, market=market, strategy_type=5, selector=selector)
            if prefetched_df is None:
                df_stock = selector.stock.get_stock_spot()
            else:
                df_stock = prefetched_df
        df_stock = df_stock.copy()
        if df_stock.empty:
            selector.logger.info('价值型策略 - 初始股票数量：0')
            return df_stock
        df_stock['代码'] = df_stock['代码'].astype(str)
        selector.logger.info(f"价值型策略 - 初始股票数量：{len(df_stock)}")

        if '总市值' in df_stock.columns:
            df_stock = df_stock[df_stock['总市值'] > market_cap_min]

        if '市盈率-动态' in df_stock.columns:
            df_stock = df_stock[df_stock['市盈率-动态'] < pe_dynamic_max]

        if '市净率' in df_stock.columns:
            df_stock = df_stock[df_stock['市净率'] < pb_max]

        date = selector.reportUtils.get_current_report_year_st()
        df_financial = selector.stock.get_stock_border_financial_indicator(
            market=market, date=date, df_stock_spot=df_stock
        )

        date_financial = selector.reportUtils.get_report_year_str(days=365 * lookback_years, format='%Y-%m-%d')

        set_stocks = self.find_financial_stock_data(
            date_financial=date_financial,
            df_financial=df_financial,
            stock_strategy=selector.stock_strategy,
            market=market,
            logger=selector.logger,
            data_type=financial_mode,
            threshold_1=threshold_profit,
            threshold_2=threshold_profit_growth,
            threshold_3=threshold_revenue_growth,
        )

        if not df_financial.empty and '净资产收益率(%)' in df_financial.columns:
            df_roe = df_financial[['股票代码', '净资产收益率(%)']].dropna()
            df_roe = selector.stock_utils.pd_convert_to_float(df_roe, '净资产收益率(%)')
            df_roe = df_roe[df_roe['净资产收益率(%)'] > roe_min]
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
        pe_dynamic_max = selector.get_threshold(6, 'filters.spot.pe_dynamic_max', 50)
        financial_mode = selector.get_threshold(6, 'filters.financial.mode', 'avg')
        lookback_years = selector.get_threshold(6, 'filters.financial.lookback_years', 3)
        threshold_profit = selector.get_threshold(6, 'filters.financial.profit_min', 0)
        threshold_profit_growth = selector.get_threshold(6, 'filters.financial.profit_growth_min', 0.05)
        threshold_revenue_growth = selector.get_threshold(6, 'filters.financial.revenue_growth_min', 0.05)

        if df_stock is None:
            prefetched_df = self._apply_market_prefilter(df_stock=None, market=market, strategy_type=6, selector=selector)
            if prefetched_df is None:
                df_stock = selector.stock.get_stock_spot()
            else:
                df_stock = prefetched_df
        df_stock = df_stock.copy()
        df_stock['代码'] = df_stock['代码'].astype(str)
        selector.logger.info(f"价值型策略 - 初始股票数量：{len(df_stock)}")

        if '市盈率-动态' in df_stock.columns:
            df_stock = df_stock[df_stock['市盈率-动态'] < pe_dynamic_max]

        df_famous_stock = selector.stock.get_famous_stock_info()
        set_famous_stocks = set(df_famous_stock['股票代码'].tolist())

        date = selector.reportUtils.get_current_report_year_st()
        df_financial = selector.stock.get_stock_border_financial_indicator(
            market=market, date=date, df_stock_spot=df_stock
        )

        date_financial = selector.reportUtils.get_report_year_str(days=365 * lookback_years, format='%Y-%m-%d')

        set_stocks = self.find_financial_stock_data(
            date_financial=date_financial,
            df_financial=df_financial,
            stock_strategy=selector.stock_strategy,
            market=market,
            logger=selector.logger,
            data_type=financial_mode,
            threshold_1=threshold_profit,
            threshold_2=threshold_profit_growth,
            threshold_3=threshold_revenue_growth,
        )

        set_stocks = set_stocks & set_famous_stocks
        df_filtered = df_stock[df_stock['股票代码'].isin(set_stocks)]

        selector.logger.info(f"价值型策略 - 最终筛选股票数量：{len(df_filtered)}")
        return df_filtered

    def deep_value_growth_strategy(
        self,
        df_stock: pd.DataFrame | None,
        market: str,
        strategy_filter: str = 'avg',
        selector: StockSelectStrategy | None = None,
    ) -> pd.DataFrame:
        """实现深度价值成长策略_7。
        
        逻辑：
        1. 基础筛选：排除垃圾股，选择主板优质标的。
        2. 财务计算：利用 StockStrategy.calculate_stock_data 获取分类、阶段和分区。
        3. 评分系统：调用 StockStrategy.calculate_score (新矩阵计分逻辑)。
        """
        selector = selector or self._create_selector(market=market, strategy_type=7)
        
        # 1. 获取初始股票池
        if df_stock is None:
            prefetched_df = self._apply_market_prefilter(df_stock=None, market=market, strategy_type=7, selector=selector)
            if prefetched_df is None:
                df_stock = selector.stock.get_stock_spot()
            else:
                df_stock = prefetched_df
        df_stock = df_stock.copy()
        df_stock['代码'] = df_stock['代码'].astype(str)
        df_stock = self._normalize_spot_filter_columns(df_stock)
        
        selector.logger.info(f"深度价值成长策略_7 - 初始股票数量：{len(df_stock)}")

        # 2. 逐个计算基本面分类和评分
        results = []
        
        # 兼容性处理：寻找 PE 和 市值相关的列
        def _get_frame_val(row, keys, default=0):
            for k in keys:
                if k in row.index and pd.notna(row[k]):
                    try:
                        return float(str(row[k]).replace(',', ''))
                    except: continue
            return default

        # 预先获取可能的财务数据 (为了计算百分位)
        # 为了性能，我们在循环外部不获取全量，而是在循环内部按需获取或批量获取（如果支持）
        
        selector.logger.info(f"深度价值成长策略_7 - 开始对股票池进行深度评分...")

        for _, row in df_stock.iterrows():
            stock_code = row['代码']
            
            # 基础初筛逻辑移入循环或使用兼容列名
            pe_val = _get_frame_val(row, ['市盈率-动态', '市盈率', 'PE'], -1)
            mkt_cap = _get_frame_val(row, ['总市值', '市值'], 0)
            
            # 初筛：排除 PE 无效或市值过小的
            if pe_val <= 0 or pe_val > 60 or mkt_cap < 10 * 1e8:
                continue
                
            try:
                # 转换 row 为 calculate_stock_data 期望的格式
                s_data = row.copy()
                s_data['market'] = market
                
                # 获取该股票的历史财报数据（用于计算 PE 分位）
                df_financial = selector.stock.get_stock_border_financial_indicator(
                    market=market, df_stock_spot=pd.DataFrame([row])
                )
                
                # A. 计算分类 (股票类型、五阶段、四区)
                df_analysis = selector.stock_strategy.calculate_stock_data(
                    df_history_data=None, 
                    df_stock_data=s_data,
                    stock_code=stock_code,
                    df_financial=df_financial
                )
                
                if df_analysis.empty:
                    continue
                
                # B. 调用最新的 calculate_score 逻辑
                score, signal_msg = selector.stock_strategy.calculate_score(
                    df_history_data=pd.DataFrame(), 
                    df_stock=pd.DataFrame([row]),
                    df_summary_data=df_analysis
                )
                
                # C. 收集结果
                res_row = df_analysis.iloc[0].to_dict()
                res_row['score'] = score
                res_row['signal'] = signal_msg
                # 显式映射列名，以兼容全盘扫描工作流的要求
                res_row['代码'] = res_row.get('stock_code', stock_code)
                res_row['名称'] = res_row.get('stock_name', row.get('名称', ''))
                results.append(res_row)
            except Exception as e:
                selector.logger.debug(f"评分跳过 {stock_code}: {str(e)}")
                continue


        if not results:
            return pd.DataFrame()

        df_result = pd.DataFrame(results)
        
        # 3. 筛选和排序
        # 排除透支期和垃圾股 (已经在评分逻辑中得分为0)
        df_result = df_result[df_result['score'] > 0]
        df_result = df_result.sort_values(by='score', ascending=False)
        
        selector.logger.info(f"深度价值成长策略_7 - 最终选出股票数量：{len(df_result)}")
        return df_result


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
