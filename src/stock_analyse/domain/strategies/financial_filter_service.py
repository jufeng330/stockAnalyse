from __future__ import annotations

import logging

import numpy as np
import pandas as pd


class FinancialFilterService:
    def find_financial_stock_data(
        self,
        *,
        date_financial: str,
        df_financial: pd.DataFrame,
        stock_strategy,
        market: str,
        logger: logging.Logger,
        data_type: str = 'continue',
        threshold_1: float = 0.0,
        threshold_2: float = 0.0,
        threshold_3: float = 0.0,
    ) -> set:
        if data_type == 'continue':
            col_lrl = '净利润'
            col_lrl_rename = '全年利润率为正'
            set_stocks_lrl = stock_strategy.get_stock_continue_postive(
                df_financial, date_financial, col_lrl, col_lrl_rename, threshold_1
            )
            logger.info(f"财务筛选 - {col_lrl} 合格股票数量：{len(set_stocks_lrl)}")

            col_lrl = '净利润同比增长率'
            col_lrl_rename = '利润率同比为正'
            set_stocks_lrl_ratio = stock_strategy.get_stock_continue_postive(
                df_financial, date_financial, col_lrl, col_lrl_rename, threshold_2
            )
            logger.info(f"财务筛选 - {col_lrl} 合格股票数量：{len(set_stocks_lrl_ratio)}")

            col_lrl = '营业总收入同比增长率'
            col_lrl_rename = '全年业务收入增长率为正'
            set_stocks_yy = stock_strategy.get_stock_continue_postive(
                df_financial, date_financial, col_lrl, col_lrl_rename, threshold_3
            )
            logger.info(f"财务筛选 - {col_lrl} 合格股票数量：{len(set_stocks_yy)}")
        else:
            col_lrl = '净利润'
            col_lrl_rename = '全年利润率为正'
            set_stocks_lrl = stock_strategy.get_stock_avg_postive(
                df_financial, date_financial, col_lrl, col_lrl_rename, threshold_1
            )
            logger.info(f"财务筛选 - {col_lrl} 合格股票数量：{len(set_stocks_lrl)}")

            col_lrl = '净利润同比增长率'
            if market == 'usa' and col_lrl not in df_financial.columns:
                df_financial = self.compute_financial_lrl_ratio(df_financial, col_lrl)
            col_lrl_rename = '利润率同比为正'
            set_stocks_lrl_ratio = stock_strategy.get_stock_avg_postive(
                df_financial, date_financial, col_lrl, col_lrl_rename, threshold_2
            )
            logger.info(f"财务筛选 - {col_lrl} 合格股票数量：{len(set_stocks_lrl_ratio)}")

            col_lrl = '营业总收入同比增长率'
            col_lrl_rename = '全年业务收入增长率为正'
            set_stocks_yy = stock_strategy.get_stock_avg_postive(
                df_financial, date_financial, col_lrl, col_lrl_rename, threshold_3
            )
            logger.info(f"财务筛选 - {col_lrl} 合格股票数量：{len(set_stocks_yy)}")

        set_stocks = set_stocks_lrl & set_stocks_yy & set_stocks_lrl_ratio
        logger.info(f"财务筛选 - 最终合格股票数量：{len(set_stocks)}")
        return set_stocks

    def compute_financial_lrl_ratio(
        self,
        df_financial: pd.DataFrame,
        col_lrl: str = '净利润同比增长率',
        col_lr: str = '净利润',
    ) -> pd.DataFrame:
        if '年份' not in df_financial.columns:
            raise ValueError("数据框必须包含'年份'列")
        df_financial = df_financial.sort_values(by='年份').reset_index(drop=True)

        prev_col = f'上一年{col_lr}'
        df_financial[prev_col] = df_financial[col_lr].shift(1)
        growth = (df_financial[col_lr] - df_financial[prev_col]) / df_financial[prev_col] * 100

        mask_no_prev = pd.isna(df_financial[prev_col])
        mask_prev_zero = df_financial[prev_col] == 0

        growth[mask_no_prev] = np.nan
        growth[mask_prev_zero & (df_financial[col_lr] == 0)] = np.nan
        growth[mask_prev_zero & (df_financial[col_lr] > 0)] = np.inf
        growth[mask_prev_zero & (df_financial[col_lr] < 0)] = -np.inf

        df_financial[col_lrl] = growth
        return df_financial.drop(columns=[prev_col])
