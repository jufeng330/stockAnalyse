from __future__ import annotations

import logging

import pandas as pd


class FinancialReportFilterService:
    def process_financial_metrics(
        self,
        *,
        df_financial: pd.DataFrame,
        date_financial: str,
        metrics_array: list[list],
        stock_strategy,
        logger: logging.Logger,
        data_type: str = 'continue',
    ) -> tuple[list[set], set]:
        results: list[set] = []
        df_financial = df_financial.copy()
        if '报告期' not in df_financial.columns and '公告日期' in df_financial.columns:
            df_financial['报告期'] = df_financial['公告日期']

        for metric_name, metric_rename, threshold, condition_type in metrics_array:
            if data_type == 'continue':
                stock_set = stock_strategy.get_stock_continue_postive(
                    df_financial=df_financial,
                    date=date_financial,
                    col_name=metric_name,
                    col_adjustment=metric_rename,
                    continue_year=1,
                    threshold=threshold,
                    condition_type=condition_type,
                )
            else:
                stock_set = stock_strategy.get_stock_avg_postive(
                    df_financial=df_financial,
                    date=date_financial,
                    col_name=metric_name,
                    col_adjustment=metric_rename,
                    continue_year=1,
                    threshold=threshold,
                    condition_type=condition_type,
                )
            results.append(stock_set)
            logger.info(f'报表筛选 - {metric_name} 合格股票数量：{len(stock_set)}')

        intersection = set()
        if results:
            intersection = results[0].copy()
            for result in results[1:]:
                intersection &= result
        return results, intersection
