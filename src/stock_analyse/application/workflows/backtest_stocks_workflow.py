from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo


class BacktestStocksWorkflow:
    def __init__(self) -> None:
        pass

    def run(self, *, market: str, high_score_stocks: list[dict], analysis_date: str):
        analysis_date_value = pd.to_datetime(analysis_date)
        df_high_score_stocks = pd.DataFrame(high_score_stocks)
        for i in range(1, 4):
            df_high_score_stocks[f'day_{i}_return'] = 0.0
            df_high_score_stocks[f'day_{i}_is_up'] = None

        for idx, row in df_high_score_stocks.iterrows():
            stock_code = row['股票代码']
            current_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
            end_date = current_date
            stock_company = stockCompanyInfo(marker=market, symbol=stock_code)
            try:
                stock_data = stock_company.get_stock_history_data(
                    start_date_str=start_date,
                    end_date_str=end_date,
                )
                if stock_data is None or stock_data.empty:
                    continue
                stock_data['日期'] = pd.to_datetime(stock_data['日期']).dt.date
                eligible_data = stock_data[stock_data['日期'].apply(lambda x: x >= analysis_date_value.date())]
                if eligible_data.empty:
                    continue
                first_trade_date = eligible_data['日期'].iloc[0]
                col_price = '收盘'
                future_data = stock_data[stock_data['日期'].apply(lambda x: x >= first_trade_date)]
                analysis_price = float(future_data[col_price].iloc[0])
                for i in range(1, 4):
                    target_date = first_trade_date + pd.Timedelta(days=1)
                    future_data = stock_data[stock_data['日期'].apply(lambda x: x >= target_date)]
                    if not future_data.empty:
                        future_price = float(future_data.iloc[0][col_price])
                        price_change_pct = (future_price - analysis_price) / analysis_price * 100
                        is_up = price_change_pct > 0
                        df_high_score_stocks.at[idx, f'day_{i}_return'] = price_change_pct
                        df_high_score_stocks.at[idx, f'day_{i}_is_up'] = is_up
                        first_trade_date = future_data.iloc[0]['日期']
                    else:
                        df_high_score_stocks.at[idx, f'day_{i}_return'] = None
                        df_high_score_stocks.at[idx, f'day_{i}_is_up'] = None
            except Exception:
                continue

        stats = self.generate_statistics_report(df_high_score_stocks)
        stats_s1 = self.generate_statistics_report(df_high_score_stocks, recommendation_type='强烈推荐买入')
        stats_s2 = self.generate_statistics_report(df_high_score_stocks, recommendation_type='建议买入')
        stats_result = f'整体统计信息:\n {stats} 强烈推荐买入统计信息:{stats_s1}\n 建议买入统计信息{stats_s2}'
        return df_high_score_stocks, stats_result

    def generate_statistics_report(self, df_result: pd.DataFrame, recommendation_type: str = 'all') -> str:
        if df_result.empty:
            return '没有可用的回测数据'
        if recommendation_type == 'all':
            df = df_result
        else:
            df = df_result[df_result['投资建议'] == recommendation_type]
        report = '===== 股票回测统计报告 =====\n\n'
        for i in range(1, 4):
            day_return_col = f'day_{i}_return'
            day_is_up_col = f'day_{i}_is_up'
            if day_return_col not in df.columns or day_is_up_col not in df.columns:
                continue
            avg_return = df[day_return_col].mean()
            up_count = len(df[df[day_is_up_col] == True])
            down_count = len(df[df[day_is_up_col] == False])
            total_count = len(df)
            if total_count > 0:
                report += f'第{i}天统计:\n'
                report += f'  平均涨跌幅: {avg_return:.2f}%\n'
                report += f'  上涨数量: {up_count} ({up_count / total_count * 100:.2f}%)\n'
                report += f'  下跌数量: {down_count} ({down_count / total_count * 100:.2f}%)\n\n'
            else:
                report += f'第{i}天统计:\n'
                report += '  总数量: 0\n'
        return report
