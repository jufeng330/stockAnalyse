from __future__ import annotations

# Legacy helper retained for compatibility; current data access should prefer stock_analyse gateways and use cases.
import datetime
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from stock_analyse.application.use_cases import get_stock_financial_report_history as get_stock_financial_report_history_use_case
from stock_analyse.application.use_cases import get_stock_history as get_stock_history_use_case


class StockMain:
    def get_stock_trading_data(self, stock_code, market, days=30):
        try:
            end_date = datetime.datetime.now().strftime('%Y%m%d')
            start_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y%m%d')
            result = get_stock_history_use_case.execute(market=market, symbol=stock_code, start_date=start_date, end_date=end_date)
            if not result.get('success'):
                return pd.DataFrame()
            return pd.DataFrame(result['data']['records'])
        except Exception as exc:
            print(f"获取股票成交数据失败: {str(exc)}")
            return pd.DataFrame()

    def get_stock_financial_reports(self, stock_code, market):
        return get_stock_financial_report_history_use_case.execute(stock_code=stock_code, market=market, years=10)


if __name__ == '__main__':
    stock_main = StockMain()

    print('\n=== 最近30天成交数据 ===')
    trading_data = stock_main.get_stock_trading_data('600519', 'SH', 30)
    print(trading_data.head())

    print('\n=== 最近10年财报数据 ===')
    zcfz, lrb, xjll = stock_main.get_stock_financial_reports('600519', 'SH')
    print('资产负债表:')
    print(zcfz.head())
    print('\n利润表:')
    print(lrb.head())
    print('\n现金流量表:')
    print(xjll.head())
