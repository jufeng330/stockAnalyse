from __future__ import annotations

import pandas as pd

from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo


def execute(stock_code: str, market: str, years: int = 10):
    try:
        border_info = stockBorderInfo(market=market)
        current_year = pd.Timestamp.now().year
        zcfz_list = []
        lrb_list = []
        xjll_list = []
        for year in range(current_year - years + 1, current_year + 1):
            report_date = f'{year}0331' if market in ['SH', 'SZ'] else f'{year}1231'
            try:
                zcfz, lrb, xjll = border_info.get_stock_border_report(market=market, date=report_date, indicator='年报')
                if not zcfz.empty:
                    zcfz_stock = zcfz[zcfz['股票代码'] == stock_code]
                    if not zcfz_stock.empty:
                        zcfz_list.append(zcfz_stock)
                if not lrb.empty:
                    lrb_stock = lrb[lrb['股票代码'] == stock_code]
                    if not lrb_stock.empty:
                        lrb_list.append(lrb_stock)
                if not xjll.empty:
                    xjll_stock = xjll[xjll['股票代码'] == stock_code]
                    if not xjll_stock.empty:
                        xjll_list.append(xjll_stock)
            except Exception:
                continue

        zcfz_all = pd.concat(zcfz_list, ignore_index=True) if zcfz_list else pd.DataFrame()
        lrb_all = pd.concat(lrb_list, ignore_index=True) if lrb_list else pd.DataFrame()
        xjll_all = pd.concat(xjll_list, ignore_index=True) if xjll_list else pd.DataFrame()
        return zcfz_all, lrb_all, xjll_all
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
