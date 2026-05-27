import sys
import os
import pandas as pd
import numpy as np

sys.path.append('/mnt/github/stock/stockAnalyse/src')

from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
from stock_analyse.domain.services.stock_strategy_service import StockStrategy


def backfill_from_financial(row, df_financial):
    s_data = row.copy()
    if df_financial is None or df_financial.empty:
        return s_data
    latest_fin = df_financial.iloc[0]
    field_map = {
        'ROE': ['roe', 'ROE', '净资产收益率', '平均净资产收益率'],
        '净利润同比增长率': ['net_profit_growth', '利润增长率', '净利润同比增长率'],
        '营业总收入同比增长率': ['revenue_growth', '营收增长率', '营业总收入同比增长率'],
        '资产负债率': ['debt_ratio', '负债率', '资产负债率'],
        'PE_TTM': ['PE_TTM', 'pe_ttm', 'pe', 'PE', '市盈率-TTM', '市盈率'],
    }
    for target, sources in field_map.items():
        for src in sources:
            if src in latest_fin.index and pd.notna(latest_fin[src]):
                s_data[target] = latest_fin[src]
                break
    return s_data


def verify_market(market, samples):
    print(f"\n===== Market: {market} =====")
    service = stockBorderInfo(market=market)
    strategy = StockStrategy()
    rows = []

    for sample in samples:
        row = pd.Series(sample)
        row['market'] = market
        code = row['代码']
        print(f"\n>>> {market} {code} {row.get('名称', '')}")
        try:
            df_financial = service.get_stock_border_financial_indicator(
                market=market,
                date='20240331',
                df_stock_spot=pd.DataFrame([row]),
            )
            s_data = backfill_from_financial(row, df_financial)
            df_analysis = strategy.calculate_stock_data(
                df_history_data=None,
                df_stock_data=s_data,
                stock_code=code,
                df_financial=df_financial,
            )
            res = df_analysis.iloc[0].to_dict()
            score, _ = strategy.calculate_score(
                df_history_data=pd.DataFrame(),
                df_stock=pd.DataFrame([row]),
                df_summary_data=df_analysis,
            )
            res['score'] = score
            rows.append(res)
            print(f"PE={res.get('PE')} ROE={res.get('ROE')} 市值={res.get('市值')} 类型={res.get('股票类型分类')} 阶段={res.get('五阶段判断模型')} 分区={res.get('四区价格分区')} score={score}")
        except Exception as e:
            print(f"ERROR: {e}")
    return pd.DataFrame(rows)


def main():
    sh_samples = [
        {'代码': '600519', '股票代码': '600519', '名称': '贵州茅台', '最新价': 1600.0, '总市值': 2000000000000.0, '市盈率-动态': 30.0, '60日涨跌幅': 5.0, '行业': '食品饮料'},
        {'代码': '600036', '股票代码': '600036', '名称': '招商银行', '最新价': 36.0, '总市值': 900000000000.0, '市盈率-动态': 7.0, '60日涨跌幅': 3.0, '行业': '银行'},
    ]
    h_samples = [
        {'代码': '00700', '股票代码': '00700', '名称': '腾讯控股', '最新价': 380.0, '总市值': 3500000000000.0, '市盈率-动态': 20.0, '60日涨跌幅': 8.0, '行业': '互联网服务'},
        {'代码': '00941', '股票代码': '00941', '名称': '中国移动', '最新价': 72.0, '总市值': 1500000000000.0, '市盈率-动态': 11.0, '60日涨跌幅': 2.0, '行业': '通信服务'},
    ]
    usa_samples = [
        {'代码': 'AAPL', '股票代码': 'AAPL', '名称': 'Apple', '最新价': 190.0, '总市值': 2900000000000.0, '市盈率-动态': 28.0, '60日涨跌幅': 2.0, '行业': 'Technology'},
        {'代码': 'NVDA', '股票代码': 'NVDA', '名称': 'NVIDIA', '最新价': 900.0, '总市值': 2200000000000.0, '市盈率-动态': 75.0, '60日涨跌幅': 45.0, '行业': 'Semiconductors'},
    ]

    sh_df = verify_market('SH', sh_samples)
    h_df = verify_market('H', h_samples)
    usa_df = verify_market('usa', usa_samples)

    print("\n===== Summary =====")
    for market, df in [('SH', sh_df), ('H', h_df), ('usa', usa_df)]:
        if df.empty:
            print(f"{market}: no results")
            continue
        ok_pe = df['PE'].notna().all() and (df['PE'] != -1).all()
        ok_roe = df['ROE'].notna().all() and (df['ROE'] != -1).all()
        ok_cap = df['市值'].notna().all() and (df['市值'] > 0).all()
        print(f"{market}: PE正常={ok_pe}, ROE正常={ok_roe}, 市值正常={ok_cap}")
        print(df[['stock_name', 'PE', 'ROE', '市值', '股票类型分类', '五阶段判断模型', '四区价格分区', 'score']].to_string(index=False))

if __name__ == '__main__':
    main()
