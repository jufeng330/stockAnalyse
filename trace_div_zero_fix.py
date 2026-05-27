import sys
import os
import pandas as pd
import numpy as np
import traceback

sys.path.append('/mnt/github/stock/stockAnalyse/src')
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo

def test_div_zero():
    market_service = stockBorderInfo(market='SH')
    
    # 模拟南宁百货 600712 导致 float division by zero 的场景
    df_spot = pd.DataFrame([{
        '代码': '600712', 
        '股票代码': '600712',
        '名称': '南宁百货',
        '最新价': 3.5,
        '总市值': 1900000000.0,
        'market': 'SH'
    }])
    
    print("--- 调试 600712 财务数据提取 ---")
    df_result = market_service.get_stock_border_financial_indicator(
        market='SH', 
        date='20240331', 
        df_stock_spot=df_spot
    )
    if df_result is not None and not df_result.empty:
        res = df_result.iloc[0]
        print(f"净利润_TTM: {res.get('净利润_TTM')}")
        print(f"每股收益_TTM: {res.get('每股收益_TTM')}")
        print(f"PE_TTM: {res.get('PE_TTM')}")

if __name__ == "__main__":
    test_div_zero()
