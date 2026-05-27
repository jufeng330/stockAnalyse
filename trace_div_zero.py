import sys
import os
import pandas as pd
import numpy as np
import traceback

sys.path.append('/mnt/github/stock/stockAnalyse/src')
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo

def test_div_zero():
    market_service = stockBorderInfo(market='SH')
    
    # 我们构造一个极端数据，试图复现 float division by zero
    # 比如：净资产为 0
    df_spot = pd.DataFrame([{
        '代码': '600712', 
        '股票代码': '600712',
        '名称': '南宁百货',
        '最新价': 3.5,
        '总市值': 1900000000.0,
        'market': 'SH'
    }])
    
    print("--- 调试 600712 财务数据提取 ---")
    try:
        df_result = market_service.get_stock_border_financial_indicator(
            market='SH', 
            date='20240331', 
            df_stock_spot=df_spot
        )
        if df_result is not None and not df_result.empty:
            res = df_result.iloc[0]
            print(f"提取成功: 净利润={res.get('净利润')}, 净利润_TTM={res.get('净利润_TTM')}, 总市值={res.get('总市值')}")
            print(f"PE_TTM={res.get('PE_TTM')}")
    except Exception as e:
        print(f"获取财务数据发生异常:")
        traceback.print_exc()

if __name__ == "__main__":
    test_div_zero()
