import sys
import os
import pandas as pd
import numpy as np

# 添加项目路径
sys.path.append('/mnt/github/stock/stockAnalyse/src')
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo

def debug_mapping():
    market_service = stockBorderInfo(market='usa')
    
    # 模拟财务表
    df_fin = pd.DataFrame([{'股票代码': 'AAPL.US', '净利润': 100, '净资产': 200, '市盈率-TTM': 30}])
    
    # 模拟行情表
    df_spot = pd.DataFrame([{
        '代码': 'AAPL', 
        '总市值': 2000000.0, 
        '最新价': 190.0,
        'market': 'usa'
    }])
    
    # 手动运行 market_data_service.py 第 415-430 行的逻辑
    print("--- 仿真映射逻辑 ---")
    
    def clean_code(c):
        c = str(c).upper()
        for suffix in ['.US', '.HK', '.SH', '.SZ']:
            if c.endswith(suffix):
                return c[:c.rfind(suffix)]
        return c

    # 1. 清洗代码
    code_series_clean = df_fin['股票代码'].apply(clean_code)
    df_spot['股票代码_clean'] = df_spot['代码'].apply(clean_code)
    
    print(f"清洗后的财务代码: {code_series_clean.tolist()}")
    print(f"清洗后的行情代码: {df_spot['股票代码_clean'].tolist()}")

    # 2. 建立映射
    mkt_map = df_spot.set_index('股票代码_clean')['总市值']
    df_fin['总市值'] = code_series_clean.map(mkt_map)
    
    price_map = df_spot.set_index('股票代码_clean')['最新价']
    df_fin['最新价'] = code_series_clean.map(price_map)

    print("\n结果:")
    print(df_fin[['股票代码', '总市值', '最新价']].to_string())

if __name__ == "__main__":
    debug_mapping()
