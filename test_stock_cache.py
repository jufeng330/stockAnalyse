import sys
import os
import time
import pandas as pd

sys.path.append('/mnt/github/stock/stockAnalyse/src')
from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo

def test_cache():
    market = 'SH'
    stock_code = '603232'
    stock_service = stockCompanyInfo(marker=market, symbol=stock_code)
    
    print(f"Testing history data for {stock_code}")
    start = time.time()
    
    # 第一次获取
    df1 = stock_service.get_stock_history_data("20230101", "20260519")
    print(f"First fetch: {len(df1)} rows, {time.time() - start:.2f} seconds")
    
    start = time.time()
    # 第二次获取
    df2 = stock_service.get_stock_history_data("20230101", "20260519")
    print(f"Second fetch: {len(df2)} rows, {time.time() - start:.2f} seconds")

if __name__ == '__main__':
    test_cache()
