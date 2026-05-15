import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def get_pe_percentile_mock(symbol):
    # 模拟数据验证逻辑
    data = {
        '600519': {'pe_series': [40, 45, 35, 30, 25, 32, 28, 42, 38, 31], 'current': 31}, # 茅台：分位约 40%
        '600036': {'pe_series': [10, 12, 8, 7, 6, 9, 7.5, 11, 9.5, 6.5], 'current': 6.5}, # 招行：分位约 10%
        '300750': {'pe_series': [150, 120, 100, 80, 60, 40, 30, 45, 35, 33], 'current': 33} # 宁德：分位约 10%
    }
    
    if symbol not in data: return None
    
    pe_series = pd.Series(data[symbol]['pe_series'])
    current_pe = data[symbol]['current']
    
    rank = (pe_series < current_pe).sum()
    percentile = (rank / len(pe_series)) * 100
    
    return {
        '代码': symbol,
        '当前PE': current_pe,
        '3年最低': pe_series.min(),
        '3年最高': pe_series.max(),
        '历史分位(%)': round(percentile, 2)
    }

for s in ['600519', '600036', '300750']:
    print(get_pe_percentile_mock(s))
