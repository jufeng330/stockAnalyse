import sys
import os
import pandas as pd
import numpy as np

# 添加项目路径
sys.path.append('/mnt/github/stock/stockAnalyse/src')
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo

market_service = stockBorderInfo()
df_spot = pd.DataFrame([{
    '代码': '600519',
    '股票代码': '600519',
    '名称': '贵州茅台',
    '最新价': 1600.0,
    '总市值': np.nan, 
    'market': 'SH'
}])

df_fin = market_service.get_stock_border_financial_indicator(market="SH", date='20240331', df_stock_spot=df_spot)
maotai = df_fin[df_fin['股票代码'] == '600519']

print("Columns in result:", maotai.columns.tolist())
print("Data sample:")
print(maotai[['股票代码', '报告期', '最新价', '总市值', '净利润_TTM', '每股收益_TTM', 'PE_TTM']].to_string())
