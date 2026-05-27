import sys
import os
import pandas as pd
import numpy as np

sys.path.append('/mnt/github/stock/stockAnalyse/src')
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo

def final_verify():
    market_service = stockBorderInfo(market='usa')
    
    # 模拟标准的 AAPL spot 数据 (因为之前探测到 AAPL 有缓存)
    df_spot = pd.DataFrame([{
        '代码': 'AAPL', 
        '股票代码': 'AAPL',
        '名称': 'Apple',
        '最新价': 190.0,
        '总市值': 2900000000000.0,
        'market': 'usa'
    }])
    
    print("--- 验证美股字段修复结果 (AAPL) ---")
    
    try:
        df_result = market_service.get_stock_border_financial_indicator(
            market='usa', 
            date='20240331', 
            df_stock_spot=df_spot
        )
        
        # 打印结果中所有代码，看看长什么样
        print(f"结果中的代码样例: {df_result['股票代码'].head(5).tolist()}")
        
        # 模糊匹配 AAPL
        mask = df_result['股票代码'].str.contains('AAPL', case=False)
        if not mask.any():
            print("❌ 未在结果中找到 AAPL")
            return
            
        res = df_result[mask].iloc[0]
        
        print(f"\n[验证指标]")
        fields = {
            '总市值': res.get('总市值'),
            '最新价': res.get('最新价'),
            'ROE': res.get('ROE'),
            'PE_TTM': res.get('PE_TTM')
        }
        
        for k, v in fields.items():
            status = '✅ 合法' if pd.notna(v) and (isinstance(v, (int, float)) and v != 0) else '❌ 异常'
            print(f"  - {k:8}: {v} ({status})")
            
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    final_verify()
