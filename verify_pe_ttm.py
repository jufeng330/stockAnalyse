import sys
import os
import pandas as pd
import numpy as np

# 添加项目路径
sys.path.append('/mnt/github/stock/stockAnalyse/src')

from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo

def verify_maotai_pe():
    market_service = stockBorderInfo()
    
    # 模拟获取贵州茅台的 spot 数据
    # 故意不提供最新价或提供 NaN，观察 PE_TTM 是否能正确计算
    df_spot = pd.DataFrame([{
        '代码': '600519',
        '股票代码': '600519',
        '名称': '贵州茅台',
        '最新价': np.nan,  # 模拟缺失最新价
        '总市值': 2000000000000.0,
        'market': 'SH'
    }])
    
    print("--- 验证贵州茅台 PE_TTM 计算 (模拟最新价缺失) ---")
    
    try:
        df_financial = market_service.get_stock_border_financial_indicator(
            market="SH", 
            date='20240331', 
            df_stock_spot=df_spot
        )
        
        if df_financial is None or df_financial.empty:
            print("错误: 返回的财务数据为空")
            return

        maotai_data = df_financial[df_financial['股票代码'] == '600519'].iloc[0]
        
        print(f"最新价: {maotai_data.get('最新价')}")
        print(f"总市值: {maotai_data.get('总市值')}")
        print(f"净利润_TTM: {maotai_data.get('净利润_TTM')}")
        print(f"PE_TTM: {maotai_data.get('PE_TTM')}")
        
        if pd.isna(maotai_data.get('PE_TTM')):
            print("\n❌ 发现问题: PE_TTM 为 NaN")
        else:
            print("\n✅ PE_TTM 计算正常")

    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_maotai_pe()
