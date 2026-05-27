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
    # 我们需要模拟 EPS_TTM 不为空的情况，因为 PE = Price / EPS
    df_spot = pd.DataFrame([{
        '代码': '600519',
        '股票代码': '600519',
        '名称': '贵州茅台',
        '最新价': 1600.0,
        '总市值': np.nan,  # 模拟缺失总市值
        'market': 'SH'
    }])
    
    print("--- 验证贵州茅台 PE_TTM 计算 (模拟总市值缺失) ---")
    
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
        print(f"每股收益_TTM: {maotai_data.get('每股收益_TTM')}")
        print(f"PE_TTM: {maotai_data.get('PE_TTM')}")
        
        # 如果 PE_TTM 仍然为 NaN，说明计算逻辑有问题
        if pd.isna(maotai_data.get('PE_TTM')):
            print("\n❌ 发现问题: PE_TTM 为 NaN")
        else:
            print(f"\n✅ PE_TTM 计算正常: {maotai_data.get('PE_TTM')}")

    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_maotai_pe()
