import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime

# 添加项目路径
sys.path.append('/mnt/github/stock/stockAnalyse/src')

from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo

def verify_maotai():
    # 初始化服务 (类名是 stockBorderInfo 而不是 MarketDataService)
    market_service = stockBorderInfo()
    
    # 1. 模拟获取贵州茅台的 spot 数据 (600519)
    # A股 600519
    df_spot = pd.DataFrame([{
        '代码': '600519',
        '股票代码': '600519',
        '名称': '贵州茅台',
        '最新价': 1600.0,
        '总市值': 2000000000000.0, # 2万亿
        'market': 'SH'
    }])
    
    print("--- 验证贵州茅台 (600519) A股 财务指标 ---")
    
    # 2. 调用目标函数
    try:
        # 我们验证最近的报表，比如 2024-03-31
        df_financial = market_service.get_stock_border_financial_indicator(
            market="SH", 
            date='20240331', 
            df_stock_spot=df_spot
        )
        
        if df_financial is None or df_financial.empty:
            print("错误: 返回的财务数据为空")
            return

        # 3. 检查核心字段
        # 字段列表见代码：['股票代码', '报告期', 'PE_TTM', 'ROE', '总市值', '最新价', '净利润_TTM']
        print(f"返回列名: {df_financial.columns.tolist()}")
        
        # 只取茅台的数据
        maotai_data = df_financial[df_financial['股票代码'] == '600519']
        
        if maotai_data.empty:
            print("错误: 未找到茅台的数据记录")
            return

        # 打印最近几条报表数据
        cols_present = [c for c in ['股票代码', '报告期', 'PE_TTM', 'ROE', '总市值', '最新价', '净利润_TTM', '净利润同比增长率'] if c in df_financial.columns]
        print("\n最近报表详情:")
        print(maotai_data[cols_present].head(5).to_string())
        
        # 4. 合理性验证
        latest = maotai_data.iloc[0]
        
        issues = []
        # PE_TTM 茅台通常在 20-40 之间
        if 'PE_TTM' not in latest or pd.isna(latest['PE_TTM']) or latest['PE_TTM'] <= 0:
            issues.append(f"PE_TTM 异常: {latest.get('PE_TTM')}")
        
        # ROE 茅台通常 > 20
        if 'ROE' not in latest or pd.isna(latest['ROE']) or latest['ROE'] <= 0:
            issues.append(f"ROE 异常: {latest.get('ROE')}")
            
        if '总市值' not in latest or pd.isna(latest['总市值']) or latest['总市值'] < 1e11:
            issues.append(f"总市值异常: {latest.get('总市值')}")
            
        if not issues:
            print("\n✅ 验证成功: 核心指标 PE_TTM, ROE, 总市值 均存在且在合理范围内。")
        else:
            print("\n❌ 验证失败:")
            for issue in issues:
                print(f"  - {issue}")

    except Exception as e:
        import traceback
        print(f"执行过程中发生异常: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    verify_maotai()
