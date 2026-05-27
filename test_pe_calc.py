import pandas as pd
import numpy as np
import sys
import os

# 将 src 目录加入路径
sys.path.append(os.path.join(os.getcwd(), 'src'))

# 模拟环境
class MockStockUtils:
    def pd_convert_to_float(self, df, col):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace('亿','00000000').str.replace('%',''), errors='coerce')
        return df

class MockReportDateUtils:
    def get_current_report_year_st(self, **kwargs): return '20241231'
    def get_finnancial_report_by_latest(self, df): return df.iloc[-1:].copy()

def test_logic():
    # 模拟 market_data_service 中的关键逻辑
    print("Testing PE calculation logic...")
    
    # 1. 模拟财务指标数据 (含计算出的 TTM)
    df_financial = pd.DataFrame([{
        '股票代码': '600519',
        '报告期': '2024-12-31',
        '净利润': 862.28e8,
        '净利润_TTM': 862.28e8,
        '净利润_年报': 862.28e8
    }])
    
    # 2. 模拟行情数据
    df_stock = pd.DataFrame([{
        '代码': '600519',
        '股票代码': '600519',
        '最新价': 1500.0,
        '总市值': 18000.0e8
    }])
    
    # 3. 模拟 merge 过程
    df_merge = pd.merge(df_stock, df_financial, on='股票代码', how='left')
    
    # 4. 执行 PE 计算 (核心代码片段)
    if '最新价' in df_merge.columns and '总市值' in df_merge.columns:
        # 计算 PE (静态)
        if '净利润_年报' in df_merge.columns:
            df_merge['PE_静态'] = df_merge['总市值'] / df_merge['净利润_年报']
        
        # 计算 PE (TTM)
        if '净利润_TTM' in df_merge.columns:
            df_merge['PE_TTM'] = df_merge['总市值'] / df_merge['净利润_TTM']

    print("\nResult Columns:", df_merge.columns.tolist())
    print("Calculated PE_TTM:", df_merge.iloc[0]['PE_TTM'])
    print("Calculated PE_静态:", df_merge.iloc[0]['PE_静态'])

if __name__ == "__main__":
    test_logic()
