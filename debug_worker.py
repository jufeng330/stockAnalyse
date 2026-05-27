import pandas as pd
import numpy as np
import sys
import os

# 加入 src 目录
sys.path.append(os.path.join(os.getcwd(), 'src'))

# 模拟我们需要测试的核心逻辑片段，不依赖外部库
def mock_get_stock_border_financial_indicator(df_stock_all, df_financial_raw, market='SH'):
    # --- 模拟我们在 market_data_service.py 中实现的逻辑 ---
    df_stock_financial_all = df_financial_raw.copy()
    
    # 1. 模拟 _calculate_ttm_net_profit 
    df_stock_financial_all['净利润_TTM'] = df_stock_financial_all['净利润'] * 1.1 # 简化逻辑
    df_stock_financial_all['净利润_年报'] = df_stock_financial_all['净利润']
    
    # 2. 模拟字段统一 (ROE)
    if '净资产收益率' in df_stock_financial_all.columns:
        df_stock_financial_all['ROE'] = df_stock_financial_all['净资产收益率']

    # 3. 补充 PE 计算逻辑 (我们刚才修改的重点)
    if df_stock_all is not None and not df_stock_all.empty:
        df_stock_all = df_stock_all.copy()
        df_stock_financial_all['股票代码'] = df_stock_financial_all['股票代码'].astype(str)
        
        if '股票代码' not in df_stock_all.columns and '代码' in df_stock_all.columns:
            df_stock_all['股票代码'] = df_stock_all['代码'].astype(str)
        
        # 清理旧列
        for col in ['最新价', '总市值']:
            if col in df_stock_financial_all.columns:
                df_stock_financial_all.drop(columns=[col], inplace=True)

        # 合并
        df_stock_financial_all = pd.merge(df_stock_financial_all, df_stock_all[['股票代码', '最新价', '总市值']], on='股票代码', how='left')
        
        # 计算 PE
        if '总市值' in df_stock_financial_all.columns:
            df_stock_financial_all['PE_TTM'] = df_stock_financial_all['总市值'] / df_stock_financial_all['净利润_TTM']
            df_stock_financial_all['PE_静态'] = df_stock_financial_all['总市值'] / df_stock_financial_all['净利润_年报']

    return df_stock_financial_all

def test_worker_data_chain():
    print("--- Mock Chain Debug for _worker ---")
    
    # 1. 模拟输入 (茅台)
    spot_row = pd.DataFrame([{
        '代码': '600519',
        '名称': '贵州茅台',
        '最新价': 1500.0,
        '总市值': 1800000000000.0
    }])
    
    # 2. 模拟原始财务数据
    raw_fin = pd.DataFrame([
        {'股票代码': '600519', '报告期': '2023-12-31', '净利润': 747e8, '净资产收益率': 0.3},
        {'股票代码': '600519', '报告期': '2024-03-31', '净利润': 240e8, '净资产收益率': 0.08}
    ])
    
    # 3. 运行逻辑
    result_df = mock_get_stock_border_financial_indicator(spot_row, raw_fin)
    
    print("\n[Result] Final Data Columns:")
    print(result_df.columns.tolist())
    
    # 验证关键字段
    check_cols = ['PE_TTM', 'PE_静态', 'ROE', '总市值', '最新价']
    for col in check_cols:
        val = result_df.iloc[-1].get(col)
        status = "OK" if pd.notna(val) else "MISSING/NaN"
        print(f"Column {col:12}: {status} (Value: {val})")

if __name__ == "__main__":
    test_worker_data_chain()
