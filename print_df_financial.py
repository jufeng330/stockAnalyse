import pandas as pd
import numpy as np

# 模拟环境设置
class MockUtils:
    def pd_convert_to_float(self, df, col):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

class MockMarketService:
    def __init__(self):
        self.stock_utils = MockUtils()
    
    def _calculate_ttm_net_profit(self, df):
        # 模拟 TTM 逻辑
        df['净利润_TTM'] = df['净利润'] * 1.05 
        df['净利润_年报'] = df['净利润'].where(df['报告期'].str.endswith('12-31')).ffill()
        df['每股收益_TTM'] = df['基本每股收益'] * 1.05
        return df

    def get_mock_result(self):
        data = {
            '股票代码': ['600519']*3,
            '报告期': ['2023-09-30', '2023-12-31', '2024-03-31'],
            '净利润': [528.76e8, 747.34e8, 240.65e8],
            '基本每股收益': [42.1, 59.5, 19.1],
            '净资产收益率': [22.5, 30.1, 8.2]
        }
        df_fin = pd.DataFrame(data)
        
        df_spot = pd.DataFrame([{
            '代码': '600519',
            '名称': '贵州茅台',
            '最新价': 1560.0,
            '总市值': 19500e8
        }])

        # --- 核心修复逻辑 ---
        df_fin = self._calculate_ttm_net_profit(df_fin)
        df_fin['股票代码'] = df_fin['股票代码'].astype(str)
        df_spot['股票代码'] = df_spot['代码'].astype(str)
        
        spot_cols = [c for c in df_spot.columns if c not in df_fin.columns or c == '股票代码']
        df_merged = pd.merge(df_fin, df_spot[spot_cols], on='股票代码', how='left')
        
        # 统一 ROE
        df_merged['ROE'] = df_merged['净资产收益率']

        # 计算 PE
        df_merged['PE_TTM'] = df_merged['总市值'] / df_merged['净利润_TTM']
        df_merged['PE_静态'] = df_merged['总市值'] / df_merged['净利润_年报']
        
        return df_merged

service = MockMarketService()
df = service.get_mock_result()

print("\n--- [df_financial] 最终输出数据结果 ---")
cols = ['报告期', 'ROE', '最新价', '总市值', '净利润_TTM', 'PE_TTM', 'PE_静态']
print(df[cols].to_string(index=False))

print("\n--- 字段类型检查 ---")
print(df[cols].dtypes)
