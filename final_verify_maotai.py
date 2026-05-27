import sys
import os
import pandas as pd
import numpy as np
import json

# 添加项目路径
sys.path.append('/mnt/github/stock/stockAnalyse/src')
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo

def run_final_verify():
    market_service = stockBorderInfo()
    
    # 模拟标准的贵州茅台 spot 数据
    # 按照当前市场真实数据模拟：股价约 1600，市值约 2 万亿
    df_spot = pd.DataFrame([{
        '代码': '600519',
        '股票代码': '600519',
        '名称': '贵州茅台',
        '最新价': 1600.0,
        '总市值': 2000000000000.0,
        'market': 'SH'
    }])
    
    print("=== 开始执行 get_stock_border_financial_indicator 完整测试 ===")
    
    try:
        # 验证 2024-03-31 报表数据
        df_result = market_service.get_stock_border_financial_indicator(
            market="SH", 
            date='20240331', 
            df_stock_spot=df_spot
        )
        
        if df_result is None or df_result.empty:
            print("❌ 错误：函数返回了空的 DataFrame")
            return

        # 筛选茅台的数据并按日期倒序排列，查看最新的一条
        maotai_df = df_result[df_result['股票代码'] == '600519'].sort_values('报告期', ascending=False)
        
        print(f"\n[函数输出结果统计]")
        print(f"总记录数: {len(maotai_df)}")
        print(f"列名列表: {maotai_df.columns.tolist()}")

        print(f"\n[最新一条记录详情 (2024-03-31 附近)]")
        # 挑选关键业务字段进行展示
        display_cols = [
            '股票代码', '报告期', '最新价', '总市值', 
            'PE_TTM', 'PE_静态', 'ROE', '净利润', 
            '净利润同比增长率', '营业总收入同比增长率', '净利润_TTM', '每股收益_TTM'
        ]
        # 只取存在的列
        display_cols = [c for c in display_cols if c in maotai_df.columns]
        
        latest_record = maotai_df.head(1)
        print(latest_record[display_cols].to_string(index=False))

        # 核心数据合理性校验
        row = latest_record.iloc[0]
        print("\n[关键指标检查]")
        print(f"1. PE_TTM: {row.get('PE_TTM')} -> {'✅ 合法' if pd.notna(row.get('PE_TTM')) and row.get('PE_TTM') > 0 else '❌ 异常'}")
        print(f"2. ROE (应 > 5): {row.get('ROE')} -> {'✅ 合法' if row.get('ROE', 0) > 5 else '❌ 异常 (单位可能仍有问题)'}")
        print(f"3. 增长率 (应 > 1): {row.get('净利润同比增长率')} -> {'✅ 合法' if abs(row.get('净利润同比增长率', 0)) > 1 else '❌ 异常 (单位可能仍有问题)'}")

    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_final_verify()
