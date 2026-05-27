import sys
import os
import pandas as pd
import numpy as np

# 添加项目路径
sys.path.append('/mnt/github/stock/stockAnalyse/src')
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo

def debug_usa_raw_fields():
    # 使用 usa 市场初始化
    market = 'usa'
    market_service = stockBorderInfo(market=market)
    
    # 模拟一个美股 spot 数据，用于触发财务指标查询
    # 我们用 NVDA 作为例子
    df_spot = pd.DataFrame([{
        '代码': 'NVDA',
        '股票代码': 'NVDA',
        '名称': 'NVIDIA',
        '最新价': 900.0,
        '总市值': 2200000000000.0,
        'market': 'usa'
    }])
    
    print(f"--- 正在调试美股 (NVDA) 原始财务字段获取 ---")
    
    try:
        # 调用核心获取函数
        df_financial = market_service.get_stock_border_financial_indicator(
            market=market, 
            date='20240331', 
            df_stock_spot=df_spot
        )
        
        if df_financial is None or df_financial.empty:
            print("❌ 错误：未获取到美股财务数据。可能是 Futu 接口连接失败或缓存为空。")
            return

        # 打印所有列名，看看实际返回了什么
        print(f"\n[1. 返回的所有列名]:\n{df_financial.columns.tolist()}")

        # 提取 NVDA 的第一行数据
        nvda_data = df_financial.iloc[0]
        
        print(f"\n[2. 核心字段值详情]:")
        # 重点看用户要求的字段
        target_fields = [
            '股票代码', '报告期', 
            '总市值', '最新价', 
            'PE_TTM', 'PE_静态', 
            'ROE', '净资产收益率', '平均净资产收益率',
            '净利润', '归属于母公司股东净利润',
            '净利润同比增长率', '营业总收入'
        ]
        
        for field in target_fields:
            val = nvda_data.get(field, "N/A (字段不存在)")
            print(f"  - {field:15}: {val}")

        # 分析 PE_TTM 为什么可能为 NaN
        print(f"\n[3. PE_TTM 计算链路分析]:")
        print(f"  - 总市值: {nvda_data.get('总市值')}")
        print(f"  - 净利润: {nvda_data.get('净利润')}")
        print(f"  - 净利润_TTM: {nvda_data.get('净利润_TTM', 'N/A')}")
        
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_usa_raw_fields()
