import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime

# 添加项目路径
sys.path.append('/mnt/github/stock/stockAnalyse/src')

from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
from stock_analyse.domain.services.stock_strategy_service import StockStrategy
from stock_analyse.domain.strategies.selection_strategy_service import SelectionStrategyService

# 模拟 Selector 对象
class MockSelector:
    def __init__(self, market):
        self.market = market
        self.stock = stockBorderInfo()
        self.stock_strategy = StockStrategy()
        self.logger = type('MockLogger', (), {'info': print, 'warning': print, 'error': print, 'debug': print})()

def verify_worker():
    market = 'SH'
    selector = MockSelector(market)
    
    # 模拟一行股票数据 (从 get_stock_spot 得到的格式)
    row = pd.Series({
        '代码': '600519',
        '名称': '贵州茅台',
        '最新价': 1600.0,
        '总市值': 2000000000000.0,
        '市盈率-动态': 30.0,
        '60日涨跌幅': 5.0,
        '行业': '白酒'
    })

    print("--- 验证 _worker 内部逻辑 ---")

    # 1. 模拟 _get_frame_val 逻辑
    def _get_frame_val(row, keys, default=0):
        for k in keys:
            if k in row.index and pd.notna(row[k]):
                try:
                    return float(str(row[k]).replace(',', ''))
                except: continue
        return default

    pe_val = _get_frame_val(row, ['PE_TTM', 'PE_静态', '市盈率-动态', '市盈率', 'PE', 'pe_ttm'], -1)
    mkt_cap = _get_frame_val(row, ['总市值', '市值', 'market_val'], 0)
    
    print(f"初筛提取: pe_val={pe_val}, mkt_cap={mkt_cap}")

    # 2. 模拟财务数据获取与回填
    stock_code = row['代码']
    df_financial = selector.stock.get_stock_border_financial_indicator(
        market=market, df_stock_spot=pd.DataFrame([row])
    )

    s_data = row.copy()
    s_data['market'] = market
    
    if df_financial is not None and not df_financial.empty:
        latest_fin = df_financial.iloc[0]
        # 使用代码中的 field_map
        field_map = {
            '平均净资产收益率': ['roe', 'ROE', '平均净资产收益率'],
            '净利润同比增长率': ['net_profit_growth', '利润增长率', '净利润同比增长率'],
            '营业总收入同比增长率': ['revenue_growth', '营收增长率', '营业总收入同比增长率'],
            '资产负债率': ['debt_ratio', '负债率', '资产负债率'],
            '市盈率': ['pe', 'PE','PE_TTM', '市盈率'],
        }
        print("\n[执行数据回填]")
        for target, sources in field_map.items():
            for src in sources:
                if src in latest_fin.index and pd.notna(latest_fin[src]):
                    print(f"  回填 {target} <- {src}: {latest_fin[src]}")
                    s_data[target] = latest_fin[src]
                    break

    # 3. 验证 calculate_stock_data
    print("\n[调用 calculate_stock_data]")
    df_analysis = selector.stock_strategy.calculate_stock_data(
        df_history_data=None, 
        df_stock_data=s_data,
        stock_code=stock_code,
        df_financial=df_financial
    )
    
    res = df_analysis.iloc[0]
    print(f"分类结果:")
    print(f"  股票类型: {res['股票类型分类']}")
    print(f"  五阶段: {res['五阶段判断模型']}")
    print(f"  四区: {res['四区价格分区']}")
    print(f"  PE值: {res['PE']}")
    print(f"  ROE值: {res['ROE']}")

    # 4. 验证评分逻辑
    print("\n[调用 calculate_score]")
    score, signal_msg = selector.stock_strategy.calculate_score(
        df_history_data=pd.DataFrame(), 
        df_stock=pd.DataFrame([row]),
        df_summary_data=df_analysis
    )
    print(f"最终评分: {score}")
    print(f"信号详情: {signal_msg}")

if __name__ == "__main__":
    verify_worker()
