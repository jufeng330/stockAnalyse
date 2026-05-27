import sys
import os
import pandas as pd
import numpy as np

# 添加项目路径
sys.path.append('/mnt/github/stock/stockAnalyse/src')
from stock_analyse.domain.services.stock_strategy_service import StockStrategy

def verify_new_classification():
    strategy = StockStrategy()
    
    print("--- 验证对齐文档后的股票分类体系 ---")
    
    # 测试案例 1: 完美修复的贵州茅台 (高增长, 合理估值, 稳步上涨)
    print("\n[测试案例 1: 贵州茅台 (合理高景气)]")
    stage1 = strategy._classify_price_stage(pe=35, change_60d=12, profit_growth=25, pe_percentile=50)
    zone1 = strategy._classify_price_zone(pe=35, pe_dynamic=35, pe_percentile=50)
    type1 = strategy._classify_stock_type(roe=25, revenue_growth=20, profit_growth=25, market_cap=20000, dividend_yield=2.5, industry="食品饮料", pe=35)
    print(f"阶段: {stage1} | 分区: {zone1} | 类型: {type1}")

    # 测试案例 2: 困境反转的周期股 (低估值, 刚跌透)
    print("\n[测试案例 2: 底部周期股]")
    stage2 = strategy._classify_price_stage(pe=10, change_60d=-8, profit_growth=-5, pe_percentile=15)
    zone2 = strategy._classify_price_zone(pe=10, pe_dynamic=10, pe_percentile=15)
    type2 = strategy._classify_stock_type(roe=8, revenue_growth=5, profit_growth=-5, market_cap=500, dividend_yield=1.5, industry="煤炭", pe=10)
    print(f"阶段: {stage2} | 分区: {zone2} | 类型: {type2}")

    # 测试案例 3: 疯狂泡沫的美股 AI 概念 (极高估值, 刚放缓)
    print("\n[测试案例 3: 泡沫期高科技股]")
    stage3 = strategy._classify_price_stage(pe=90, change_60d=5, profit_growth=40, pe_percentile=90)
    zone3 = strategy._classify_price_zone(pe=90, pe_dynamic=90, pe_percentile=90)
    type3 = strategy._classify_stock_type(roe=15, revenue_growth=50, profit_growth=40, market_cap=10000, dividend_yield=0, industry="半导体", pe=90)
    print(f"阶段: {stage3} | 分区: {zone3} | 类型: {type3}")

if __name__ == "__main__":
    verify_new_classification()
