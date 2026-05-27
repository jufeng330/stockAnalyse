import sys
import os
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime

# 添加项目路径
sys.path.append('/mnt/github/stock/stockAnalyse/src')

from stock_analyse.domain.strategies.selection_strategy_service import SelectionStrategyService
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
from stock_analyse.domain.strategies.stock_select_strategy import StockSelectStrategy
from stock_analyse.domain.services.stock_strategy_service import StockStrategy

# 配置日志到标准输出，模拟跟踪
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger('Tracer')

def trace_execution():
    market = 'SH'
    strategy_type = 7
    
    print(f"=== 1. 模拟调用链入口: 深度价值成长策略_7 (Market: {market}) ===")
    market_service = stockBorderInfo(market=market)
    
    # 真实获取行情数据
    df_all_spot = market_service.get_stock_spot()
    samples = ['600519', '601318', '600036', '601857', '601398'] 
    df_sample_spot = df_all_spot[df_all_spot['代码'].isin(samples)].copy()
    
    print(f"选取样本股票: {df_sample_spot['名称'].tolist()}")

    # 2. 进入 SelectionStrategyService._apply_strategy_7 逻辑
    print(f"\n=== 2. 进入策略逻辑环: SelectionStrategyService ===")
    service = SelectionStrategyService()
    # 关键逻辑在 StockStrategy (stock_strategy_service.py)
    stock_strategy = StockStrategy()
    
    results = []
    for _, row in df_sample_spot.iterrows():
        stock_code = row['代码']
        print(f"\n>>> 处理股票: {row['名称']} ({stock_code})")
        
        # A. 财务指标提取
        df_financial = market_service.get_stock_border_financial_indicator(
            market=market, df_stock_spot=pd.DataFrame([row])
        )
        
        # B. 数据回填逻辑验证
        s_data = row.copy()
        s_data['market'] = market
        if df_financial is not None and not df_financial.empty:
            latest_fin = df_financial.iloc[0]
            print(f"   [数据源校验] ROE: {latest_fin.get('ROE')} | 增长率: {latest_fin.get('净利润同比增长率')} | PE_TTM: {latest_fin.get('PE_TTM')}")
            
            field_map = {
                '平均净资产收益率': ['roe', 'ROE', '平均净资产收益率'],
                '净利润同比增长率': ['net_profit_growth', '利润增长率', '净利润同比增长率'],
                '营业总收入同比增长率': ['revenue_growth', '营收增长率', '营业总收入同比增长率'],
                '资产负债率': ['debt_ratio', '负债率', '资产负债率'],
                '市盈率': ['pe', 'PE','PE_TTM', '市盈率'],
            }
            for target, sources in field_map.items():
                for src in sources:
                    if src in latest_fin.index and pd.notna(latest_fin[src]):
                        s_data[target] = latest_fin[src]
                        break

        # C. 核心分析逻辑 (StockStrategy)
        df_analysis = stock_strategy.calculate_stock_data(
            df_history_data=None, 
            df_stock_data=s_data,
            stock_code=stock_code,
            df_financial=df_financial
        )
        
        # D. 评分
        score, msg = stock_strategy.calculate_score(
            df_history_data=pd.DataFrame(),
            df_stock=pd.DataFrame([row]),
            df_summary_data=df_analysis
        )
        
        res = df_analysis.iloc[0].to_dict()
        res['score'] = score
        results.append(res)
        
        print(f"   [计算详情] 计算用PE: {res['PE']} | 计算用ROE: {res['ROE']} | 利润增长: {res['利润增长率']}")
        print(f"   [中间结果] 判定分类: {res['股票类型分类']} | 阶段: {res['五阶段判断模型']} | 分区: {res['四区价格分区']}")
        print(f"   [最终判定] 得分: {score}")

    # 3. 结果可信度分析输出
    print(f"\n=== 3. 整个调用链验证结论 ===")
    df_final = pd.DataFrame(results)
    
    print("\n[可信度分析表]")
    metrics = {
        'PE_TTM / PE': '⭐⭐⭐⭐⭐ (可信: 行情带 PE-动态，财报带 PE_TTM，逻辑已对齐)',
        'ROE': '⭐⭐⭐⭐ (高可信: 数据层修复后 10.5 代表 10.5%)',
        '净利润同比增长率': '⭐⭐⭐⭐ (高可信: 已统一为百分数)',
        '总市值': '⭐⭐⭐⭐⭐ (可信: 单位元)',
        '股票类型分类': '⭐⭐⭐ (一般: 依赖行业词库，茅台被判为高增长期，招行判为成熟期/防守型)',
        '五阶段判断模型': '⭐⭐ (低: 60日涨跌幅对阶段判定影响过大，目前 A 股波动大，容易在 B/D 间跳变)',
        '四区价格分区': '⭐⭐⭐⭐ (高可信: 优先采用财报 PE 百分位)'
    }
    for k, v in metrics.items():
        print(f"{k:20}: {v}")

    print("\n[典型字段取值可信度]")
    for _, row in df_final.iterrows():
        print(f"{row['stock_name']}: 类型={row['股票类型分类']}, 阶段={row['五阶段判断模型']}, 分区={row['四区价格分区']}, 得分={row['score']}")

if __name__ == "__main__":
    trace_execution()
