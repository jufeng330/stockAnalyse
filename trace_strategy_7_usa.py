import sys
import os
import pandas as pd
import numpy as np
import logging
from datetime import datetime

# 添加项目路径
sys.path.append('/mnt/github/stock/stockAnalyse/src')

from stock_analyse.infrastructure.services.futu_market_data_provider import FutuMarketDataProvider
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
from stock_analyse.domain.services.stock_strategy_service import StockStrategy
from stock_analyse.domain.strategies.selection_strategy_service import SelectionStrategyService

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger('TracerUSA')

def trace_execution_usa():
    market = 'usa'
    strategy_type = 7
    
    print(f"=== 1. 模拟调用链入口: 深度价值成长策略_7 (Market: {market}) ===")
    
    # 尝试使用 Futu Provider 获取美股快照（如果有缓存或能连接）
    # 如果失败，手动模拟 NVDA (英伟达) 和 AAPL (苹果)
    try:
        futu = FutuMarketDataProvider(market)
        # 模拟 NVDA 和 AAPL 的代码
        # NVDA 股价 ~900, 市值 ~2.2T, PE ~70
        # AAPL 股价 ~190, 市值 ~2.9T, PE ~28
        samples = [
            {'代码': 'NVDA', '名称': 'NVIDIA', '最新价': 900.0, '总市值': 2200000000000.0, '市盈率-动态': 75.0, '60日涨跌幅': 45.0, '行业': 'Semiconductors'},
            {'代码': 'AAPL', '名称': 'Apple', '最新价': 190.0, '总市值': 2900000000000.0, '市盈率-动态': 28.0, '60日涨跌幅': 2.0, '行业': 'Technology'},
            {'代码': 'TIGER', '名称': 'SmallCap', '最新价': 5.0, '总市值': 100000000.0, '市盈率-动态': 5.0, '60日涨跌幅': -15.0, '行业': 'Retail'}
        ]
        df_sample_spot = pd.DataFrame(samples)
    except:
        print("Futu Provider 初始化失败，使用硬编码模拟数据")
        return

    print(f"选取样本股票: {df_sample_spot['名称'].tolist()}")

    # 2. 进入逻辑环
    market_service = stockBorderInfo(market=market)
    stock_strategy = StockStrategy()
    
    results = []
    for _, row in df_sample_spot.iterrows():
        stock_code = row['代码']
        print(f"\n>>> 处理股票: {row['名称']} ({stock_code})")
        
        # A. 财务指标提取
        # 注意：美股财务数据在本地可能缺失，会尝试 get_stock_snapshot_detail
        df_financial = market_service.get_stock_border_financial_indicator(
            market=market, df_stock_spot=pd.DataFrame([row])
        )
        
        s_data = row.copy()
        s_data['market'] = market
        
        if df_financial is not None and not df_financial.empty:
            latest_fin = df_financial.iloc[0]
            print(f"   [财务数据提取成功] ROE: {latest_fin.get('ROE')} | 利润增长: {latest_fin.get('净利润同比增长率')}")
            
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
        else:
            print(f"   [财务数据缺失] 警告：分类将回退到默认逻辑")

        # B. 核心分析逻辑
        df_analysis = stock_strategy.calculate_stock_data(
            df_history_data=None, 
            df_stock_data=s_data,
            stock_code=stock_code,
            df_financial=df_financial
        )
        
        # C. 评分
        score, msg = stock_strategy.calculate_score(
            df_history_data=pd.DataFrame(),
            df_stock=pd.DataFrame([row]),
            df_summary_data=df_analysis
        )
        
        res = df_analysis.iloc[0].to_dict()
        res['score'] = score
        results.append(res)
        
        print(f"   [计算详情] PE: {res['PE']} | ROE: {res['ROE']} | 利润增长: {res['利润增长率']}")
        print(f"   [中间结果] 判定分类: {res['股票类型分类']} | 阶段: {res['五阶段判断模型']} | 分区: {res['四区价格分区']}")
        print(f"   [最终判定] 得分: {score}")

    print(f"\n=== 3. 美股调用链验证结论 ===")
    for r in results:
        print(f"{r['stock_name']}: 分类={r['股票类型分类']}, 阶段={r['五阶段判断模型']}, 分区={r['四区价格分区']}, 得分={r['score']}")

if __name__ == "__main__":
    trace_execution_usa()
