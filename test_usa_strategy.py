import pandas as pd
import numpy as np
from stock_analyse.domain.strategies.selection_strategy_service import SelectionStrategyService
from stock_analyse.domain.strategies.stock_select_strategy import StockSelectStrategy

def test_usa_strategy_7():
    service = SelectionStrategyService()
    # 模拟美股数据 (usa 市场)
    # 注意：美股列名可能略有不同，但 _normalize_spot_filter_columns 会进行归一化
    mock_data = pd.DataFrame([
        {
            '代码': 'AAPL', '名称': '苹果', '最新价': 190, 
            '市盈率-动态': 28, '总市值': 30000 * 1e8, '60日涨跌幅': 5, 
            'ROE': 150, '营业总收入同比增长率': 5, '净利润同比增长率': 10, '现金分红-股息率': 0.5, '行业': '科技'
        },
        {
            '代码': 'TSLA', '名称': '特斯拉', '最新价': 170, 
            '市盈率-动态': 45, '总市值': 5000 * 1e8, '60日涨跌幅': -15, 
            'ROE': 15, '营业总收入同比增长率': -5, '净利润同比增长率': -20, '现金分红-股息率': 0, '行业': '汽车'
        },
        {
            '代码': 'NVDA', '名称': '英伟达', '最新价': 900, 
            '市盈率-动态': 35, '总市值': 22000 * 1e8, '60日涨跌幅': 80, 
            'ROE': 90, '营业总收入同比增长率': 260, '净利润同比增长率': 400, '现金分红-股息率': 0.02, '行业': '半导体'
        }
    ])
    
    print("开始测试【美股 usa】深度价值成长策略_7...")
    try:
        # 显式指定 market='usa'
        df_result = service.deep_value_growth_strategy(df_stock=mock_data, market='usa')
        
        if df_result is not None and not df_result.empty:
            cols = ['代码', '名称', 'score', '股票类型分类', '五阶段判断模型', '四区价格分区的取值']
            # 注意：实际输出列名可能是 '四区价格分区'，我们根据代码确认下
            final_cols = [c for c in ['代码', '名称', 'score', '股票类型分类', '五阶段判断模型', '四区价格分区'] if c in df_result.columns]
            print(df_result[final_cols].to_markdown(index=False))
        else:
            print("未选出美股股票")
    except Exception as e:
        print(f"美股测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_usa_strategy_7()
