import pandas as pd
import numpy as np
from stock_analyse.domain.strategies.selection_strategy_service import SelectionStrategyService
from stock_analyse.domain.strategies.stock_select_strategy import StockSelectStrategy

def test_strategy_7():
    service = SelectionStrategyService()
    # 构造包含完整财务特征的数据，以触发不同的分类
    mock_data = pd.DataFrame([
        {
            '代码': '600519', '名称': '贵州茅台', '最新价': 1700, 
            '市盈率-动态': 12, '总市值': 20000 * 1e8, '60日涨跌幅': -15, 
            'ROE': 25, '营业总收入同比增长率': 15, '净利润同比增长率': 18, '现金分红-股息率': 3.0, '行业': '白酒'
        },
        {
            '代码': '600036', '名称': '招商银行', '最新价': 35, 
            '市盈率-动态': 5, '总市值': 8000 * 1e8, '60日涨跌幅': 5, 
            'ROE': 15, '营业总收入同比增长率': 5, '净利润同比增长率': 8, '现金分红-股息率': 5.0, '行业': '银行'
        },
        {
            '代码': '300750', '名称': '宁德时代', '最新价': 180, 
            '市盈率-动态': 25, '总市值': 8000 * 1e8, '60日涨跌幅': 40, 
            'ROE': 20, '营业总收入同比增长率': 40, '净利润同比增长率': 45, '现金分红-股息率': 1.0, '行业': '锂电池'
        }
    ])
    
    print("开始多场景测试深度价值成长策略_7...")
    try:
        df_result = service.deep_value_growth_strategy(df_stock=mock_data, market='SH')
        
        if df_result is not None and not df_result.empty:
            cols = ['代码', '名称', 'score', '股票类型分类', '五阶段判断模型', '四区价格分区']
            print(df_result[cols].to_markdown(index=False))
        else:
            print("未选出股票")
    except Exception as e:
        print(f"失败: {e}")

if __name__ == "__main__":
    test_strategy_7()
