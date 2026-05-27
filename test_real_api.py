from src.stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer
from src.stock_analyse.infrastructure.config.settings import load_settings
import logging
logging.basicConfig(level=logging.INFO)
def run_real_test():
    analyzer = StockAiAnalyzer()
    print(f"--- 使用模型: {analyzer.model} 进行真实环境测试 ---")
    try:
        result = analyzer.stock_indicator_analyse(
            market='SH', 
            symbol='603259', 
            start_date='2026-04-01', 
            end_date='2026-05-26',
            client_id='test_integration_user'
        )
        print("\n--- 分析结果 (最终 JSON) ---")
        print(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
if __name__ == "__main__":
    run_real_test()
