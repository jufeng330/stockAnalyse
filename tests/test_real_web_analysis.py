import unittest
import json
import time
from src.stock_analyse.interfaces.web.app import create_app

class TestRealWebAnalysis(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.testing = True
        self.client = self.app.test_client()

    def test_single_stock_analysis_real(self):
        print("\n--- 正在请求真实环境 API 进行分析 ---")
        payload = {
            "stock_code": "603259",
            "market": "SH",
            "start_date": "2026-04-01",
            "end_date": "2026-05-26",
            "trade_date": "2026-05-26",
            "analysis_depth": "standard",
            "client_id": "test_web_integration_001"
        }
        
        response = self.client.post('/api/analyze_stock_ai', 
                                    json=payload, 
                                    content_type='application/json')
        
        print(f"API 响应: {response.data.decode()}")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data.get("success"), "API 返回失败")
        print("API 触发成功，后端任务已启动。")

if __name__ == '__main__':
    unittest.main()
