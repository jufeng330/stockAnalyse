from __future__ import annotations

import unittest


class WebAppTest(unittest.TestCase):
    def test_create_app_registers_root_route(self):
        from stock_analyse.interfaces.web.app import create_app

        app = create_app()

        with app.test_client() as client:
            response = client.get('/')

        self.assertNotEqual(response.status_code, 404)

    def test_create_app_registers_stock_ai_route(self):
        from stock_analyse.interfaces.web.app import create_app

        app = create_app()

        with app.test_client() as client:
            response = client.get('/stock_ai')

        self.assertNotEqual(response.status_code, 404)
        self.assertIn(b'stock_ai', response.data)
        self.assertIn('/api/analyze_stock_ai'.encode(), response.data)


if __name__ == '__main__':
    unittest.main()
