from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from stock_analyse.interfaces.web.app import create_app, web_app_context
from stock_analyse.interfaces.web.services import trading_decision_service as trading_decision_service_module


class StubStockBorderInfo:
    def __init__(self, market='SH'):
        self.market = market

    def get_stock_spot(self):
        if self.market == 'usa':
            return pd.DataFrame(
                [
                    {'代码': 'AAPL', '名称': 'Apple Inc.', '股票代码': 'AAPL', '最新价': 189.52, '市盈率': 29.1},
                ]
            )
        if self.market == 'H':
            return pd.DataFrame(
                [
                    {'代码': '00700', '名称': '腾讯控股', '股票代码': '00700', '最新价': 315.8, '市盈率-动态': 18.6},
                ]
            )
        return pd.DataFrame(
            [
                {'代码': '300750', '名称': '宁德时代', '股票代码': '300750', '最新价': 182.4, '市盈率-动态': 21.8},
                {'代码': '600519', '名称': '贵州茅台', '股票代码': '600519', '最新价': 1688.0, '市盈率-动态': 29.6},
            ]
        )


def _patch_stock_lookup_source(monkeypatch):
    monkeypatch.setattr(trading_decision_service_module, 'stockBorderInfo', StubStockBorderInfo)


class TestHoldingStockRoutes:
    def setup_method(self):
        self.original_service = web_app_context.trading_decision_service
        self.original_settings = web_app_context.settings
        if not hasattr(web_app_context.settings, 'web'):
            web_app_context.settings = SimpleNamespace(
                web=SimpleNamespace(flask_secret_key='test-secret'),
                ai=getattr(self.original_settings, 'ai', SimpleNamespace()),
            )
        self.app = create_app()
        self.client = self.app.test_client()

    def teardown_method(self):
        web_app_context.trading_decision_service = self.original_service
        web_app_context.settings = self.original_settings

    def test_holding_stocks_page_renders_real_template(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'holding-page.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/holding-stocks')

        assert response.status_code == 200
        assert '持仓股票列表'.encode() in response.data
        assert '/api/trading-decision/holding-stocks'.encode() in response.data
        assert '当前持仓标的'.encode() in response.data
        assert '买入'.encode() in response.data
        assert '股票代码 / 名称搜索'.encode() in response.data
        content = response.data.decode('utf-8')
        assert 'id="market" name="market"' not in content
        assert 'id="asset_type" name="asset_type"' not in content
        assert 'id="form_market" name="market"' in content
        assert 'id="form_asset_type" name="asset_type"' in content
        assert '根据买入数量 × 买入价格自动计算。' in content
        assert 'syncAmountField' in content
        assert '请选择资产类型' in content
        assert content.count('id="market"') == 1
        assert content.count('id="asset_type"') == 1
        assert content.count('id="form_market"') == 1
        assert content.count('id="form_asset_type"') == 1
        assert '<select id="form_asset_type" name="asset_type" class="form-control" required>' in content
        assert '<input id="amount" name="amount" class="form-control readonly-input" type="number" step="0.01" readonly>' in content
        assert "document.getElementById('quantity').addEventListener('input', syncAmountField);" in content
        assert "document.getElementById('price').addEventListener('input', syncAmountField);" in content
        assert "document.getElementById('market').value" not in content
        assert "document.getElementById('asset_type').value" not in content
        assert "throw new Error(result.message || result?.error?.message || '保存失败');" in content
        assert 'placeholder="留空则自动按数量 × 价格计算"' not in content
        assert 'readonly' in content

    def test_holding_stock_api_create_and_append_buy(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'holding-api.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/holding-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'industry': '白酒',
                'asset_type': '成长龙头',
                'trade_date': '2026-04-28',
                'quantity': 100,
                'price': 1680,
                'current_price': 1688,
                'note': '首笔建仓',
            },
        )
        assert create_response.status_code == 200
        created = create_response.get_json()['data']
        assert created['stock_code'] == '600519'
        assert created['quantity'] == 100
        assert created['lot_count'] == 1
        assert len(created['lots']) == 1
        assert len(created['trades']) == 1

        append_response = self.client.put(
            f"/api/trading-decision/holding-stocks/{created['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 50,
                'price': 1600,
                'current_price': 1695,
                'note': '第二笔加仓',
            },
        )
        assert append_response.status_code == 200
        updated = append_response.get_json()['data']
        assert updated['quantity'] == 150
        assert updated['lot_count'] == 2
        assert len(updated['lots']) == 2
        assert len(updated['trades']) == 2
        assert round(updated['total_buy_amount'], 4) == 248000
        assert round(updated['average_cost'], 4) == round(248000 / 150, 4)

    def test_convert_watch_stock_to_holding_buy_updates_watch_link(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'holding-from-watch.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_watch_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
                'industry': '新能源',
                'current_price': 182.4,
            },
        )
        watch_stock = create_watch_response.get_json()['data']

        convert_response = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 200,
                'price': 180,
                'current_price': 182.4,
                'note': '从关注转持仓',
            },
        )
        assert convert_response.status_code == 200
        holding = convert_response.get_json()['data']
        assert holding['linked_watch_stock_id'] == watch_stock['id']
        assert holding['quantity'] == 200
        assert len(holding['trades']) == 1

        watch_detail_response = self.client.get(f"/api/trading-decision/watch-stocks/{watch_stock['id']}")
        updated_watch = watch_detail_response.get_json()['data']
        assert updated_watch['linked_holding_stock_id'] == holding['id']

        page_response = self.client.get(f"/holding-stocks?watch_stock_id={watch_stock['id']}")
        assert page_response.status_code == 200
        assert '本次提交会追加一笔买入'.encode() in page_response.data or '本次提交会创建持仓并自动建立关联'.encode() in page_response.data
