from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from types import SimpleNamespace

from flask import Flask

from stock_analyse.interfaces.web.routes.analysis import register_analysis_routes


class _DummySSEManager:
    def add_client(self, client_id, queue):
        return None

    def remove_client(self, client_id):
        return None

    def send_to_client(self, client_id, event_type, data):
        return None


class _DummyAnalyzer:
    def __init__(self):
        self.streaming = None
        self.calls = []

    def stock_ai_analysis_process(self, stock_code, market, start_date, end_date, **kwargs):
        self.calls.append(
            {
                'stock_code': stock_code,
                'market': market,
                'start_date': start_date,
                'end_date': end_date,
                **kwargs,
            }
        )
        return {'success': True, 'data': {'stock_code': stock_code}}


class _DummyTradingDecisionService:
    def build_cached_stock_analysis_result(self, **kwargs):
        return None

    def annotate_result_source(self, result, result_source='live'):
        result['result_source'] = result_source


def test_analyze_stock_ai_uses_scene_specific_lock_keys():
    app = Flask(__name__)
    context = SimpleNamespace(
        analyzer=_DummyAnalyzer(),
        sse_manager=_DummySSEManager(),
        executor=ThreadPoolExecutor(max_workers=4),
        task_lock=Lock(),
        analysis_tasks={},
        trading_decision_service=_DummyTradingDecisionService(),
    )
    app.extensions['stock_analyse.context'] = context
    register_analysis_routes(app)

    client = app.test_client()

    stock_analysis_response = client.post(
        '/api/analyze_stock_ai',
        json={
            'stock_code': '600900',
            'market': 'SH',
            'client_id': 'client-stock-analysis',
            'analysis_scene': 'stock_analysis',
        },
    )
    holding_reanalysis_response = client.post(
        '/api/analyze_stock_ai',
        json={
            'stock_code': '600900',
            'market': 'SH',
            'client_id': 'client-holding-reanalysis',
            'analysis_scene': 'holding_reanalysis',
            'holding_stock_id': 'HS-001',
        },
    )

    context.executor.shutdown(wait=True)

    assert stock_analysis_response.status_code == 200
    assert holding_reanalysis_response.status_code == 200
    assert len(context.analyzer.calls) == 2
    assert context.analyzer.calls[0]['analysis_scene'] == 'stock_analysis'
    assert context.analyzer.calls[1]['analysis_scene'] == 'holding_reanalysis'
    assert context.analyzer.calls[1]['holding_stock_id'] == 'HS-001'
    assert context.analysis_tasks == {}
