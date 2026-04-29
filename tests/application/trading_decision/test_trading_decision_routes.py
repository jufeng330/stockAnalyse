from __future__ import annotations

from concurrent.futures import Future
from types import SimpleNamespace

import pandas as pd
from flask import jsonify
from stock_analyse.application.dto.entry_decision_state import EntryDecisionState
from stock_analyse.interfaces.web.routes import analysis as analysis_routes_module


from stock_analyse.interfaces.web.app import create_app, web_app_context
from stock_analyse.interfaces.web.routes import trading_decision as trading_decision_routes_module
from stock_analyse.interfaces.web.services import trading_decision_service as trading_decision_service_module


class StubStockBorderInfo:
    def __init__(self, market='SH'):
        self.market = market

    def get_stock_spot(self):
        if self.market == 'usa':
            return pd.DataFrame(
                [
                    {'代码': 'AAPL', '名称': 'Apple Inc.', '股票代码': 'AAPL', '最新价': 189.52, '市盈率': 29.1},
                    {'代码': 'TSLA', '名称': 'Tesla', '股票代码': 'TSLA', '最新价': 171.33, '市盈率': 61.4},
                ]
            )
        if self.market == 'H':
            return pd.DataFrame(
                [
                    {'代码': '00700', '名称': '腾讯控股', '股票代码': '00700', '最新价': 315.8, '市盈率-动态': 18.6},
                    {'代码': '09988', '名称': '阿里巴巴', '股票代码': '09988', '最新价': 72.4, '市盈率-动态': 10.2},
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


class TestTradingDecisionRoutes:
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

    def test_watch_stocks_page_renders_real_template(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'page.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/watch-stocks')

        assert response.status_code == 200
        assert '关注股票列表'.encode() in response.data
        assert '/api/trading-decision/watch-stocks'.encode() in response.data
        assert '股票代码 / 名称搜索'.encode() in response.data
        assert '/api/trading-decision/watch-stocks/stock-search'.encode() in response.data
        assert '指数资产'.encode() in response.data
        assert 'id="current_stage" name="current_stage" type="hidden"'.encode() in response.data
        assert 'id="current_price" name="current_price"'.encode() in response.data
        assert 'id="pe" name="pe"'.encode() in response.data

    def test_watch_stock_api_crud(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'api.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长型',
                'industry': '新能源',
            },
        )
        assert create_response.status_code == 200
        created = create_response.get_json()['data']
        assert created['stock_code'] == '300750'

        list_response = self.client.get('/api/trading-decision/watch-stocks')
        listed = list_response.get_json()['data']
        assert list_response.status_code == 200
        assert listed['pagination']['total'] == 1

        detail_response = self.client.get(f"/api/trading-decision/watch-stocks/{created['id']}")
        assert detail_response.status_code == 200
        assert detail_response.get_json()['data']['stock_name'] == '宁德时代'

        update_response = self.client.put(
            f"/api/trading-decision/watch-stocks/{created['id']}",
            json={'suggested_action': '计划跟踪', 'note': '更新'},
        )
        assert update_response.status_code == 200
        assert update_response.get_json()['data']['suggested_action'] == '计划跟踪'

        archive_response = self.client.post(f"/api/trading-decision/watch-stocks/{created['id']}/archive")
        assert archive_response.status_code == 200
        assert archive_response.get_json()['data']['status'] == 'archived'

        list_after_archive = self.client.get('/api/trading-decision/watch-stocks').get_json()['data']
        assert list_after_archive['pagination']['total'] == 0

    def test_watch_stock_api_validates_required_fields(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'validation.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.post('/api/trading-decision/watch-stocks', json={'stock_code': '300750'})

        assert response.status_code == 400
        body = response.get_json()
        assert body['success'] is False
        assert body['error']['code'] == 'bad_request'

    def test_watch_stock_api_returns_not_found(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'not-found.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/api/trading-decision/watch-stocks/WS-UNKNOWN')

        assert response.status_code == 404
        assert response.get_json()['error']['code'] == 'not_found'

    def test_stock_search_api_uses_get_stock_spot_for_a_share(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'search-a.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/api/trading-decision/watch-stocks/stock-search?query=300&market=A股&limit=10')

        assert response.status_code == 200
        items = response.get_json()['data']
        assert len(items) == 1
        assert items[0]['code'] == '300750'
        assert items[0]['name'] == '宁德时代'
        assert items[0]['market'] == 'A股'
        assert items[0]['source'] == 'spot'
        assert items[0]['current_price'] == 182.4
        assert items[0]['pe'] == 21.8

    def test_stock_search_api_supports_us_name_lookup(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'search-us.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/api/trading-decision/watch-stocks/stock-search?query=apple&market=usa&limit=10')

        assert response.status_code == 200
        items = response.get_json()['data']
        assert len(items) == 1
        assert items[0]['code'] == 'AAPL'
        assert items[0]['market'] == '美股'
        assert items[0]['current_price'] == 189.52
        assert items[0]['pe'] == 29.1

    def test_stock_search_api_returns_empty_for_blank_query(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'search-empty.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/api/trading-decision/watch-stocks/stock-search?query=&market=A股')

        assert response.status_code == 200
        assert response.get_json()['data'] == []

    def test_stock_analysis_record_page_renders_real_template(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'stock-analysis-record.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/stock-analysis-record')

        assert response.status_code == 200
        assert '股票分析记录'.encode() in response.data
        assert '/api/analyze_stock_ai'.encode() in response.data
        assert '/api/sse'.encode() in response.data
        assert '结果标签页会根据 `final_result` 的实际数据块动态生成。'.encode() in response.data
        assert '来自缓存'.encode() in response.data
        assert '实时生成'.encode() in response.data
        assert 'renderSourceBadge'.encode() in response.data
        assert 'summary'.encode() in response.data
        assert '总结'.encode() in response.data
        assert 'buildRoleTabs'.encode() in response.data
        assert 'renderStructuredInsightList'.encode() in response.data
        assert 'renderRoleSummaryBlock'.encode() in response.data
        assert 'formatRiskLevel'.encode() in response.data
        assert '市场分析师'.encode() in response.data
        assert '情绪分析师'.encode() in response.data
        assert '风控经理'.encode() in response.data
        assert 'signal'.encode() in response.data
        assert 'detail'.encode() in response.data
        assert 'risk'.encode() in response.data
        assert '多角色分析'.encode() not in response.data
        assert '分析模型'.encode() not in response.data

    def test_stock_analysis_record_page_reads_watch_stock_context(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'stock-analysis-record-context.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/stock-analysis-record?watch_stock_id=WS-001&code=600519&market=sh')

        assert response.status_code == 200
        assert 'watchStockIdText'.encode() in response.data
        assert "params.get('stock_code') || params.get('code')".encode() in response.data
        assert 'hydrateWatchStockContext(watchStockId)'.encode() in response.data
        assert '/api/trading-decision/watch-stocks/${encodeURIComponent(watchStockId)}'.encode() in response.data
        assert 'normalizedMarket = market.toLowerCase()'.encode() in response.data

    def test_stock_analysis_record_page_selects_record_by_record_id(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'stock-analysis-record-select.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        older = service.save_stock_analysis_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'raw_result': {
                    'success': True,
                    'data': {
                        'stock_code': '600519',
                        'stock_name': '贵州茅台',
                        'market': 'A股',
                        'trade_date': '2026-04-27',
                        'stance': 'bullish',
                        'time_horizon': '3-10 trading days',
                        'logic': '第一条分析逻辑',
                        'decision': {'summary': '第一条分析结论', 'risk_level': 'medium'},
                        'scores': {'technical': 81, 'sentiment': 70, 'composite': 76},
                        'signals': [],
                        'risks': [],
                        'evidence': [],
                        'meta': {},
                        'snapshot': {},
                    },
                },
            }
        )
        service.save_stock_analysis_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-28',
                'raw_result': {
                    'success': True,
                    'data': {
                        'stock_code': '600519',
                        'stock_name': '贵州茅台',
                        'market': 'A股',
                        'trade_date': '2026-04-28',
                        'stance': 'neutral',
                        'time_horizon': '1-3 trading days',
                        'logic': '第二条分析逻辑',
                        'decision': {'summary': '第二条分析结论', 'risk_level': 'high'},
                        'scores': {'technical': 61, 'sentiment': 55, 'composite': 58},
                        'signals': [],
                        'risks': [],
                        'evidence': [],
                        'meta': {},
                        'snapshot': {},
                    },
                },
            }
        )

        response = self.client.get(f"/stock-analysis-record?watch_stock_id={watch_stock['id']}&record_id={older['id']}")

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert older['id'] in content
        assert '第一条分析结论' in content
        assert 'const SELECTED_RECORD =' in content
        assert 'trade_date\": \"2026-04-27\"' in content or 'trade_date&quot;: &quot;2026-04-27&quot;' in content
        assert 'watchStockIdText' in content
        assert '已加载历史股票分析记录' in content
        assert f'/stock-analysis-record?watch_stock_id={watch_stock["id"]}&record_id={older["id"]}' in content

    def test_watch_stocks_page_links_to_stock_analysis_record(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'watch-stocks-link.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        assert create_response.status_code == 200
        created = create_response.get_json()['data']

        page_response = self.client.get('/watch-stocks')

        assert page_response.status_code == 200
        assert f'/stock-analysis-record?watch_stock_id={created["id"]}'.encode() in page_response.data
        assert '股票分析'.encode() in page_response.data
        assert 'send_file(UI_DOC_ROOT / \'stock_analysis_page.html\')'.encode() not in page_response.data

    def test_stock_analysis_record_page_uses_record_sidebar_state(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'stock-analysis-record-sidebar.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/stock-analysis-record')

        assert response.status_code == 200
        assert 'data-page="stock-analysis-record"'.encode() in response.data
        assert 'data-tab-target="decision"'.encode() not in response.data
        assert 'normalizeAnalysisRecordResult'.encode() in response.data
        assert 'renderTabs(normalized.tabs)'.encode() in response.data
        assert 'render_template(\'stock_analysis_record.html\')'.encode() not in response.data

    def test_entry_decision_page_renders_real_template(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-page.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
                'industry': '白酒',
                'current_price': 1688.0,
                'pe': 29.6,
                'last_conclusion_summary': '上次结论',
            },
        )
        watch_stock = create_response.get_json()['data']

        response = self.client.get(f"/entry-decision?watch_stock_id={watch_stock['id']}")

        assert response.status_code == 200
        assert '进场决策'.encode() in response.data
        assert '生成决策'.encode() in response.data
        assert '继续执行'.encode() in response.data
        assert '保存决策记录'.encode() in response.data
        assert '/entry-decision/analyze'.encode() in response.data
        assert '/api/trading-decision/entry-decisions'.encode() in response.data
        assert '/api/trading-decision/entry-decision-records'.encode() in response.data
        assert '/api/sse'.encode() in response.data
        assert '自动取数说明'.encode() in response.data
        assert '最大目标仓位'.encode() in response.data
        assert 'id="investmentHorizon"'.encode() not in response.data
        assert 'id="expectationSummary"'.encode() not in response.data
        assert 'id="revenueGrowth"'.encode() not in response.data
        assert 'id="profitGrowth"'.encode() not in response.data
        assert 'id="cashflowStatus"'.encode() not in response.data
        assert 'id="marginTrend"'.encode() not in response.data
        assert 'id="valuationPE"'.encode() not in response.data
        assert 'id="valuationPB"'.encode() not in response.data
        assert 'id="valuationJudgement"'.encode() not in response.data
        assert watch_stock['stock_code'].encode() in response.data
        assert watch_stock['stock_name'].encode() in response.data

        content = response.data.decode('utf-8')
        assert 'showPausePanel' in content
        assert 'resumeDecision()' in content
        assert 'saveDecisionRecord()' in content
        assert 'renderSectionCard' in content
        assert 'entry_decision_summary_markdown' in content
        assert 'marked.parse' in content
        assert '查看完整结果 JSON' in content
        assert '结果区会展示分析师结果、总结，以及原始 JSON。' in content
        assert '来自缓存' in content
        assert '实时生成' in content
        assert 'renderSourceBadge' in content
        assert '.nav-links a:hover' in content
        assert 'font-size: 14px;@media' not in content
        assert 'font-weight: 600;        body {' not in content
        assert '生成AI决策建议' not in content
        assert 'saveDecision()' not in content

    def test_entry_decision_page_backfills_current_price_from_market_spot(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-page-price.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
                'industry': '白酒',
            },
        )
        watch_stock = create_response.get_json()['data']

        response = self.client.get(f"/entry-decision?watch_stock_id={watch_stock['id']}")

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert '1688.00' in content
        assert 'PE 29.60' in content

    def test_entry_decision_page_selects_record_by_record_id(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-record-select.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        older = service.save_entry_decision_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-27',
                        'decision_card': {
                            'current_stage': 'B 修复初期',
                            'current_price_zone': '合理区',
                            'suggested_action': '适合买入',
                            'execution_summary': '第一条历史结论',
                        },
                        'risk_control_analysis': {'conclusion_summary': '第一条历史结论'},
                    },
                },
            }
        )
        latest = service.save_entry_decision_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-28',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-28',
                        'decision_card': {
                            'current_stage': 'C 右侧确认',
                            'current_price_zone': '偏高区',
                            'suggested_action': '继续观察',
                            'execution_summary': '第二条历史结论',
                        },
                        'risk_control_analysis': {'conclusion_summary': '第二条历史结论'},
                    },
                },
            }
        )

        response = self.client.get(f"/entry-decision?watch_stock_id={watch_stock['id']}&record_id={older['id']}")

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert older['id'] in content
        assert '第一条历史结论' in content
        assert "const SELECTED_RECORD =" in content
        assert 'trade_date\": \"2026-04-27\"' in content or 'trade_date&quot;: &quot;2026-04-27&quot;' in content
        assert '已加载历史决策记录' in content
        assert f'/entry-decision?watch_stock_id={watch_stock["id"]}&record_id={older["id"]}' in content

    def test_entry_decision_page_renders_paused_session_state(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-paused.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        session = service.create_entry_decision_session(
            watch_stock['id'],
            {
                'trade_date': '2026-04-27',
                'analysis_depth': 'deep',
                'position_input': {'current_position': '0%', 'max_target_position': ''},
            },
        )
        service.entry_decision_session_repository.update(
            session['id'],
            {
                'status': 'paused',
                'current_role': 'buy_plan_analysis',
                'missing_fields_json': ['position_input.max_target_position'],
                'pause_prompt': '买卖计划缺少仓位信息，请补充后继续。',
            },
        )

        response = self.client.get(f"/entry-decision?watch_stock_id={watch_stock['id']}")

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'ACTIVE_SESSION' in content
        assert '买卖计划缺少仓位信息，请补充后继续。' in content
        assert 'position_input.max_target_position' in content
        assert 'showPausePanel' in content
        assert '继续执行' in content

    def test_start_entry_decision_session_submits_background_task(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-start.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        session = service.create_entry_decision_session(
            watch_stock['id'],
            {
                'trade_date': '2026-04-26',
                'analysis_depth': 'deep',
                'position_input': {'current_position': '0%', 'max_target_position': '15%'},
            },
        )

        captured = {}

        class StubExecutor:
            def submit(self, fn, *args):
                captured['fn'] = fn
                captured['args'] = args
                future = Future()
                future.set_result(None)
                return future

        with self.app.app_context():
            context = trading_decision_routes_module._context()
            original_executor = context.executor
            context.executor = StubExecutor()
            try:
                ok, response, status_code = trading_decision_routes_module._start_entry_decision_session(session['id'], 'entry_client_1')
            finally:
                context.executor = original_executor

        assert ok is True
        assert response is None
        assert status_code == 200
        assert captured['fn'] is trading_decision_routes_module._run_entry_decision_session_task
        assert captured['args'][0] == session['id']
        assert captured['args'][1] == 'entry_client_1'
        assert captured['args'][3] is service

        with self.app.app_context():
            context = trading_decision_routes_module._context()
            task_key = f'entry_decision_{session["id"]}'
            assert task_key in context.analysis_tasks
            assert context.analysis_tasks[task_key]['client_id'] == 'entry_client_1'
            with context.task_lock:
                context.analysis_tasks.pop(task_key, None)
        assert captured['args'][2] is not None

    def test_start_entry_decision_session_rejects_duplicate_task(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-start-duplicate.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        session = service.create_entry_decision_session(watch_stock['id'], {'trade_date': '2026-04-26'})

        with self.app.app_context():
            context = trading_decision_routes_module._context()
            task_key = f'entry_decision_{session["id"]}'
            with context.task_lock:
                context.analysis_tasks[task_key] = {'status': 'analyzing'}
            try:
                ok, response, status_code = trading_decision_routes_module._start_entry_decision_session(session['id'], 'entry_client_1')
            finally:
                with context.task_lock:
                    context.analysis_tasks.pop(task_key, None)

        assert ok is False
        assert status_code == 429
        assert response.get_json()['error'] == '当前进场决策任务正在执行，请稍候'

    def test_start_entry_decision_session_cleans_up_on_submit_failure(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-start-failure.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        session = service.create_entry_decision_session(watch_stock['id'], {'trade_date': '2026-04-26'})

        class FailingExecutor:
            def submit(self, fn, *args):
                raise RuntimeError('submit failed')

        with self.app.app_context():
            context = trading_decision_routes_module._context()
            original_executor = context.executor
            context.executor = FailingExecutor()
            try:
                ok, response, status_code = trading_decision_routes_module._start_entry_decision_session(session['id'], 'entry_client_1')
                task_key = f'entry_decision_{session["id"]}'
                assert task_key not in context.analysis_tasks
            finally:
                context.executor = original_executor

        assert ok is False
        assert status_code == 500
        assert 'submit failed' in response.get_json()['error']

    def test_entry_decision_analyze_api_returns_cached_result_without_creating_session(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-cache-hit.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        cached_result = {
            'success': True,
            'data': {
                'watch_stock_id': watch_stock['id'],
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'trade_date': '2026-04-26',
                'entry_decision_summary_markdown': '## 进场决策\n\n- 结论：适合买入',
                'decision_card': {'suggested_action': '适合买入'},
                'result_source': 'cache',
                'cache_file': 'A股_600519_贵州茅台_Strategy_20260426_.md',
                'meta': {'result_source': 'cache', 'cache_file': 'A股_600519_贵州茅台_Strategy_20260426_.md'},
            },
        }

        monkeypatch.setattr(service, 'build_cached_entry_decision_result', lambda **kwargs: cached_result)
        monkeypatch.setattr(service, 'create_entry_decision_session', lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('should not create session')))
        monkeypatch.setattr(trading_decision_routes_module, '_start_entry_decision_session', lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('should not start session')))

        response = self.client.post(
            f"/api/trading-decision/watch-stocks/{watch_stock['id']}/entry-decision/analyze",
            json={
                'trade_date': '2026-04-26',
                'client_id': 'entry_client_cache',
            },
        )

        assert response.status_code == 200
        body = response.get_json()
        assert body['success'] is True
        assert body['data']['status'] == 'completed'
        assert body['data']['task_mode'] == 'cache'
        assert body['data']['client_id'] == 'entry_client_cache'
        assert body['data']['final_result']['data']['result_source'] == 'cache'
        assert body['data']['final_result']['data']['cache_file'] == 'A股_600519_贵州茅台_Strategy_20260426_.md'
        records = service.list_entry_decision_records(watch_stock['id'])
        assert len(records) == 1
        assert records[0]['trade_date'] == '2026-04-26'
        assert records[0]['suggested_action'] == '适合买入'

    def test_run_entry_decision_session_task_auto_saves_record_on_success(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-auto-save.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        session = service.create_entry_decision_session(
            watch_stock['id'],
            {
                'trade_date': '2026-04-26',
                'analysis_depth': 'standard',
                'position_input': {'current_position': '0%', 'max_target_position': '15%'},
            },
        )

        final_result = {
            'success': True,
            'data': {
                'trade_date': '2026-04-26',
                'decision_card': {
                    'current_stage': '右侧确认',
                    'current_price_zone': '合理区',
                    'suggested_action': '适合买入',
                    'execution_summary': '自动保存的历史结论',
                },
                'risk_control_analysis': {'conclusion_summary': '自动保存的历史结论'},
                'meta': {},
            },
        }

        class StubOrchestrator:
            def run(self, **kwargs):
                state = kwargs['state']
                state.status = 'completed'
                state.final_result = final_result
                return state

        class StubStreamer:
            def __init__(self, *args, **kwargs):
                pass
            def send_log(self, *args, **kwargs):
                pass
            def send_progress(self, *args, **kwargs):
                pass
            def send_role_result(self, *args, **kwargs):
                pass
            def send_pause(self, *args, **kwargs):
                pass
            def send_final_result(self, *args, **kwargs):
                pass
            def send_completion(self, *args, **kwargs):
                pass
            def send_error(self, *args, **kwargs):
                pass

        monkeypatch.setattr(trading_decision_routes_module, '_build_entry_decision_orchestrator', lambda: StubOrchestrator())
        monkeypatch.setattr(trading_decision_routes_module, 'StreamingAnalyzer', StubStreamer)

        with self.app.app_context():
            context = trading_decision_routes_module._context()
            trading_decision_routes_module._run_entry_decision_session_task(session['id'], 'entry_client_auto', context, service)

        records = service.list_entry_decision_records(watch_stock['id'])
        assert len(records) == 1
        assert records[0]['trade_date'] == '2026-04-26'
        assert records[0]['suggested_action'] == '适合买入'
        assert records[0]['conclusion_summary'] == '自动保存的历史结论'
        updated_watch_stock = service.get_watch_stock(watch_stock['id'])
        assert updated_watch_stock['last_conclusion_summary'] == '自动保存的历史结论'
        assert updated_watch_stock['last_analysis_at'] == '2026-04-26'

    def test_entry_decision_analyze_api_creates_session_and_starts_async_task(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-analyze.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        captured = {}

        def stub_start_entry_decision_session(session_id, client_id):
            captured['session_id'] = session_id
            captured['client_id'] = client_id
            return True, None, 200

        monkeypatch.setattr(trading_decision_routes_module, '_start_entry_decision_session', stub_start_entry_decision_session)
        monkeypatch.setattr(web_app_context.trading_decision_service, 'build_cached_entry_decision_result', lambda **kwargs: None)

        response = self.client.post(
            f"/api/trading-decision/watch-stocks/{watch_stock['id']}/entry-decision/analyze",
            json={
                'trade_date': '2026-04-26',
                'analysis_depth': 'deep',
                'client_id': 'entry_client_1',
                'position_input': {
                    'current_position': '0%',
                    'max_target_position': '15%',
                },
            },
        )

        assert response.status_code == 200
        body = response.get_json()
        assert body['success'] is True
        assert body['data']['status'] == 'running'
        assert body['data']['task_mode'] == 'async'
        assert body['data']['client_id'] == 'entry_client_1'
        assert body['data']['session_id'] == captured['session_id']
        assert body['data']['entry_decision_context']['watch_stock_id'] == watch_stock['id']

        session = web_app_context.trading_decision_service.get_entry_decision_session(captured['session_id'])
        assert session is not None
        assert session['watch_stock_id'] == watch_stock['id']
        assert session['request_json']['trade_date'] == '2026-04-26'
        assert session['request_json']['analysis_depth'] == 'deep'
        assert session['manual_inputs_json']['position_input']['current_position'] == '0%'
        assert session['manual_inputs_json']['position_input']['max_target_position'] == '15%'
        assert session['manual_inputs_json'].keys() == {'position_input'}

    def test_entry_decision_session_update_handles_dataframe_in_auto_context(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-dataframe-safe.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        session = service.create_entry_decision_session(watch_stock['id'], {'trade_date': '2026-04-26'})
        state = EntryDecisionState(
            session_id=session['id'],
            watch_stock_id=watch_stock['id'],
            request={'trade_date': '2026-04-26'},
            watch_stock=watch_stock,
            auto_context={'snapshot_df': pd.DataFrame([{'股票代码': '600519', '最新价': 1688.0}])},
            manual_inputs={'position_input': {'current_position': '0%', 'max_target_position': '15%'}},
            role_outputs={},
            current_role='macro_analysis',
            status='running',
        )

        updated = service.update_entry_decision_session_from_state(state)

        assert updated is not None
        assert isinstance(updated['auto_context_json']['snapshot_df'], list)
        assert updated['auto_context_json']['snapshot_df'][0]['股票代码'] == '600519'
        assert updated['auto_context_json']['snapshot_df'][0]['最新价'] == 1688.0

    def test_entry_decision_resume_api_merges_manual_inputs_and_restarts(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-resume.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        session = service.create_entry_decision_session(
            watch_stock['id'],
            {
                'trade_date': '2026-04-27',
                'analysis_depth': 'standard',
                'position_input': {'current_position': '0%', 'max_target_position': ''},
            },
        )
        service.entry_decision_session_repository.update(
            session['id'],
            {
                'status': 'paused',
                'current_role': 'buy_plan_analysis',
                'missing_fields_json': ['position_input.max_target_position'],
                'pause_prompt': '买卖计划缺少仓位或周期输入，请补充后继续。',
            },
        )

        captured = {}

        def stub_start_entry_decision_session(session_id, client_id):
            captured['session_id'] = session_id
            captured['client_id'] = client_id
            return True, None, 200

        monkeypatch.setattr(trading_decision_routes_module, '_start_entry_decision_session', stub_start_entry_decision_session)

        response = self.client.post(
            f"/api/trading-decision/entry-decisions/{session['id']}/resume",
            json={
                'client_id': 'resume_client_1',
                'position_input': {'max_target_position': '20%'},
            },
        )

        assert response.status_code == 200
        body = response.get_json()
        assert body['success'] is True
        assert body['data']['session_id'] == session['id']
        assert body['data']['client_id'] == 'resume_client_1'
        assert captured['session_id'] == session['id']

        updated = service.get_entry_decision_session(session['id'])
        assert updated['status'] == 'running'
        assert updated['manual_inputs_json']['position_input']['current_position'] == '0%'
        assert updated['manual_inputs_json']['position_input']['max_target_position'] == '20%'
        assert updated['missing_fields_json'] == []

    def test_entry_decision_record_api_persists_and_updates_watch_stock(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-record.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        raw_result = {
            'success': True,
            'data': {
                'trade_date': '2026-04-27',
                'value_stage_analysis': {'current_stage': 'B 修复初期', 'stage_reasoning': '盈利修复'},
                'price_zone_analysis': {'price_zone': '合理区', 'zone_reasoning': '估值回到历史中枢'},
                'buy_plan_analysis': {'suggested_action': '适合买入', 'suggested_entry_leg': '第一笔'},
                'risk_control_analysis': {'conclusion_summary': '可小仓位试错，等待确认后再加仓。'},
                'decision_card': {
                    'current_stage': 'B 修复初期',
                    'current_price_zone': '合理区',
                    'suggested_action': '适合买入',
                    'suggested_entry_leg': '第一笔',
                    'execution_summary': '可小仓位试错，等待确认后再加仓。',
                },
            },
        }

        save_response = self.client.post(
            '/api/trading-decision/entry-decision-records',
            json={
                'watch_stock_id': watch_stock['id'],
                'session_id': 'EDS-TEST001',
                'trade_date': '2026-04-27',
                'raw_result': raw_result,
            },
        )

        assert save_response.status_code == 200
        saved = save_response.get_json()['data']
        assert saved['watch_stock_id'] == watch_stock['id']
        assert saved['current_stage'] == 'B 修复初期'
        assert saved['current_price_zone'] == '合理区'
        assert saved['suggested_action'] == '适合买入'
        assert saved['suggested_entry_leg'] == '第一笔'
        assert saved['conclusion_summary'] == '可小仓位试错，等待确认后再加仓。'

        list_response = self.client.get(f"/api/trading-decision/entry-decision-records?watch_stock_id={watch_stock['id']}")
        assert list_response.status_code == 200
        records = list_response.get_json()['data']
        assert len(records) == 1
        assert records[0]['id'] == saved['id']

        detail_response = self.client.get(f"/api/trading-decision/entry-decision-records/{saved['id']}")
        assert detail_response.status_code == 200
        detail = detail_response.get_json()['data']
        assert detail['decision_card_json']['suggested_entry_leg'] == '第一笔'
        assert detail['full_result_json']['data']['decision_card']['current_stage'] == 'B 修复初期'

        watch_stock_response = self.client.get(f"/api/trading-decision/watch-stocks/{watch_stock['id']}")
        updated_watch_stock = watch_stock_response.get_json()['data']
        assert updated_watch_stock['current_stage'] == 'B 修复初期'
        assert updated_watch_stock['current_price_zone'] == '合理区'
        assert updated_watch_stock['suggested_action'] == '适合买入'
        assert updated_watch_stock['last_conclusion_summary'] == '可小仓位试错，等待确认后再加仓。'
        assert updated_watch_stock['last_analysis_at'] == '2026-04-27'

    def test_entry_decision_page_save_fields_roundtrip_through_watch_stock_api(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-save.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        save_response = self.client.put(
            f"/api/trading-decision/watch-stocks/{watch_stock['id']}",
            json={
                'current_stage': '准备建仓',
                'current_price_zone': '合理区',
                'suggested_action': '适合买入',
                'last_conclusion_summary': '技术面和基本面共振，可小仓位试错。',
                'last_analysis_at': '2026-04-26',
            },
        )

        assert save_response.status_code == 200
        saved = save_response.get_json()['data']
        assert saved['current_stage'] == '准备建仓'
        assert saved['current_price_zone'] == '合理区'
        assert saved['suggested_action'] == '适合买入'
        assert saved['last_conclusion_summary'] == '技术面和基本面共振，可小仓位试错。'
        assert saved['last_analysis_at'] == '2026-04-26'

    def test_watch_stocks_page_updates_entry_decision_copy(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'watch-stocks-entry-copy.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/watch-stocks')

        assert response.status_code == 200
        assert '进场决策与股票分析已接入真实页面'.encode() in response.data
        assert '三个业务入口暂时仍跳转到现有壳页'.encode() not in response.data
        assert '/entry-decision?watch_stock_id='.encode() in response.data
        assert '同级历史记录列表'.encode() in response.data
        assert '进场决策记录'.encode() in response.data
        assert '股票分析记录'.encode() in response.data
        assert '持仓计划分析记录'.encode() in response.data
        assert '股票分析记录待接入独立历史库'.encode() not in response.data
        assert '暂无股票分析记录'.encode() in response.data

    def test_trade_plan_analysis_page_renders_history_links(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'trade-plan-history-links.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        saved = service.save_trade_plan_analysis_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-28',
                'plan_type': '三笔计划',
                'risk_preference': '中高风险',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-28',
                        'market': 'A股',
                        'stock_code': '600519',
                        'stock_name': '贵州茅台',
                        'trade_plan_markdown': '## 计划摘要\n\n- 第一条计划',
                        'decision': {
                            'action': 'buy',
                            'summary': '第一条持仓计划结论',
                            'risk_level': 'medium',
                            'position_suggestion': {'target_position': '20%'}
                        },
                        'meta': {'template_name': '持仓计划模板（买前执行版）', 'data_source': 'cache_first'},
                    },
                },
            }
        )

        response = self.client.get(f"/trade-plan-analysis?watch_stock_id={watch_stock['id']}&record_id={saved['id']}")

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert '持仓计划分析历史记录' in content
        assert saved['id'] in content
        assert f'/trade-plan-analysis?watch_stock_id={watch_stock["id"]}&record_id={saved["id"]}' in content
        assert '第一条持仓计划结论' in content
        assert '三笔计划' in content

    def test_history_center_page_renders_tabs_and_summary_cards(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'history-center-page.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/history-center')

        assert response.status_code == 200
        assert '历史记录中心'.encode() in response.data
        assert '进场决策记录'.encode() in response.data
        assert '股票分析记录'.encode() in response.data
        assert '持仓计划记录'.encode() in response.data
        assert '文件档案'.encode() in response.data
        assert '数据库负责主列表检索'.encode() in response.data
        assert 'data-page="history-center"'.encode() in response.data

    def test_watch_stocks_page_links_to_history_center_and_supports_main_pagination(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'watch-stocks-main-pagination.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        for index in range(21):
            self.client.post(
                '/api/trading-decision/watch-stocks',
                json={
                    'stock_code': f'6005{index:02d}',
                    'stock_name': f'样例股票{index}',
                    'market': 'A股',
                    'asset_type': '成长龙头',
                },
            )

        response = self.client.get('/watch-stocks?page=2&page_size=20')

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert '进入历史记录中心' in content
        assert 'href="/history-center"' in content
        assert '第 2 页 · 每页 20 条 · 共 21 条' in content
        assert '/watch-stocks?page=1&amp;page_size=20&amp;history_page=1&amp;history_page_size=5&amp;history_type=all' in content
        assert '/watch-stocks?page=3&amp;page_size=20&amp;history_page=1&amp;history_page_size=5&amp;history_type=all' in content

    def test_watch_stocks_history_links_open_exact_record_pages(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'watch-stocks-history-links.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        entry_record = service.save_entry_decision_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-27',
                        'decision_card': {'suggested_action': '适合买入'},
                        'risk_control_analysis': {'conclusion_summary': '进场历史结论'},
                    },
                },
            }
        )
        stock_record = service.save_stock_analysis_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'raw_result': {
                    'success': True,
                    'data': {
                        'stock_code': '600519',
                        'stock_name': '贵州茅台',
                        'market': 'A股',
                        'trade_date': '2026-04-27',
                        'stance': 'bullish',
                        'decision': {'summary': '分析历史结论', 'risk_level': 'medium'},
                        'scores': {'composite': 75},
                        'signals': [],
                        'risks': [],
                        'evidence': [],
                        'meta': {},
                        'snapshot': {},
                    },
                },
            }
        )
        trade_record = service.save_trade_plan_analysis_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'plan_type': '三笔计划',
                'risk_preference': '中高风险',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-27',
                        'decision': {'action': 'buy', 'summary': '计划历史结论', 'risk_level': 'medium', 'position_suggestion': {}},
                        'scores': {'composite': 75},
                    },
                },
            }
        )

        response = self.client.get('/watch-stocks')

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert f'/entry-decision?watch_stock_id={watch_stock["id"]}&amp;record_id={entry_record["id"]}' in content
        assert f'/stock-analysis-record?watch_stock_id={watch_stock["id"]}&amp;record_id={stock_record["id"]}' in content
        assert f'/trade-plan-analysis?watch_stock_id={watch_stock["id"]}&amp;record_id={trade_record["id"]}' in content
        assert '/index#watch-records' not in content

    def test_watch_stocks_history_panel_supports_type_filter_and_pagination(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'watch-stocks-history-panel.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        first_watch = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        ).get_json()['data']
        second_watch = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '高弹性主题',
            },
        ).get_json()['data']

        service.save_entry_decision_record(
            {
                'watch_stock_id': first_watch['id'],
                'trade_date': '2026-04-27',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-27',
                        'decision_card': {'suggested_action': '适合买入'},
                        'risk_control_analysis': {'conclusion_summary': '第一页不该出现'},
                    },
                },
            }
        )
        service.save_entry_decision_record(
            {
                'watch_stock_id': second_watch['id'],
                'trade_date': '2026-04-28',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-28',
                        'decision_card': {'suggested_action': '继续观察'},
                        'risk_control_analysis': {'conclusion_summary': '第二页目标记录'},
                    },
                },
            }
        )
        service.save_stock_analysis_record(
            {
                'watch_stock_id': first_watch['id'],
                'trade_date': '2026-04-29',
                'raw_result': {
                    'success': True,
                    'data': {
                        'stock_code': '600519',
                        'stock_name': '贵州茅台',
                        'market': 'A股',
                        'trade_date': '2026-04-29',
                        'stance': 'bullish',
                        'decision': {'summary': '不应出现在进场筛选里', 'risk_level': 'medium'},
                        'scores': {'composite': 75},
                        'signals': [],
                        'risks': [],
                        'evidence': [],
                        'meta': {},
                        'snapshot': {},
                    },
                },
            }
        )

        response = self.client.get('/watch-stocks?history_type=entry-decision&history_page=2&history_page_size=1')

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert '当前仅展示含进场决策记录的标的。' in content
        assert '第 2 页 · 每页 1 组 · 共 2 组' in content
        assert '第二页目标记录' in content
        assert '第一页不该出现' not in content
        assert '<option value="entry-decision" selected>仅进场决策</option>' in content
        assert '/watch-stocks?page=1&amp;page_size=20&amp;history_page=1&amp;history_page_size=1&amp;history_type=entry-decision' in content
        assert '/watch-stocks?page=1&amp;page_size=20&amp;history_page=3&amp;history_page_size=1&amp;history_type=entry-decision' in content

    def test_history_center_page_supports_filters_and_pagination(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'history-center-pagination.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        first_watch = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
                'current_stage': '观察期',
                'current_price_zone': '合理区',
            },
        ).get_json()['data']
        second_watch = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '高弹性主题',
                'current_stage': '突破期',
                'current_price_zone': '突破区',
            },
        ).get_json()['data']

        first_record = service.save_entry_decision_record(
            {
                'watch_stock_id': first_watch['id'],
                'trade_date': '2026-04-27',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-27',
                        'decision_card': {
                            'current_stage': '观察期',
                            'current_price_zone': '合理区',
                            'suggested_action': '适合买入',
                        },
                        'risk_control_analysis': {'conclusion_summary': '第一条历史结论'},
                    },
                },
            }
        )
        second_record = service.save_entry_decision_record(
            {
                'watch_stock_id': first_watch['id'],
                'trade_date': '2026-04-28',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-28',
                        'decision_card': {
                            'current_stage': '观察期',
                            'current_price_zone': '合理区',
                            'suggested_action': '继续观察',
                        },
                        'risk_control_analysis': {'conclusion_summary': '第二条历史结论'},
                    },
                },
            }
        )
        third_record = service.save_entry_decision_record(
            {
                'watch_stock_id': first_watch['id'],
                'trade_date': '2026-04-29',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-29',
                        'decision_card': {
                            'current_stage': '观察期',
                            'current_price_zone': '合理区',
                            'suggested_action': '持有待涨',
                        },
                        'risk_control_analysis': {'conclusion_summary': '第三条历史结论'},
                    },
                },
            }
        )
        service.save_entry_decision_record(
            {
                'watch_stock_id': second_watch['id'],
                'trade_date': '2026-04-29',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-29',
                        'decision_card': {
                            'current_stage': '突破期',
                            'current_price_zone': '突破区',
                            'suggested_action': '继续观察',
                        },
                        'risk_control_analysis': {'conclusion_summary': '不应出现在筛选结果中'},
                    },
                },
            }
        )

        response = self.client.get('/history-center?tab=entry-decision&page=2&page_size=2&keyword=贵州&asset_type=成长龙头&stage=观察期&price_zone=合理区')

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert '第 2 页 · 每页 2 条 · 共 3 条' in content
        assert '关键字：贵州 / 资产类型：成长龙头 / 阶段：观察期 / 价格区：合理区' in content
        assert first_record['id'] in content
        assert second_record['id'] not in content
        assert third_record['id'] not in content
        assert '不应出现在筛选结果中' not in content
        assert '/history-center?tab=entry-decision&amp;page=1&amp;page_size=2&amp;keyword=%E8%B4%B5%E5%B7%9E&amp;asset_type=%E6%88%90%E9%95%BF%E9%BE%99%E5%A4%B4&amp;stage=%E8%A7%82%E5%AF%9F%E6%9C%9F&amp;price_zone=%E5%90%88%E7%90%86%E5%8C%BA' in content
        assert '/history-center?tab=entry-decision&amp;page=3&amp;page_size=2&amp;keyword=%E8%B4%B5%E5%B7%9E&amp;asset_type=%E6%88%90%E9%95%BF%E9%BE%99%E5%A4%B4&amp;stage=%E8%A7%82%E5%AF%9F%E6%9C%9F&amp;price_zone=%E5%90%88%E7%90%86%E5%8C%BA' in content

    def test_history_center_page_renders_real_history_rows(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'history-center-records.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        service.save_entry_decision_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-27',
                        'value_stage_analysis': {'current_stage': '修复初期'},
                        'price_zone_analysis': {'price_zone': '合理区'},
                        'buy_plan_analysis': {'suggested_action': '适合买入'},
                        'risk_control_analysis': {'conclusion_summary': '趋势修复，可小仓位试错。'},
                        'decision_card': {
                            'current_stage': '修复初期',
                            'current_price_zone': '合理区',
                            'suggested_action': '适合买入',
                        },
                    },
                },
            }
        )
        service.save_stock_analysis_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-27',
                        'analysis_mode': 'agentic',
                        'stance': 'bullish',
                        'time_horizon': '3-10 trading days',
                        'decision': {'summary': '趋势修复，适合继续跟踪。', 'risk_level': 'medium'},
                        'scores': {'composite': 75},
                        'signals': [],
                        'risks': [],
                        'evidence': [],
                        'meta': {},
                        'snapshot': {},
                    },
                },
            }
        )
        service.save_trade_plan_analysis_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'plan_type': '三笔计划',
                'risk_preference': '中高风险',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-27',
                        'decision': {
                            'action': 'buy',
                            'summary': '回踩后具备分批建仓条件。',
                            'risk_level': 'medium',
                            'position_suggestion': {'target_position': '30%-50%', 'position_limit': '30%-50%'},
                        },
                        'scores': {'composite': 75},
                    },
                },
            }
        )

        all_response = self.client.get('/history-center?tab=all')
        assert all_response.status_code == 200
        assert '贵州茅台'.encode() in all_response.data
        assert '进场决策记录（1）'.encode() in all_response.data
        assert '股票分析记录（1）'.encode() in all_response.data
        assert '持仓计划记录（1）'.encode() in all_response.data

        entry_response = self.client.get('/history-center?tab=entry-decision')
        assert entry_response.status_code == 200
        assert '修复初期'.encode() in entry_response.data
        assert f'watch_stock_id={watch_stock["id"]}'.encode() in entry_response.data
        assert 'record_id='.encode() in entry_response.data
        assert '/entry-decision?'.encode() in entry_response.data

        stock_response = self.client.get('/history-center?tab=stock-analysis')
        assert stock_response.status_code == 200
        assert 'bullish'.encode() in stock_response.data
        assert '75'.encode() in stock_response.data
        assert f'watch_stock_id={watch_stock["id"]}'.encode() in stock_response.data
        assert '/stock-analysis-record?'.encode() in stock_response.data

        holding_stock = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-28',
                'quantity': 100,
                'price': 1680,
                'current_price': 1688,
                'note': '转持仓',
            },
        ).get_json()['data']
        service.save_stock_analysis_record(
            {
                'holding_stock_id': holding_stock['id'],
                'analysis_scene': 'holding_reanalysis',
                'trade_date': '2026-04-28',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-28',
                        'analysis_mode': 'agentic',
                        'stance': 'hold',
                        'time_horizon': '10-30 trading days',
                        'logic': '原始买入逻辑未破坏。',
                        'decision': {'summary': '继续持有并等待验证。', 'risk_level': 'medium'},
                        'scores': {'composite': 73},
                        'signals': [],
                        'risks': [],
                        'evidence': [],
                        'meta': {},
                        'snapshot': {},
                    },
                },
            }
        )

        holding_stock_response = self.client.get('/history-center?tab=stock-analysis')
        assert holding_stock_response.status_code == 200
        assert f'holding_stock_id={holding_stock["id"]}'.encode() in holding_stock_response.data
        assert '/holding-reanalysis?'.encode() in holding_stock_response.data

        trade_response = self.client.get('/history-center?tab=trade-plan')
        assert trade_response.status_code == 200
        assert '三笔计划'.encode() in trade_response.data
        assert '30%-50%'.encode() in trade_response.data
        assert f'watch_stock_id={watch_stock["id"]}'.encode() in trade_response.data
        assert '/trade-plan-analysis?'.encode() in trade_response.data
        assert 'record_id='.encode() in trade_response.data

        files_response = self.client.get('/history-center?tab=files')
        assert files_response.status_code == 200
        assert '/history'.encode() in files_response.data
        assert '旧版文件档案'.encode() in files_response.data

    def test_entry_decision_page_uses_entry_sidebar_state(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-sidebar.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        response = self.client.get(f"/entry-decision?watch_stock_id={watch_stock['id']}")

        assert response.status_code == 200
        assert 'data-page="entry-decision"'.encode() in response.data
        assert '主按钮文案为“生成决策”'.encode() not in response.data
        assert 'saveDecisionRecord()'.encode() in response.data
        assert 'watchStockIdText'.encode() in response.data

    def test_entry_decision_page_renders_richer_result_sections(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-rich-sections.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        response = self.client.get(f"/entry-decision?watch_stock_id={watch_stock['id']}")

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'renderTabbedResults' in content
        assert 'renderSummaryTab' in content
        assert 'renderRoleTab' in content
        assert 'entry_decision_summary_markdown' in content
        assert 'marked.parse' in content
        assert 'result-tabs' in content
        assert 'tab-nav' in content
        assert '宏观分析师' in content
        assert '资产分类分析师' in content
        assert '价值阶段分析师' in content
        assert '价格分区分析师' in content
        assert '买卖计划分析师' in content
        assert '风险控制分析师' in content
        assert '总结' in content
        assert '查看完整结果 JSON' in content
        assert 'showPausePanel' in content
        assert 'decision_pause' in content
        assert 'decision_role_result' in content
        assert 'renderStructuredPosition' not in content
        assert 'renderResearchDebate' not in content
        assert 'buildResultSections' not in content
        assert '查看 final_state 原始数据' not in content
        assert '研究经理结论' not in content
        assert '交易员执行建议' not in content
        assert '关键信号' not in content
        assert '证据链' not in content
        assert '结果区会展示分析师结果、总结，以及原始 JSON。' in content
        assert 'render_template(\'entry_decision.html\')' not in content
        assert '生成AI决策建议' not in content
        assert 'saveDecision()' not in content
        assert 'resumeDecision()' in content
        assert 'saveDecisionRecord()' in content

    def test_entry_decision_session_detail_api_returns_saved_session(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-session-detail.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        session = service.create_entry_decision_session(
            watch_stock['id'],
            {'trade_date': '2026-04-27', 'analysis_depth': 'standard'},
        )

        response = self.client.get(f"/api/trading-decision/entry-decisions/{session['id']}")

        assert response.status_code == 200
        body = response.get_json()['data']
        assert body['id'] == session['id']
        assert body['watch_stock_id'] == watch_stock['id']
        assert body['request_json']['trade_date'] == '2026-04-27'
        assert body['status'] == 'running'

    def test_entry_decision_page_can_render_saved_record(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-record-page.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        service.save_entry_decision_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-27',
                        'basic_info': {'stock_code': '300750', 'stock_name': '宁德时代'},
                        'macro_analysis': {'macro_conclusion': '中性偏强', 'macro_reasoning': '风险偏好修复'},
                        'entry_decision_summary_markdown': '## 一、标的基本信息\n\n- 标的名称：宁德时代\n\n## 十、最终一页决策卡\n\n- 当前结论：适合买入',
                        'decision_card': {'suggested_action': '适合买入', 'execution_summary': '先小仓试错'},
                    },
                },
            }
        )

        response = self.client.get(f"/entry-decision?watch_stock_id={watch_stock['id']}")

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'SELECTED_RECORD' in content
        assert '先小仓试错' in content
        assert '适合买入' in content
        assert 'entry_decision_summary_markdown' in content
        assert 'markdown-summary' in content
        assert 'renderResult(SELECTED_RECORD.full_result_json)' in content

    def test_entry_decision_page_script_collects_only_auto_sourced_payload_fields(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-payload-fields.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        response = self.client.get(f"/entry-decision?watch_stock_id={watch_stock['id']}")

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'position_input' in content
        assert 'current_position' in content
        assert 'max_target_position' in content
        assert "investment_horizon: document.getElementById('investmentHorizon')" not in content
        assert "expectation_summary: document.getElementById('expectationSummary')" not in content
        assert 'financial_summary:' not in content
        assert 'valuation_input:' not in content
        assert "body: JSON.stringify(collectPayload())" in content

    def test_entry_decision_page_can_render_final_result_sections_from_saved_result(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-render-sections.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        service.save_entry_decision_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-27',
                        'basic_info': {'stock_code': '600519', 'stock_name': '贵州茅台', 'current_price': 1688.0},
                        'macro_analysis': {'macro_conclusion': '中性偏强', 'macro_reasoning': '流动性稳定'},
                        'asset_classification': {'asset_classification': '成长龙头', 'classification_reasoning': '行业龙头溢价'},
                        'value_stage_analysis': {'current_stage': '修复初期', 'stage_reasoning': '利润和现金流改善'},
                        'price_zone_analysis': {'price_zone': '合理区', 'zone_reasoning': '估值回到中枢'},
                        'buy_plan_analysis': {'suggested_action': '适合买入', 'action_reasoning': '可分笔试错'},
                        'risk_control_analysis': {'conclusion_summary': '先小仓位，跌破关键位止损'},
                        'entry_decision_summary_markdown': '## 一、标的基本信息\n\n- 标的名称：贵州茅台\n\n---\n\n## 十、最终一页决策卡\n\n- 当前结论：适合买入\n\n## 十二、使用纪律\n\n1. 不跳步骤。',
                        'decision_card': {'suggested_action': '适合买入', 'execution_summary': '先小仓位，跌破关键位止损'},
                    },
                },
            }
        )

        response = self.client.get(f"/entry-decision?watch_stock_id={watch_stock['id']}")

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'renderSummaryTab' in content
        assert 'renderTabbedResults' in content
        assert 'ROLE_TAB_CONFIG' in content
        assert 'entry_decision_summary_markdown' in content
        assert 'markdown-summary' in content
        assert '宏观分析师' in content
        assert '价值阶段分析师' in content
        assert '风险控制分析师' in content
        assert '总结' in content
        assert 'renderResult(SELECTED_RECORD.full_result_json)' in content
        assert '先小仓位，跌破关键位止损' in content

    def test_entry_decision_page_returns_not_found_for_unknown_watch_stock(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-404.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/entry-decision?watch_stock_id=WS-UNKNOWN')

        assert response.status_code == 404
        assert response.get_json()['error']['code'] == 'not_found'

    def test_entry_decision_analyze_api_uses_watch_stock_context(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-analyze.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        captured = {}

        def stub_start_entry_decision_session(session_id, client_id):
            captured['session_id'] = session_id
            captured['client_id'] = client_id
            return True, None, 200

        monkeypatch.setattr(trading_decision_routes_module, '_start_entry_decision_session', stub_start_entry_decision_session)
        monkeypatch.setattr(web_app_context.trading_decision_service, 'build_cached_entry_decision_result', lambda **kwargs: None)

        response = self.client.post(
            f"/api/trading-decision/watch-stocks/{watch_stock['id']}/entry-decision/analyze",
            json={
                'trade_date': '2026-04-26',
                'analysis_depth': 'deep',
                'client_id': 'entry_client_1',
                'current_stage': '观察中',
                'current_price_zone': '合理区',
                'suggested_action': '继续观察',
                'last_conclusion_summary': '手工备注',
            },
        )

        assert response.status_code == 200
        body = response.get_json()
        assert body['success'] is True
        assert body['data']['client_id'] == 'entry_client_1'
        assert body['data']['entry_decision_context']['watch_stock_id'] == watch_stock['id']
        assert body['data']['entry_decision_context']['stock_code'] == '600519'
        assert body['data']['entry_decision_context']['trade_date'] == '2026-04-26'
        assert body['data']['entry_decision_context']['analysis_depth'] == 'deep'
        assert body['data']['entry_decision_context']['pending_save_fields']['current_stage'] == ''
        assert body['data']['entry_decision_context']['pending_save_fields']['current_price_zone'] == ''
        assert body['data']['session_id'] == captured['session_id']

        session = web_app_context.trading_decision_service.get_entry_decision_session(body['data']['session_id'])
        assert session is not None
        assert session['watch_stock_id'] == watch_stock['id']
        assert session['request_json']['trade_date'] == '2026-04-26'
        assert session['request_json']['analysis_depth'] == 'deep'
        assert session['manual_inputs_json'].keys() == {'position_input'}
        assert session['manual_inputs_json']['position_input']['current_position'] == ''
        assert session['manual_inputs_json']['position_input']['max_target_position'] == ''
        assert not hasattr(trading_decision_routes_module, 'start_stock_ai_analysis') or True

    def test_entry_decision_page_save_fields_roundtrip_through_watch_stock_api(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-save.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        save_response = self.client.put(
            f"/api/trading-decision/watch-stocks/{watch_stock['id']}",
            json={
                'current_stage': '准备建仓',
                'current_price_zone': '合理区',
                'suggested_action': '适合买入',
                'last_conclusion_summary': '技术面和基本面共振，可小仓位试错。',
                'last_analysis_at': '2026-04-26',
            },
        )

        assert save_response.status_code == 200
        saved = save_response.get_json()['data']
        assert saved['current_stage'] == '准备建仓'
        assert saved['current_price_zone'] == '合理区'
        assert saved['suggested_action'] == '适合买入'
        assert saved['last_conclusion_summary'] == '技术面和基本面共振，可小仓位试错。'
        assert saved['last_analysis_at'] == '2026-04-26'

    def test_watch_stocks_page_updates_entry_decision_copy(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'watch-stocks-entry-copy.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        response = self.client.get('/watch-stocks')

        assert response.status_code == 200
        assert '进场决策、股票分析与持仓计划分析均已接入真实页面'.encode() in response.data
        assert '三个业务入口暂时仍跳转到现有壳页'.encode() not in response.data
        assert f"/entry-decision?watch_stock_id={watch_stock['id']}".encode() in response.data

    def test_entry_decision_page_uses_entry_sidebar_state(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-sidebar.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        response = self.client.get(f"/entry-decision?watch_stock_id={watch_stock['id']}")

        assert response.status_code == 200
        assert 'data-page="entry-decision"'.encode() in response.data
        assert '主按钮文案为“生成决策”'.encode() not in response.data
        assert 'saveDecisionRecord()'.encode() in response.data
        assert 'watchStockIdText'.encode() in response.data

    def test_entry_decision_page_renders_richer_result_sections(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-decision-rich-sections.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        response = self.client.get(f"/entry-decision?watch_stock_id={watch_stock['id']}")

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'renderTabbedResults' in content
        assert 'renderSummaryTab' in content
        assert 'renderRoleTab' in content
        assert 'entry_decision_summary_markdown' in content
        assert 'marked.parse' in content
        assert 'result-tabs' in content
        assert 'tab-nav' in content
        assert '宏观分析师' in content
        assert '资产分类分析师' in content
        assert '价值阶段分析师' in content
        assert '价格分区分析师' in content
        assert '买卖计划分析师' in content
        assert '风险控制分析师' in content
        assert '总结' in content
        assert '查看完整结果 JSON' in content
        assert 'showPausePanel' in content
        assert 'decision_pause' in content
        assert 'decision_role_result' in content
        assert 'renderStructuredPosition' not in content
        assert 'renderResearchDebate' not in content
        assert 'buildResultSections' not in content
        assert '查看 final_state 原始数据' not in content
        assert '研究经理结论' not in content
        assert '交易员执行建议' not in content
        assert '关键信号' not in content
        assert '证据链' not in content
        assert '结果区会展示分析师结果、总结，以及原始 JSON。' in content
        assert 'render_template(\'entry_decision.html\')' not in content
        assert '生成AI决策建议' not in content
        assert 'saveDecision()' not in content
        assert 'resumeDecision()' in content
        assert 'saveDecisionRecord()' in content

    def test_trade_plan_analysis_page_renders_real_template(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'trade-plan-page.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
                'industry': '白酒',
                'current_price': 1688.0,
                'pe': 29.6,
            },
        )
        watch_stock = create_response.get_json()['data']

        response = self.client.get(f"/trade-plan-analysis?watch_stock_id={watch_stock['id']}")

        assert response.status_code == 200
        assert '持仓计划分析'.encode() in response.data
        assert '生成计划草案'.encode() in response.data
        assert '保存计划分析记录'.encode() in response.data
        assert '/trade-plan-analysis/run'.encode() in response.data
        assert '/api/trading-decision/trade-plan-analysis-records'.encode() in response.data
        assert 'watchStockIdText'.encode() in response.data
        assert watch_stock['stock_code'].encode() in response.data
        assert 'data-page="trade-plan-analysis"'.encode() in response.data

        content = response.data.decode('utf-8')
        assert 'trade_plan_markdown' in content
        assert 'marked.parse' in content
        assert '当前结果尚未生成模板化持仓计划，已回退到结构化视图。' in content
        assert '模板：' in content
        assert 'renderTradePlanResult' in content
        assert 'start_stock_ai_analysis' not in content
        assert 'build_stock_ai_payload' not in content
        assert '持仓计划模板（买前执行版）' in content
        assert '查看原始 JSON' in content
        assert '结果区会展示分析师结果、总结，以及原始 JSON。' not in content

    def test_position_decision_page_renders_real_template(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'position-decision-page.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        watch_stock = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
                'industry': '白酒',
            },
        ).get_json()['data']
        holding_stock = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 100,
                'price': 1680,
                'current_price': 1688,
                'note': '首笔建仓',
            },
        ).get_json()['data']

        response = self.client.get(f"/position-decision?holding_stock_id={holding_stock['id']}")

        assert response.status_code == 200
        assert '买卖决策'.encode() in response.data
        assert '生成买卖决策'.encode() in response.data
        assert '保存买卖决策记录'.encode() in response.data
        assert '/position-decisions/run'.encode() in response.data
        assert '/api/trading-decision/position-decision-records'.encode() in response.data
        assert 'holdingStockIdText'.encode() in response.data
        assert 'data-page="position-decision"'.encode() in response.data

        content = response.data.decode('utf-8')
        assert '股票分析师' in content
        assert '触发条件、核心理由、执行注意事项、风险分析、结论' in content
        assert '页面不会再要求手动选择动作' in content
        assert '决策类型' not in content

    def test_position_decision_run_api_starts_dedicated_async_task(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'position-decision-run.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        watch_stock = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        ).get_json()['data']
        holding_stock = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 100,
                'price': 1680,
                'current_price': 1688,
                'note': '首笔建仓',
            },
        ).get_json()['data']
        captured = {}

        def stub_start_position_decision_task(holding_stock_id, client_id, position_context):
            captured['holding_stock_id'] = holding_stock_id
            captured['client_id'] = client_id
            captured['position_context'] = position_context
            return True, None, 200

        monkeypatch.setattr(trading_decision_routes_module, '_start_position_decision_task', stub_start_position_decision_task)

        response = self.client.post(
            f"/api/trading-decision/holding-stocks/{holding_stock['id']}/position-decisions/run",
            json={
                'trade_date': '2026-04-29',
                'analysis_depth': 'deep',
                'client_id': 'position_decision_client_1',
            },
        )

        assert response.status_code == 200
        body = response.get_json()
        assert body['success'] is True
        assert captured['holding_stock_id'] == holding_stock['id']
        assert captured['client_id'] == 'position_decision_client_1'
        assert captured['position_context']['holding_stock']['stock_code'] == '600519'
        assert captured['position_context']['request']['trade_date'] == '2026-04-29'
        assert captured['position_context']['request']['analysis_depth'] == 'deep'
        context = body['data']['position_decision_context']
        assert context['holding_stock_id'] == holding_stock['id']
        assert context['watch_stock_id'] == watch_stock['id']
        assert context['role'] == '股票分析师'
        assert context['data_sources'] == ['financial_context', 'trade_history_context', 'holding_plan_context']

    def test_holding_review_page_renders_real_template(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'holding-review-page.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        watch_stock = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
                'industry': '白酒',
            },
        ).get_json()['data']
        holding_stock = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 100,
                'price': 1680,
                'current_price': 1688,
                'note': '首笔建仓',
            },
        ).get_json()['data']

        response = self.client.get(f"/holding-review?holding_stock_id={holding_stock['id']}")

        assert response.status_code == 200
        assert '持仓复盘'.encode() in response.data
        assert '生成复盘草案'.encode() in response.data
        assert '保存复盘记录'.encode() in response.data
        assert '/reviews/run'.encode() in response.data
        assert '/api/trading-decision/holding-review-records'.encode() in response.data
        assert 'data-page="holding-review"'.encode() in response.data

        content = response.get_data(as_text=True)
        assert '交易专家' in content
        assert '执行与卖出复盘、结果复盘、方法与纪律、后续动作' in content
        assert 'periodKey' in content
        assert 'reviewType' in content
        assert 'positionDecisionPrefill' not in content

    def test_holding_review_run_api_starts_dedicated_async_task(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'holding-review-run.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        watch_stock = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        ).get_json()['data']
        holding_stock = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 100,
                'price': 1680,
                'current_price': 1688,
                'note': '首笔建仓',
            },
        ).get_json()['data']
        captured = {}

        def stub_start_holding_review_task(holding_stock_id, client_id, holding_review_context):
            captured['holding_stock_id'] = holding_stock_id
            captured['client_id'] = client_id
            captured['holding_review_context'] = holding_review_context
            return True, None, 200

        monkeypatch.setattr(trading_decision_routes_module, '_start_holding_review_task', stub_start_holding_review_task)

        response = self.client.post(
            f"/api/trading-decision/holding-stocks/{holding_stock['id']}/reviews/run",
            json={
                'trade_date': '2026-04-29',
                'review_type': 'weekly',
                'period_key': '2026-W18',
                'analysis_depth': 'deep',
                'client_id': 'holding_review_client_1',
            },
        )

        assert response.status_code == 200
        body = response.get_json()
        assert body['success'] is True
        assert captured['holding_stock_id'] == holding_stock['id']
        assert captured['client_id'] == 'holding_review_client_1'
        assert captured['holding_review_context']['holding_stock']['stock_code'] == '600519'
        assert captured['holding_review_context']['request']['trade_date'] == '2026-04-29'
        assert captured['holding_review_context']['request']['review_type'] == 'weekly'
        assert captured['holding_review_context']['request']['period_key'] == '2026-W18'
        assert captured['holding_review_context']['request']['analysis_depth'] == 'deep'
        assert captured['holding_review_context']['role_instruction'] == '交易专家'
        context = body['data']['holding_review_context']
        assert context['holding_stock_id'] == holding_stock['id']
        assert context['watch_stock_id'] == watch_stock['id']
        assert context['role'] == '交易专家'
        assert context['review_type'] == 'weekly'
        assert context['period_key'] == '2026-W18'

    def test_holding_review_record_api_persists_and_lists_history(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'holding-review-record.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        watch_stock = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        ).get_json()['data']
        holding_stock = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 200,
                'price': 180,
                'current_price': 182.4,
                'note': '从关注转持仓',
            },
        ).get_json()['data']

        raw_result = {
            'success': True,
            'data': {
                'trade_date': '2026-04-29',
                'review_type': 'weekly',
                'period_key': '2026-W18',
                'analysis_depth': 'deep',
                'performance_summary': '收益结果尚可，但未明显跑赢预期。',
                'execution_summary': '执行节奏基本合规，卖点仍需更明确。',
                'risk_summary': '波动放大后，仓位暴露开始偏高。',
                'discipline_summary': '整体遵守计划，但止盈纪律仍需强化。',
                'next_action_summary': '下周优先观察放量冲高后的承接，必要时准备减仓。',
                'conclusion_tag': 'prepare_reduce',
                'tabs': [
                    {'id': 'execution_review', 'title': '执行与卖出复盘', 'summary': '执行节奏基本合规。', 'evidence': ['买入后未追高加仓', '卖出条件尚未完全触发']},
                    {'id': 'result_review', 'title': '结果复盘', 'summary': '收益结果尚可。', 'evidence': ['当前仍保持浮盈', '但未明显跑赢行业指数']},
                    {'id': 'discipline_review', 'title': '方法与纪律', 'summary': '主要问题在止盈纪律。', 'evidence': ['高位阶段未充分兑现', '复核节奏略慢']},
                    {'id': 'next_action', 'title': '后续动作', 'summary': '若冲高乏力则准备减仓。', 'evidence': ['风险暴露正在抬升', '需要更主动管理仓位']},
                ],
                'evidence': [{'tab': '后续动作', 'detail': '需要更主动管理仓位'}],
                'meta': {'role': '交易专家'},
            },
        }

        save_response = self.client.post(
            '/api/trading-decision/holding-review-records',
            json={
                'holding_stock_id': holding_stock['id'],
                'trade_date': '2026-04-29',
                'review_type': 'weekly',
                'period_key': '2026-W18',
                'analysis_depth': 'deep',
                'raw_result': raw_result,
            },
        )

        assert save_response.status_code == 200
        saved = save_response.get_json()['data']
        assert saved['holding_stock_id'] == holding_stock['id']
        assert saved['watch_stock_id'] == watch_stock['id']
        assert saved['review_type'] == 'weekly'
        assert saved['review_type_label'] == '周复盘'
        assert saved['conclusion_tag'] == 'prepare_reduce'
        assert saved['conclusion_tag_label'] == '准备减仓'
        assert saved['tabs_json'][3]['title'] == '后续动作'
        assert saved['next_action_summary'] == '下周优先观察放量冲高后的承接，必要时准备减仓。'

        list_response = self.client.get(f"/api/trading-decision/holding-review-records?holding_stock_id={holding_stock['id']}")
        assert list_response.status_code == 200
        records = list_response.get_json()['data']
        assert len(records) == 1
        assert records[0]['id'] == saved['id']

        detail_response = self.client.get(f"/api/trading-decision/holding-review-records/{saved['id']}")
        assert detail_response.status_code == 200
        detail = detail_response.get_json()['data']
        assert detail['raw_result_json']['data']['conclusion_tag'] == 'prepare_reduce'
        assert detail['tabs_json'][0]['summary'] == '执行节奏基本合规。'

        page_response = self.client.get(f"/holding-review?holding_stock_id={holding_stock['id']}&record_id={saved['id']}")
        assert page_response.status_code == 200
        page_content = page_response.get_data(as_text=True)
        assert 'holdingReviewPrefill' in page_content
        assert 'renderHoldingReviewResult' in page_content
        assert 'normalizeHoldingReviewResult' in page_content
        assert '准备减仓' in page_content
        assert '执行与卖出复盘' in page_content
        assert '后续动作' in page_content

        holding_detail = self.client.get(f"/api/trading-decision/holding-stocks/{holding_stock['id']}").get_json()['data']
        assert holding_detail['last_review_at'] == '2026-04-29'
        assert holding_detail['suggested_action'] == '下周优先观察放量冲高后的承接，必要时准备减仓。'

        repo_record = service.holding_review_record_repository.get_by_id(saved['id'])
        assert repo_record['review_type'] == 'weekly'
        assert repo_record['conclusion_tag'] == 'prepare_reduce'
        assert repo_record['tabs_json'][0]['title'] == '执行与卖出复盘'
        assert repo_record['raw_result_json']['data']['meta']['role'] == '交易专家'

    def test_position_decision_record_api_persists_and_lists_history(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'position-decision-record.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        watch_stock = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        ).get_json()['data']
        holding_stock = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 200,
                'price': 180,
                'current_price': 182.4,
                'note': '从关注转持仓',
            },
        ).get_json()['data']

        raw_result = {
            'success': True,
            'data': {
                'trade_date': '2026-04-29',
                'analysis_depth': 'deep',
                'decision': {
                    'action': 'reduce',
                    'status': 'reduce_candidate',
                    'confidence': 'high',
                    'summary': '短期涨幅较大，适合先兑现部分仓位。',
                },
                'tabs': [
                    {'id': 'trigger', 'title': '触发条件', 'summary': '股价偏离成本较多。', 'evidence': ['浮盈明显扩大']},
                    {'id': 'reason', 'title': '核心理由', 'summary': '赔率下降。', 'evidence': ['估值压力抬升']},
                    {'id': 'execution', 'title': '执行注意事项', 'summary': '分批减仓。', 'evidence': ['避免一次性砸盘']},
                    {'id': 'risk', 'title': '风险分析', 'summary': '减仓后可能踏空。', 'evidence': ['趋势仍未完全转弱']},
                    {'id': 'conclusion', 'title': '结论', 'summary': '建议先减仓 20%-30%。', 'evidence': ['前四项证据偏向收缩仓位']},
                ],
                'evidence': [{'tab': '核心理由', 'detail': '估值压力抬升'}],
                'meta': {'role': '股票分析师'},
            },
        }

        save_response = self.client.post(
            '/api/trading-decision/position-decision-records',
            json={
                'holding_stock_id': holding_stock['id'],
                'trade_date': '2026-04-29',
                'analysis_depth': 'deep',
                'raw_result': raw_result,
            },
        )

        assert save_response.status_code == 200
        saved = save_response.get_json()['data']
        assert saved['holding_stock_id'] == holding_stock['id']
        assert saved['watch_stock_id'] == watch_stock['id']
        assert saved['decision_type'] == 'reduce'
        assert saved['decision_type_label'] == '适合减仓'
        assert saved['decision_status'] == 'reduce_candidate'
        assert saved['conclusion_summary'] == '短期涨幅较大，适合先兑现部分仓位。'
        assert saved['tabs_json'][4]['title'] == '结论'

        list_response = self.client.get(f"/api/trading-decision/position-decision-records?holding_stock_id={holding_stock['id']}")
        assert list_response.status_code == 200
        records = list_response.get_json()['data']
        assert len(records) == 1
        assert records[0]['id'] == saved['id']

        detail_response = self.client.get(f"/api/trading-decision/position-decision-records/{saved['id']}")
        assert detail_response.status_code == 200
        detail = detail_response.get_json()['data']
        assert detail['raw_result_json']['data']['decision']['action'] == 'reduce'
        assert detail['tabs_json'][0]['summary'] == '股价偏离成本较多。'

        holding_detail = self.client.get(f"/api/trading-decision/holding-stocks/{holding_stock['id']}").get_json()['data']
        assert holding_detail['suggested_action'] == '适合减仓'
        assert holding_detail['last_review_at'] == '2026-04-29'

    def test_position_decision_record_api_serializes_timestamp_in_raw_result(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'position-decision-timestamp.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        watch_stock = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        ).get_json()['data']
        holding_stock = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 100,
                'price': 1680,
                'current_price': 1688,
                'note': '首笔建仓',
            },
        ).get_json()['data']

        saved = service.save_position_decision_record(
            {
                'holding_stock_id': holding_stock['id'],
                'trade_date': '2026-04-29',
                'analysis_depth': 'standard',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-29',
                        'decision': {
                            'action': 'watch',
                            'status': 'observe',
                            'confidence': 'medium',
                            'summary': '继续观察。',
                        },
                        'tabs': [
                            {'id': 'trigger', 'title': '触发条件', 'summary': '暂无新触发。', 'evidence': ['等待财报验证']},
                            {'id': 'reason', 'title': '核心理由', 'summary': '赔率一般。', 'evidence': ['缺少新增催化']},
                            {'id': 'execution', 'title': '执行注意事项', 'summary': '维持跟踪。', 'evidence': ['不追高']},
                            {'id': 'risk', 'title': '风险分析', 'summary': '行业仍有波动。', 'evidence': ['情绪尚未稳定']},
                            {'id': 'conclusion', 'title': '结论', 'summary': '继续观察。', 'evidence': ['暂无明确动作信号']},
                        ],
                        'evidence': [],
                        'meta': {'generated_at': pd.Timestamp('2026-04-29 16:11:23')},
                        'context_snapshot': {'as_of': pd.Timestamp('2026-04-29 00:00:00')},
                    },
                },
            }
        )

        assert saved['raw_result_json']['data']['meta']['generated_at'] == '2026-04-29 16:11:23'
        assert saved['raw_result_json']['data']['context_snapshot']['as_of'] == '2026-04-29 00:00:00'
        assert saved['decision_type'] == 'watch'
        assert saved['decision_type_label'] == '继续观察'

        detail_response = self.client.get(f"/api/trading-decision/position-decision-records/{saved['id']}")
        assert detail_response.status_code == 200
        detail = detail_response.get_json()['data']
        assert detail['raw_result_json']['data']['meta']['generated_at'] == '2026-04-29 16:11:23'
        assert detail['raw_result_json']['data']['context_snapshot']['as_of'] == '2026-04-29 00:00:00'
        assert detail['decision_type'] == 'watch'
        assert detail['decision_type_label'] == '继续观察'

        holding_detail = self.client.get(f"/api/trading-decision/holding-stocks/{holding_stock['id']}").get_json()['data']
        assert holding_detail['suggested_action'] == '继续观察'
        assert holding_detail['last_review_at'] == '2026-04-29'

        list_response = self.client.get(f"/api/trading-decision/position-decision-records?holding_stock_id={holding_stock['id']}")
        assert list_response.status_code == 200
        records = list_response.get_json()['data']
        assert records[0]['id'] == saved['id']
        assert records[0]['raw_result_json']['data']['meta']['generated_at'] == '2026-04-29 16:11:23'

        repo_record = service.position_decision_record_repository.get_by_id(saved['id'])
        assert repo_record['raw_result_json']['data']['meta']['generated_at'] == '2026-04-29 16:11:23'
        assert repo_record['raw_result_json']['data']['context_snapshot']['as_of'] == '2026-04-29 00:00:00'
        assert repo_record['decision_type'] == 'watch'
        assert repo_record['decision_status'] == 'observe'
        assert repo_record['confidence'] == 'medium'
        assert repo_record['conclusion_summary'] == '继续观察。'
        assert repo_record['analysis_depth'] == 'standard'
        assert repo_record['trade_date'] == '2026-04-29'
        assert repo_record['holding_stock_id'] == holding_stock['id']
        assert repo_record['watch_stock_id'] == watch_stock['id']
        assert repo_record['tabs_json'][4]['title'] == '结论'
        assert repo_record['tabs_json'][4]['summary'] == '继续观察。'
        assert repo_record['tabs_json'][0]['evidence'][0] == '等待财报验证'
        assert repo_record['raw_result_json']['success'] is True
        assert repo_record['raw_result_json']['data']['decision']['summary'] == '继续观察。'
        assert repo_record['raw_result_json']['data']['decision']['action'] == 'watch'
        assert repo_record['raw_result_json']['data']['decision']['status'] == 'observe'
        assert repo_record['raw_result_json']['data']['decision']['confidence'] == 'medium'
        assert repo_record['raw_result_json']['data']['trade_date'] == '2026-04-29'
        assert repo_record['raw_result_json']['data']['context_snapshot']['as_of'] == '2026-04-29 00:00:00'
        assert repo_record['raw_result_json']['data']['meta']['generated_at'] == '2026-04-29 16:11:23'
        assert repo_record['created_at']
        assert repo_record['updated_at']
        assert repo_record['id'].startswith('PDR-')
        assert repo_record['stock_code'] == '600519'
        assert repo_record['stock_name'] == '贵州茅台'
        assert repo_record['market'] == 'A股'
        assert repo_record['evidence_json'] == []
        assert repo_record['reason_summary'] == '赔率一般。'
        assert repo_record['trigger_summary'] == '暂无新触发。'
        assert repo_record['execution_summary'] == '维持跟踪。'
        assert repo_record['risk_summary'] == '行业仍有波动。'

    def test_position_decision_page_returns_bad_request_without_holding_stock_id(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'position-decision-required.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/position-decision')

        assert response.status_code == 400
        assert response.get_json()['error']['code'] == 'bad_request'

    def test_position_decision_page_returns_bad_request_without_holding_stock_id(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'position-decision-required.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/position-decision')

        assert response.status_code == 400
        assert response.get_json()['error']['code'] == 'bad_request'

    def test_position_decision_page_can_open_saved_record(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'position-decision-record-page.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        watch_stock = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        ).get_json()['data']
        holding_stock = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 200,
                'price': 180,
                'current_price': 182.4,
                'note': '从关注转持仓',
            },
        ).get_json()['data']

        saved = service.save_position_decision_record(
            {
                'holding_stock_id': holding_stock['id'],
                'trade_date': '2026-04-29',
                'analysis_depth': 'standard',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-29',
                        'analysis_depth': 'standard',
                        'decision': {
                            'action': 'watch',
                            'status': 'observe',
                            'confidence': 'medium',
                            'summary': '暂时继续观察，等待新的触发条件。',
                        },
                        'tabs': [
                            {'id': 'trigger', 'title': '触发条件', 'summary': '暂无明确加减仓触发。', 'evidence': ['价格仍在区间中部']},
                            {'id': 'reason', 'title': '核心理由', 'summary': '赔率一般。', 'evidence': ['缺少新的催化']},
                            {'id': 'execution', 'title': '执行注意事项', 'summary': '维持观察。', 'evidence': ['保留后手']},
                            {'id': 'risk', 'title': '风险分析', 'summary': '波动仍可能扩大。', 'evidence': ['行业情绪未稳']},
                            {'id': 'conclusion', 'title': '结论', 'summary': '继续观察。', 'evidence': ['前四项未形成明确动作共识']},
                        ],
                        'evidence': [],
                        'meta': {'role': '股票分析师'},
                    },
                },
            }
        )

        response = self.client.get(f"/position-decision?holding_stock_id={holding_stock['id']}&record_id={saved['id']}")

        assert response.status_code == 200
        content = response.get_data(as_text=True)
        assert 'positionDecisionPrefill' in content
        assert '买卖决策结果' in content
        assert '查看原始 JSON' in content
        assert 'renderPositionDecisionResult' in content
        assert 'normalizePositionDecisionResult' in content
        assert 'normalizeTabs' in content
        assert 'data-page="position-decision"' in content

        detail = service.get_position_decision_record(saved['id'])
        assert detail is not None
        assert detail['decision_type'] == 'watch'
        assert detail['decision_status'] == 'observe'
        assert detail['confidence'] == 'medium'
        assert detail['conclusion_summary'] == '暂时继续观察，等待新的触发条件。'
        assert detail['tabs_json'][0]['title'] == '触发条件'
        assert detail['tabs_json'][4]['title'] == '结论'

        repo_record = service.position_decision_record_repository.get_by_id(saved['id'])
        assert repo_record is not None
        assert repo_record['decision_type'] == 'watch'
        assert repo_record['decision_status'] == 'observe'
        assert repo_record['analysis_depth'] == 'standard'
        assert repo_record['trade_date'] == '2026-04-29'

    def test_position_decision_page_prefill_supports_raw_top_level_result(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'position-decision-raw-prefill.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        watch_stock = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600900',
                'stock_name': '长江电力',
                'market': 'A股',
                'asset_type': '红利资产',
            },
        ).get_json()['data']
        holding_stock = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 300,
                'price': 28.5,
                'current_price': 28.9,
                'note': '建立底仓',
            },
        ).get_json()['data']

        saved = service.save_position_decision_record(
            {
                'holding_stock_id': holding_stock['id'],
                'trade_date': '2026-04-29',
                'analysis_depth': 'deep',
                'raw_result': {
                    'recommended_action': 'watch',
                    'decision_status': 'observe',
                    'confidence': 'medium',
                    'conclusion_summary': '继续观察，等待更清晰的催化与估值错配。',
                    'tabs': [
                        {'id': 'conclusion', 'title': '结论', 'summary': '继续观察。', 'evidence': ['当前仍以跟踪为主']},
                        {'id': 'risk', 'title': '风险分析', 'summary': '利率波动可能扰动红利资产。', 'evidence': ['高股息风格可能阶段性承压']},
                        {'id': 'execution', 'title': '执行注意事项', 'summary': '不追涨，等待更优位置。', 'evidence': ['优先关注回撤后的承接']},
                        {'id': 'reason', 'title': '核心理由', 'summary': '经营稳定但短期赔率一般。', 'evidence': ['估值尚未出现明显折价']},
                        {'id': 'trigger', 'title': '触发条件', 'summary': '暂无明确加减仓触发。', 'evidence': ['需要新的基本面或价格触发']},
                    ],
                    'meta': {'role': '股票分析师'},
                    'context_snapshot': {'source': 'test'},
                },
            }
        )

        response = self.client.get(f"/position-decision?holding_stock_id={holding_stock['id']}&record_id={saved['id']}")

        assert response.status_code == 200
        content = response.get_data(as_text=True)
        assert 'positionDecisionPrefill' in content
        assert 'recommended_action' in content
        assert 'decision_status' in content
        assert 'confidence' in content
        assert '买卖决策结果' in content
        assert '查看原始 JSON' in content

        detail = service.get_position_decision_record(saved['id'])
        assert detail is not None
        assert detail['decision_type'] == 'watch'
        assert detail['decision_status'] == 'observe'
        assert detail['confidence'] == 'medium'
        assert detail['conclusion_summary'] == '继续观察，等待更清晰的催化与估值错配。'
        assert detail['raw_result_json']['data']['decision']['action'] == 'watch'
        assert detail['raw_result_json']['data']['tabs'][0]['title'] == '结论'
        assert detail['raw_result_json']['data']['tabs'][4]['title'] == '触发条件'
        assert detail['raw_result_json']['data']['analysis_depth'] == 'deep'
        assert detail['raw_result_json']['data']['trade_date'] == '2026-04-29'
        assert detail['raw_result_json']['data']['meta']['role'] == '股票分析师'
        assert detail['raw_result_json']['data']['context_snapshot']['source'] == 'test'

        repo_record = service.position_decision_record_repository.get_by_id(saved['id'])
        assert repo_record is not None
        assert repo_record['decision_type'] == 'watch'
        assert repo_record['decision_status'] == 'observe'
        assert repo_record['analysis_depth'] == 'deep'
        assert repo_record['trade_date'] == '2026-04-29'
        assert repo_record['reason_summary'] == '经营稳定但短期赔率一般。'
        assert repo_record['trigger_summary'] == '暂无明确加减仓触发。'
        assert repo_record['execution_summary'] == '不追涨，等待更优位置。'
        assert repo_record['risk_summary'] == '利率波动可能扰动红利资产。'
        assert repo_record['evidence_json'][0]['detail'] == '当前仍以跟踪为主'
        assert repo_record['evidence_json'][-1]['detail'] == '需要新的基本面或价格触发'

        detail_response = self.client.get(f"/api/trading-decision/position-decision-records/{saved['id']}")
        assert detail_response.status_code == 200
        detail_payload = detail_response.get_json()['data']
        assert detail_payload['decision_type'] == 'watch'
        assert detail_payload['decision_type_label'] == '继续观察'

        holding_detail = self.client.get(f"/api/trading-decision/holding-stocks/{holding_stock['id']}").get_json()['data']
        assert holding_detail['suggested_action'] == '继续观察'
        assert holding_detail['last_review_at'] == '2026-04-29'

        list_response = self.client.get(f"/api/trading-decision/position-decision-records?holding_stock_id={holding_stock['id']}")
        assert list_response.status_code == 200
        records = list_response.get_json()['data']
        assert records[0]['id'] == saved['id']
        assert records[0]['decision_type'] == 'watch'

        assert saved['raw_result_json']['data']['decision']['action'] == 'watch'
        assert saved['raw_result_json']['data']['tabs'][0]['title'] == '结论'
        assert saved['raw_result_json']['data']['tabs'][4]['title'] == '触发条件'
        assert saved['raw_result_json']['data']['analysis_depth'] == 'deep'
        assert saved['raw_result_json']['data']['trade_date'] == '2026-04-29'
        assert saved['raw_result_json']['data']['meta']['role'] == '股票分析师'
        assert saved['raw_result_json']['data']['context_snapshot']['source'] == 'test'
        assert saved['evidence_json'][0]['detail'] == '当前仍以跟踪为主'
        assert saved['evidence_json'][-1]['detail'] == '需要新的基本面或价格触发'

        assert response.get_data(as_text=True).count('positionDecisionPrefill') >= 1
        assert response.get_data(as_text=True).count('recommended_action') >= 1
        assert response.get_data(as_text=True).count('decision_status') >= 1
        assert response.get_data(as_text=True).count('confidence') >= 1
        assert response.get_data(as_text=True).count('股票分析师') >= 1

    def test_trade_plan_analysis_record_api_persists_and_lists_history(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'trade-plan-analysis-records.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        watch_stock = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600900',
                'stock_name': '长江电力',
                'market': 'A股',
                'asset_type': '红利资产',
            },
        ).get_json()['data']

        raw_result = {
            'success': True,
            'data': {
                'trade_plan_markdown': '# 持仓计划\n\n测试内容',
                'decision': {
                    'action': 'buy',
                    'summary': '回踩后具备分批建仓条件。',
                    'logic': '估值与趋势配合。',
                    'risk_level': 'medium',
                    'risks': ['回撤风险'],
                    'time_horizon': '5-15 trading days',
                    'position_suggestion': {
                        'target_position': '20%',
                        'position_limit': '20%',
                        'add_condition': '回踩支撑不破',
                        'reduce_condition': '跌破支撑减仓',
                        'stop_loss_reference': '跌破关键位止损',
                    },
                },
                'meta': {
                    'role': '股票分析师',
                    'data_source': 'cache_first',
                },
            },
        }

        save_response = self.client.post(
            '/api/trading-decision/trade-plan-analysis-records',
            json={
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-29',
                'plan_type': '三笔计划',
                'risk_preference': 'balanced',
                'raw_result': raw_result,
            },
        )

        assert save_response.status_code == 200
        saved = save_response.get_json()['data']
        assert saved['watch_stock_id'] == watch_stock['id']
        assert saved['decision_action'] == 'buy'
        assert saved['conclusion_summary'] == '回踩后具备分批建仓条件。'

        detail_response = self.client.get(f"/api/trading-decision/trade-plan-analysis-records/{saved['id']}")
        assert detail_response.status_code == 200
        detail = detail_response.get_json()['data']
        assert detail['decision_action'] == 'buy'

        list_response = self.client.get(f"/api/trading-decision/trade-plan-analysis-records?watch_stock_id={watch_stock['id']}")
        assert list_response.status_code == 200
        records = list_response.get_json()['data']
        assert records[0]['id'] == saved['id']
        assert records[0]['decision_action'] == 'buy'

        updated_watch_stock = self.client.get(f"/api/trading-decision/watch-stocks/{watch_stock['id']}").get_json()['data']
        assert updated_watch_stock['last_conclusion_summary'] == '回踩后具备分批建仓条件。'
        assert updated_watch_stock['last_trade_plan_at'] == '2026-04-29'
        assert updated_watch_stock['trade_plan_status'] == '已有计划'
        assert updated_watch_stock['trade_plan_action'] == '适合买入'
        assert updated_watch_stock['trade_plan_record_id'] == saved['id']
        assert updated_watch_stock['last_risk_level'] == 'medium'
        assert updated_watch_stock['last_plan_type'] == '三笔计划'
        assert updated_watch_stock['last_risk_preference'] == 'balanced'
        assert updated_watch_stock['last_trade_plan_markdown'] == '# 持仓计划\n\n测试内容'

        repo_record = service.trade_plan_analysis_record_repository.get_by_id(saved['id'])
        assert repo_record['watch_stock_id'] == watch_stock['id']
        assert repo_record['decision_action'] == 'buy'
        assert repo_record['conclusion_summary'] == '回踩后具备分批建仓条件。'
        assert repo_record['trade_plan_markdown'] == '# 持仓计划\n\n测试内容'
        assert repo_record['risk_level'] == 'medium'
        assert repo_record['risks_json'] == ['回撤风险']
        assert repo_record['position_suggestion_json']['target_position'] == '20%'
        assert repo_record['raw_result_json']['data']['decision']['summary'] == '回踩后具备分批建仓条件。'
        assert repo_record['raw_result_json']['data']['decision']['action'] == 'buy'
        assert repo_record['raw_result_json']['data']['trade_plan_markdown'] == '# 持仓计划\n\n测试内容'
        assert repo_record['raw_result_json']['data']['meta']['data_source'] == 'cache_first'
        assert repo_record['trade_date'] == '2026-04-29'
        assert repo_record['plan_type'] == '三笔计划'
        assert repo_record['risk_preference'] == 'balanced'
        assert repo_record['created_at']
        assert repo_record['updated_at']
        assert repo_record['id'].startswith('TPA-')
        assert repo_record['stock_code'] == '600900'
        assert repo_record['stock_name'] == '长江电力'
        assert repo_record['market'] == 'A股'
        assert repo_record['decision_logic'] == '估值与趋势配合。'
        assert repo_record['time_horizon'] == '5-15 trading days'
        assert repo_record['position_suggestion_json']['position_limit'] == '20%'
        assert repo_record['position_suggestion_json']['add_condition'] == '回踩支撑不破'
        assert repo_record['position_suggestion_json']['reduce_condition'] == '跌破支撑减仓'
        assert repo_record['position_suggestion_json']['stop_loss_reference'] == '跌破关键位止损'
        assert repo_record['raw_result_json']['success'] is True
        assert repo_record['raw_result_json']['data']['meta']['role'] == '股票分析师'
        assert saved['decision_action_label'] == '适合买入'
        assert detail['decision_action_label'] == '适合买入'
        assert records[0]['decision_action_label'] == '适合买入'

        page_response = self.client.get(f"/trade-plan-analysis?watch_stock_id={watch_stock['id']}&record_id={saved['id']}")
        assert page_response.status_code == 200
        content = page_response.get_data(as_text=True)
        assert '持仓计划分析' in content
        assert '# 持仓计划' in content
        assert '回踩后具备分批建仓条件。' in content
        assert 'positionSuggestion' in content
        assert 'tradePlanPrefill' in content
        assert '股票分析师' in content
        assert '回撤风险' in content
        assert '适合买入' in content
        assert 'trade-plan-analysis-records' in content
        assert '三笔计划' in content
        assert 'balanced' in content
        assert 'cache_first' in content
        assert '5-15 trading days' in content
        assert '20%' in content
        assert '回踩支撑不破' in content
        assert '跌破支撑减仓' in content
        assert '跌破关键位止损' in content
        assert 'trade-plan-analysis' in content
        assert 'renderTradePlanResult' in content
        assert '保存持仓计划分析记录' in content
        assert '生成持仓计划分析' in content
        assert '风险偏好' in content
        assert '计划类型' in content
        assert 'tradeDate' in content
        assert 'planType' in content
        assert 'riskPreference' in content
        assert '系统就绪' in content
        assert 'SSE 未连接' in content
        assert 'tradePlanMarkdown' in content
        assert 'positionSuggestionJson' in content
        assert '回踩后具备分批建仓条件。' in content
        assert 'tradePlanPrefill' in content
        assert 'trade_plan_analysis' in content
        assert 'markdown-body' in content
        assert 'decision-action' in content
        assert '风险等级' in content
        assert '时间维度' in content
        assert '止损参考' in content
        assert '目标仓位' in content
        assert '加仓条件' in content
        assert '减仓条件' in content
        assert 'position_suggestion' in content
        assert 'trade_plan_markdown' in content
        assert 'decision_action' in content
        assert 'conclusion_summary' in content
        assert 'data-page="trade-plan-analysis"' in content
        assert 'tradePlanHistory' in content
        assert 'tradePlanRawJson' in content
        assert '回踩后具备分批建仓条件。' in content
        assert '适合买入' in content
        assert '股票分析师' in content
        assert 'cache_first' in content
        assert '回撤风险' in content
        assert '5-15 trading days' in content
        assert '20%' in content
        assert '回踩支撑不破' in content
        assert '跌破支撑减仓' in content
        assert '跌破关键位止损' in content
        assert 'trade-plan-analysis-records' in content
        assert 'trade-plan-analysis?watch_stock_id=' in content
        assert saved['id'] in content
        assert detail['id'] == saved['id']
        assert records[0]['id'] == saved['id']
        assert repo_record['id'] == saved['id']
        assert updated_watch_stock['trade_plan_record_id'] == saved['id']
        assert detail['decision_action'] == saved['decision_action'] == repo_record['decision_action']
        assert detail['conclusion_summary'] == saved['conclusion_summary'] == repo_record['conclusion_summary']
        assert detail['risk_level'] == saved['risk_level'] == repo_record['risk_level']
        assert detail['trade_plan_markdown'] == saved['trade_plan_markdown'] == repo_record['trade_plan_markdown']
        assert detail['position_suggestion_json']['target_position'] == saved['position_suggestion_json']['target_position'] == repo_record['position_suggestion_json']['target_position']
        assert detail['raw_result_json']['data']['decision']['summary'] == saved['raw_result_json']['data']['decision']['summary'] == repo_record['raw_result_json']['data']['decision']['summary']
        assert detail['raw_result_json']['data']['meta']['data_source'] == saved['raw_result_json']['data']['meta']['data_source'] == repo_record['raw_result_json']['data']['meta']['data_source']
        assert records[0]['conclusion_summary'] == saved['conclusion_summary']
        assert records[0]['risk_level'] == saved['risk_level']
        assert records[0]['trade_plan_markdown'] == saved['trade_plan_markdown']
        assert records[0]['position_suggestion_json']['target_position'] == saved['position_suggestion_json']['target_position']
        assert detail['trade_date'] == saved['trade_date'] == repo_record['trade_date']
        assert detail['plan_type'] == saved['plan_type'] == repo_record['plan_type']
        assert detail['risk_preference'] == saved['risk_preference'] == repo_record['risk_preference']
        assert detail['watch_stock_id'] == saved['watch_stock_id'] == repo_record['watch_stock_id']
        assert detail['stock_code'] == saved['stock_code'] == repo_record['stock_code']
        assert detail['stock_name'] == saved['stock_name'] == repo_record['stock_name']
        assert detail['market'] == saved['market'] == repo_record['market']
        assert detail['decision_logic'] == saved['decision_logic'] == repo_record['decision_logic']
        assert detail['time_horizon'] == saved['time_horizon'] == repo_record['time_horizon']
        assert detail['risks_json'] == saved['risks_json'] == repo_record['risks_json']
        assert detail['position_suggestion_json'] == saved['position_suggestion_json'] == repo_record['position_suggestion_json']
        assert detail['raw_result_json']['success'] is True
        assert saved['raw_result_json']['success'] is True
        assert repo_record['raw_result_json']['success'] is True
        assert detail['raw_result_json']['data']['decision']['action'] == 'buy'
        assert saved['raw_result_json']['data']['decision']['action'] == 'buy'
        assert records[0]['raw_result_json']['data']['decision']['action'] == 'buy'
        assert detail['raw_result_json']['data']['meta']['role'] == '股票分析师'
        assert saved['raw_result_json']['data']['meta']['role'] == '股票分析师'
        assert records[0]['raw_result_json']['data']['meta']['role'] == '股票分析师'
        assert detail['raw_result_json']['data']['trade_plan_markdown'] == '# 持仓计划\n\n测试内容'
        assert saved['raw_result_json']['data']['trade_plan_markdown'] == '# 持仓计划\n\n测试内容'
        assert records[0]['raw_result_json']['data']['trade_plan_markdown'] == '# 持仓计划\n\n测试内容'
        assert detail['raw_result_json']['data']['decision']['position_suggestion']['target_position'] == '20%'
        assert saved['raw_result_json']['data']['decision']['position_suggestion']['target_position'] == '20%'
        assert records[0]['raw_result_json']['data']['decision']['position_suggestion']['target_position'] == '20%'
        assert detail['raw_result_json']['data']['decision']['position_suggestion']['position_limit'] == '20%'
        assert saved['raw_result_json']['data']['decision']['position_suggestion']['position_limit'] == '20%'
        assert records[0]['raw_result_json']['data']['decision']['position_suggestion']['position_limit'] == '20%'
        assert detail['raw_result_json']['data']['decision']['position_suggestion']['add_condition'] == '回踩支撑不破'
        assert saved['raw_result_json']['data']['decision']['position_suggestion']['add_condition'] == '回踩支撑不破'
        assert records[0]['raw_result_json']['data']['decision']['position_suggestion']['add_condition'] == '回踩支撑不破'
        assert detail['raw_result_json']['data']['decision']['position_suggestion']['reduce_condition'] == '跌破支撑减仓'
        assert saved['raw_result_json']['data']['decision']['position_suggestion']['reduce_condition'] == '跌破支撑减仓'
        assert records[0]['raw_result_json']['data']['decision']['position_suggestion']['reduce_condition'] == '跌破支撑减仓'
        assert detail['raw_result_json']['data']['decision']['position_suggestion']['stop_loss_reference'] == '跌破关键位止损'
        assert saved['raw_result_json']['data']['decision']['position_suggestion']['stop_loss_reference'] == '跌破关键位止损'
        assert records[0]['raw_result_json']['data']['decision']['position_suggestion']['stop_loss_reference'] == '跌破关键位止损'
        assert updated_watch_stock['last_conclusion_summary'] == saved['conclusion_summary']
        assert updated_watch_stock['trade_plan_action'] == saved['decision_action_label']
        assert updated_watch_stock['trade_plan_status'] == '已有计划'
        assert updated_watch_stock['last_risk_level'] == saved['risk_level']
        assert updated_watch_stock['last_plan_type'] == saved['plan_type']
        assert updated_watch_stock['last_risk_preference'] == saved['risk_preference']
        assert updated_watch_stock['last_trade_plan_markdown'] == saved['trade_plan_markdown']

        service_records = service.list_trade_plan_analysis_records(watch_stock['id'])
        assert service_records[0]['id'] == saved['id']
        assert service_records[0]['decision_action'] == 'buy'
        assert service_records[0]['conclusion_summary'] == '回踩后具备分批建仓条件。'
        assert service_records[0]['decision_action_label'] == '适合买入'
        assert service_records[0]['raw_result_json']['data']['decision']['action'] == 'buy'
        assert service_records[0]['position_suggestion_json']['target_position'] == '20%'

        service_detail = service.get_trade_plan_analysis_record(saved['id'])
        assert service_detail['id'] == saved['id']
        assert service_detail['decision_action'] == 'buy'
        assert service_detail['conclusion_summary'] == '回踩后具备分批建仓条件。'
        assert service_detail['position_suggestion_json']['target_position'] == '20%'
        assert service_detail['raw_result_json']['data']['decision']['action'] == 'buy'
        assert service_detail['raw_result_json']['data']['trade_plan_markdown'] == '# 持仓计划\n\n测试内容'

        assert page_response.get_data(as_text=True).count('tradePlanPrefill') == 1
        assert page_response.get_data(as_text=True).count('回踩后具备分批建仓条件。') >= 1
        assert page_response.get_data(as_text=True).count('股票分析师') >= 1
        assert page_response.get_data(as_text=True).count('适合买入') >= 1
        assert page_response.get_data(as_text=True).count('20%') >= 1
        assert page_response.get_data(as_text=True).count('回踩支撑不破') >= 1
        assert page_response.get_data(as_text=True).count('跌破支撑减仓') >= 1
        assert page_response.get_data(as_text=True).count('跌破关键位止损') >= 1
        assert page_response.get_data(as_text=True).count('trade-plan-analysis-records') >= 1
        assert page_response.get_data(as_text=True).count('trade-plan-analysis?watch_stock_id=') >= 1
        assert page_response.get_data(as_text=True).count(saved['id']) >= 1
        assert detail_response.get_json()['success'] is True
        assert list_response.get_json()['success'] is True
        assert save_response.get_json()['success'] is True

        assert saved['decision_action'] == detail['decision_action'] == repo_record['decision_action'] == service_detail['decision_action']
        assert saved['conclusion_summary'] == detail['conclusion_summary'] == repo_record['conclusion_summary'] == service_detail['conclusion_summary']
        assert saved['risk_level'] == detail['risk_level'] == repo_record['risk_level'] == service_detail['risk_level']
        assert saved['trade_plan_markdown'] == detail['trade_plan_markdown'] == repo_record['trade_plan_markdown'] == service_detail['trade_plan_markdown']
        assert saved['position_suggestion_json']['target_position'] == detail['position_suggestion_json']['target_position'] == repo_record['position_suggestion_json']['target_position'] == service_detail['position_suggestion_json']['target_position']
        assert saved['raw_result_json']['data']['decision']['action'] == detail['raw_result_json']['data']['decision']['action'] == repo_record['raw_result_json']['data']['decision']['action'] == service_detail['raw_result_json']['data']['decision']['action']
        assert saved['raw_result_json']['data']['trade_plan_markdown'] == detail['raw_result_json']['data']['trade_plan_markdown'] == repo_record['raw_result_json']['data']['trade_plan_markdown'] == service_detail['raw_result_json']['data']['trade_plan_markdown']
        assert saved['raw_result_json']['data']['meta']['data_source'] == detail['raw_result_json']['data']['meta']['data_source'] == repo_record['raw_result_json']['data']['meta']['data_source'] == service_detail['raw_result_json']['data']['meta']['data_source']
        assert updated_watch_stock['last_conclusion_summary'] == service_detail['conclusion_summary']
        assert updated_watch_stock['trade_plan_action'] == service_detail['decision_action_label']
        assert updated_watch_stock['trade_plan_record_id'] == service_detail['id']
        assert service_records[0]['id'] == service_detail['id']
        assert service_records[0]['decision_action'] == service_detail['decision_action']
        assert service_records[0]['conclusion_summary'] == service_detail['conclusion_summary']
        assert service_records[0]['position_suggestion_json']['target_position'] == service_detail['position_suggestion_json']['target_position']

        assert True


    def test_start_stock_ai_analysis_task_uses_cached_result_async_sse(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'stock-analysis-cache-hit.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        captured = {}

        class StubExecutor:
            def submit(self, fn, *args):
                captured['fn'] = fn
                captured['args'] = args
                future = Future()
                future.set_result(None)
                return future

        cached_result = {
            'success': True,
            'data': {
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'trade_date': '2026-04-26',
                'result_source': 'cache',
                'cache_file': 'A股_600519_贵州茅台_analyse_20260426_.md',
                'meta': {'result_source': 'cache', 'cache_file': 'A股_600519_贵州茅台_analyse_20260426_.md'},
            },
        }
        monkeypatch.setattr(service, 'build_cached_stock_analysis_result', lambda **kwargs: cached_result)

        with self.app.app_context():
            context = analysis_routes_module._context()
            original_executor = context.executor
            context.executor = StubExecutor()
            try:
                result = analysis_routes_module._start_stock_ai_analysis_task(
                    context,
                    {
                        'stock_code': '600519',
                        'stock_name': '贵州茅台',
                        'market': 'A股',
                        'client_id': 'stock_client_cache',
                        'trade_date': '2026-04-26',
                        'analysis_depth': 'standard',
                        'start_date': '2026-01-01',
                        'end_date': '2026-04-26',
                    },
                )
            finally:
                context.executor = original_executor
                with context.task_lock:
                    context.analysis_tasks.pop('ai_600519', None)

        body = result.get_json()
        assert body['success'] is True
        assert body['task_mode'] == 'async'
        assert body['client_id'] == 'stock_client_cache'
        assert captured['fn'].__name__ == '_send_cached_stock_analysis_result'
        assert captured['args'][2]['data']['result_source'] == 'cache'

    def test_start_stock_ai_analysis_task_uses_cached_result_async_sse(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'stock-analysis-cache-hit.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        captured = {}

        class StubExecutor:
            def submit(self, fn, *args):
                captured['fn'] = fn
                captured['args'] = args
                future = Future()
                future.set_result(None)
                return future

        cached_result = {
            'success': True,
            'data': {
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'trade_date': '2026-04-26',
                'result_source': 'cache',
                'cache_file': 'A股_600519_贵州茅台_analyse_20260426_.md',
                'meta': {'result_source': 'cache', 'cache_file': 'A股_600519_贵州茅台_analyse_20260426_.md'},
            },
        }
        monkeypatch.setattr(service, 'build_cached_stock_analysis_result', lambda **kwargs: cached_result)

        with self.app.app_context():
            context = analysis_routes_module._context()
            original_executor = context.executor
            context.executor = StubExecutor()
            try:
                result = analysis_routes_module._start_stock_ai_analysis_task(
                    context,
                    {
                        'stock_code': '600519',
                        'stock_name': '贵州茅台',
                        'market': 'A股',
                        'client_id': 'stock_client_cache',
                        'trade_date': '2026-04-26',
                        'analysis_depth': 'standard',
                        'start_date': '2026-01-01',
                        'end_date': '2026-04-26',
                    },
                )
            finally:
                context.executor = original_executor
                with context.task_lock:
                    context.analysis_tasks.pop('ai_600519', None)

        body = result.get_json()
        assert body['success'] is True
        assert body['task_mode'] == 'async'
        assert body['client_id'] == 'stock_client_cache'
        assert captured['fn'].__name__ == '_send_cached_stock_analysis_result'
        assert captured['args'][2]['data']['result_source'] == 'cache'

    def test_trade_plan_run_api_starts_dedicated_async_task(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'trade-plan-run.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        captured = {}

        def stub_start_trade_plan_analysis_task(watch_stock_id, client_id, trade_plan_context):
            captured['watch_stock_id'] = watch_stock_id
            captured['client_id'] = client_id
            captured['trade_plan_context'] = trade_plan_context
            return True, None, 200

        monkeypatch.setattr(trading_decision_routes_module, '_start_trade_plan_analysis_task', stub_start_trade_plan_analysis_task)

        response = self.client.post(
            f"/api/trading-decision/watch-stocks/{watch_stock['id']}/trade-plan-analysis/run",
            json={
                'trade_date': '2026-04-27',
                'plan_type': '三笔计划',
                'risk_preference': '中高风险',
                'analysis_depth': 'deep',
                'client_id': 'trade_plan_client_1',
            },
        )

        assert response.status_code == 200
        body = response.get_json()
        assert body['success'] is True
        assert captured['watch_stock_id'] == watch_stock['id']
        assert captured['client_id'] == 'trade_plan_client_1'
        assert captured['trade_plan_context']['watch_stock']['stock_code'] == '600519'
        assert captured['trade_plan_context']['request']['trade_date'] == '2026-04-27'
        assert captured['trade_plan_context']['request']['analysis_depth'] == 'deep'
        context = body['data']['trade_plan_analysis_context']
        assert context['watch_stock_id'] == watch_stock['id']
        assert context['plan_type'] == '三笔计划'
        assert context['risk_preference'] == '中高风险'
        assert context['template_name'] == '持仓计划模板（买前执行版）'

    def test_trade_plan_analysis_page_returns_not_found_for_unknown_watch_stock(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'trade-plan-404.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/trade-plan-analysis?watch_stock_id=WS-UNKNOWN')

        assert response.status_code == 404
        assert response.get_json()['error']['code'] == 'not_found'

    def test_stock_analysis_record_api_persists_and_updates_watch_stock(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'stock-analysis-record-save.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        raw_result = {
            'success': True,
            'data': {
                'trade_date': '2026-04-27',
                'analysis_mode': 'agentic',
                'stance': 'bullish',
                'time_horizon': '3-10 trading days',
                'logic': '趋势修复，等待右侧确认。',
                'decision': {
                    'summary': '趋势修复，适合继续跟踪。',
                    'risk_level': 'medium',
                },
                'scores': {
                    'technical': 78,
                    'sentiment': 71,
                    'composite': 75,
                },
                'signals': [{'signal': '放量回升', 'detail': '量价共振'}],
                'risks': [{'risk': '板块波动仍大', 'detail': '需要控制追高'}],
                'evidence': [{'type': 'price_action', 'detail': '均线拐头向上'}],
                'meta': {},
                'snapshot': {},
            },
        }

        save_response = self.client.post(
            '/api/trading-decision/stock-analysis-records',
            json={
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'raw_result': raw_result,
            },
        )

        assert save_response.status_code == 200
        saved = save_response.get_json()['data']
        assert saved['watch_stock_id'] == watch_stock['id']
        assert saved['analysis_mode'] == 'agentic'
        assert saved['stance'] == 'bullish'
        assert saved['conclusion_summary'] == '趋势修复，适合继续跟踪。'
        assert saved['scores_json']['composite'] == 75
        assert saved['signals_json'][0]['detail'] == '量价共振'

        list_response = self.client.get(f"/api/trading-decision/stock-analysis-records?watch_stock_id={watch_stock['id']}")
        assert list_response.status_code == 200
        records = list_response.get_json()['data']
        assert len(records) == 1
        assert records[0]['id'] == saved['id']

        detail_response = self.client.get(f"/api/trading-decision/stock-analysis-records/{saved['id']}")
        assert detail_response.status_code == 200
        detail = detail_response.get_json()['data']
        assert detail['raw_result_json']['data']['stance'] == 'bullish'
        assert detail['risks_json'][0]['risk'] == '板块波动仍大'

        watch_stock_response = self.client.get(f"/api/trading-decision/watch-stocks/{watch_stock['id']}")
        updated_watch_stock = watch_stock_response.get_json()['data']
        assert updated_watch_stock['suggested_action'] == ''
        assert updated_watch_stock['last_conclusion_summary'] == '趋势修复，适合继续跟踪。'
        assert updated_watch_stock['last_analysis_at'] == '2026-04-27'

    def test_holding_reanalysis_page_requires_holding_stock_id(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'holding-reanalysis-required.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/holding-reanalysis')

        assert response.status_code == 400
        assert response.get_json()['error']['code'] == 'bad_request'

    def test_holding_reanalysis_page_renders_saved_record_and_history(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'holding-reanalysis-page.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        watch_stock = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
                'industry': '白酒',
            },
        ).get_json()['data']
        holding_stock = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 100,
                'price': 1680,
                'current_price': 1688,
                'note': '首笔建仓',
            },
        ).get_json()['data']

        saved = service.save_stock_analysis_record(
            {
                'holding_stock_id': holding_stock['id'],
                'analysis_scene': 'holding_reanalysis',
                'trade_date': '2026-04-29',
                'raw_result': {
                    'success': True,
                    'data': {
                        'stock_code': '600519',
                        'stock_name': '贵州茅台',
                        'market': 'A股',
                        'trade_date': '2026-04-29',
                        'analysis_mode': 'agentic',
                        'decision': {'summary': '继续持有，等待下一次验证。', 'risk_level': 'medium'},
                        'scores': {'technical': 70, 'fundamental': 82, 'sentiment': 61, 'composite': 74},
                        'signals': [{'signal': '成交稳定', 'detail': '缩量整理'}],
                        'risks': [{'risk': '估值不便宜', 'detail': '需要等待业绩验证'}],
                        'evidence': [],
                        'logic': '原始买入逻辑未被破坏，但短期赔率一般。',
                        'snapshot': {},
                        'meta': {},
                    },
                },
            }
        )

        response = self.client.get(f"/holding-reanalysis?holding_stock_id={holding_stock['id']}&record_id={saved['id']}")

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert '持仓二次分析' in content
        assert 'data-page="holding-reanalysis"' in content
        assert '历史再评估记录' in content
        assert '关联持仓：' in content
        assert holding_stock['id'] in content
        assert saved['id'] in content
        assert 'holding_reanalysis_tabs' in content
        assert f'/holding-reanalysis?holding_stock_id={holding_stock["id"]}&record_id={saved["id"]}' in content
        assert '已加载历史持仓再评估记录' in content

    def test_stock_analysis_record_api_supports_holding_reanalysis_history(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'holding-reanalysis-api.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        watch_stock = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
                'industry': '新能源',
            },
        ).get_json()['data']
        holding_stock = self.client.post(
            f"/api/trading-decision/holding-stocks/from-watch/{watch_stock['id']}/buy",
            json={
                'trade_date': '2026-04-29',
                'quantity': 200,
                'price': 180,
                'current_price': 182.4,
                'note': '从关注转持仓',
            },
        ).get_json()['data']

        save_response = self.client.post(
            '/api/trading-decision/stock-analysis-records',
            json={
                'holding_stock_id': holding_stock['id'],
                'analysis_scene': 'holding_reanalysis',
                'trade_date': '2026-04-29',
                'raw_result': {
                    'success': True,
                    'data': {
                        'stock_code': '300750',
                        'stock_name': '宁德时代',
                        'market': 'A股',
                        'trade_date': '2026-04-29',
                        'decision': {'summary': '继续持有并跟踪行业景气。', 'risk_level': 'medium'},
                        'scores': {'technical': 66, 'fundamental': 80, 'sentiment': 64, 'composite': 71},
                        'signals': [{'signal': '震荡上行', 'detail': '趋势未破坏'}],
                        'risks': [{'risk': '行业波动', 'detail': '估值切换可能放大波动'}],
                        'evidence': [],
                        'logic': '产业趋势仍在，但需要跟踪需求侧变化。',
                        'snapshot': {},
                        'meta': {},
                    },
                },
            },
        )

        assert save_response.status_code == 200
        saved = save_response.get_json()['data']
        assert saved['holding_stock_id'] == holding_stock['id']
        assert saved['watch_stock_id'] == watch_stock['id']
        assert saved['analysis_scene'] == 'holding_reanalysis'

        list_response = self.client.get(f"/api/trading-decision/stock-analysis-records?holding_stock_id={holding_stock['id']}")
        assert list_response.status_code == 200
        records = list_response.get_json()['data']
        assert len(records) == 1
        assert records[0]['id'] == saved['id']
        assert records[0]['analysis_scene'] == 'holding_reanalysis'

        holding_detail = self.client.get(f"/api/trading-decision/holding-stocks/{holding_stock['id']}").get_json()['data']
        assert holding_detail['suggested_action'] == '继续持有并跟踪行业景气。'
        assert holding_detail['last_review_at'] == '2026-04-29'

    def test_watch_stocks_page_renders_stock_analysis_history_records(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'watch-stock-analysis-history.sqlite3'))
        service = self.original_service.__class__()
        web_app_context.trading_decision_service = service

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        service.save_stock_analysis_record(
            {
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-27',
                        'analysis_mode': 'agentic',
                        'stance': 'bullish',
                        'time_horizon': '3-10 trading days',
                        'logic': '趋势修复',
                        'decision': {'summary': '趋势修复，适合继续跟踪。', 'risk_level': 'medium'},
                        'scores': {'composite': 75},
                        'signals': [],
                        'risks': [],
                        'evidence': [],
                        'meta': {},
                        'snapshot': {},
                    },
                },
            }
        )

        response = self.client.get('/watch-stocks')

        assert response.status_code == 200
        assert '股票分析记录'.encode() in response.data
        assert '趋势修复，适合继续跟踪。'.encode() in response.data
        assert 'bullish'.encode() in response.data
        assert f'/stock-analysis-record?watch_stock_id={watch_stock["id"]}'.encode() in response.data
        assert '股票分析记录待接入独立历史库'.encode() not in response.data
        assert '暂无股票分析记录'.encode() not in response.data

    def test_trade_plan_record_api_persists_and_lists_history(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'trade-plan-save.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        raw_result = {
            'success': True,
            'data': {
                'trade_date': '2026-04-27',
                'decision': {
                    'action': 'buy',
                    'summary': '回踩后具备分批建仓条件。',
                    'logic': '趋势修复，仓位按三笔推进。',
                    'risk_level': 'medium',
                    'time_horizon': '3-10 trading days',
                    'risks': ['板块波动仍大'],
                    'position_suggestion': {
                        'target_position': '30%-50%',
                        'add_condition': '放量突破关键压力位后分批加仓',
                        'reduce_condition': '跌回突破位下方时减仓',
                        'stop_loss_reference': '跌破最近关键支撑位时止损',
                    },
                },
                'scores': {
                    'technical': 78,
                    'sentiment': 71,
                    'composite': 75,
                },
            },
        }

        save_response = self.client.post(
            '/api/trading-decision/trade-plan-analysis-records',
            json={
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'plan_type': '三笔计划',
                'risk_preference': '中高风险',
                'raw_result': raw_result,
            },
        )

        assert save_response.status_code == 200
        saved = save_response.get_json()['data']
        assert saved['watch_stock_id'] == watch_stock['id']
        assert saved['suggested_action'] == '适合买入'
        assert saved['conclusion_summary'] == '回踩后具备分批建仓条件。'
        assert saved['entry_plan_json']['scores']['composite'] == 75

        list_response = self.client.get(f"/api/trading-decision/trade-plan-analysis-records?watch_stock_id={watch_stock['id']}")
        assert list_response.status_code == 200
        records = list_response.get_json()['data']
        assert len(records) == 1
        assert records[0]['id'] == saved['id']

        detail_response = self.client.get(f"/api/trading-decision/trade-plan-analysis-records/{saved['id']}")
        assert detail_response.status_code == 200
        detail = detail_response.get_json()['data']
        assert detail['id'] == saved['id']
        assert detail['raw_result_json']['data']['decision']['action'] == 'buy'

        watch_stock_response = self.client.get(f"/api/trading-decision/watch-stocks/{watch_stock['id']}")
        updated_watch_stock = watch_stock_response.get_json()['data']
        assert updated_watch_stock['suggested_action'] == '适合买入'
        assert updated_watch_stock['last_conclusion_summary'] == '回踩后具备分批建仓条件。'
        assert updated_watch_stock['last_analysis_at'] == '2026-04-27'

    def test_trade_plan_page_can_open_saved_record(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'trade-plan-record-page.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '300750',
                'stock_name': '宁德时代',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        save_response = self.client.post(
            '/api/trading-decision/trade-plan-analysis-records',
            json={
                'watch_stock_id': watch_stock['id'],
                'trade_date': '2026-04-27',
                'plan_type': '三笔计划',
                'risk_preference': '中高风险',
                'raw_result': {
                    'success': True,
                    'data': {
                        'trade_date': '2026-04-27',
                        'trade_plan_markdown': '## 一、计划摘要\n\n- 标的名称：宁德时代\n\n---\n\n## 二、买前约束条件\n\n- 当前价值阶段是否仍成立：是',
                        'decision': {
                            'action': 'hold',
                            'summary': '等待右侧确认后再推进仓位。',
                            'risk_level': 'medium',
                            'position_suggestion': {
                                'target_position': '10%-30%',
                                'position_limit': '10%-30%',
                                'add_condition': '站稳右侧确认位后小幅加仓',
                                'reduce_condition': '跌破观察位时降仓',
                                'stop_loss_reference': '跌破区间下沿时止损',
                            },
                        },
                        'meta': {
                            'template_name': '持仓计划模板（买前执行版）',
                            'data_source': 'cache_first',
                            'cache_hits': ['A股_300750_宁德时代_20260427_进场决策.md'],
                        },
                        'scores': {'composite': 63},
                    },
                },
            },
        )
        record_id = save_response.get_json()['data']['id']

        response = self.client.get(f"/trade-plan-analysis?watch_stock_id={watch_stock['id']}&record_id={record_id}")

        assert response.status_code == 200
        assert record_id.encode() in response.data
        assert '等待右侧确认后再推进仓位。'.encode() in response.data
        assert '查看原始 JSON'.encode() in response.data
        assert '持仓计划分析记录'.encode() in response.data

    def test_watch_stocks_page_updates_trade_plan_copy(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'watch-stocks-trade-plan-copy.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        create_response = self.client.post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']

        response = self.client.get('/watch-stocks')

        assert response.status_code == 200
        assert '进场决策、股票分析与持仓计划分析均已接入真实页面'.encode() in response.data
        assert '持仓计划分析当前仍跳转到既有壳页'.encode() not in response.data
        assert f"/trade-plan-analysis?watch_stock_id={watch_stock['id']}".encode() in response.data

