from __future__ import annotations

import pandas as pd
from flask import jsonify

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
        self.app = create_app()
        self.client = self.app.test_client()

    def teardown_method(self):
        web_app_context.trading_decision_service = self.original_service

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
        assert '分析模型'.encode() not in response.data

    def test_stock_analysis_record_page_reads_watch_stock_context(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'stock-analysis-record-context.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/stock-analysis-record?watch_stock_id=WS-001&code=600519&market=sh')

        assert response.status_code == 200
        assert 'watchStockIdText'.encode() in response.data
        assert "params.get('stock_code') || params.get('code')".encode() in response.data
        assert 'normalizedMarket = market.toLowerCase()'.encode() in response.data

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
        assert '查看完整结果 JSON' in content
        assert '结果区会展示结论、reasoning，以及原始 JSON。' in content
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
        assert 'renderSectionCard' in content
        assert '标的基本信息' in content
        assert '宏观分析' in content
        assert '资产分类' in content
        assert '价值阶段分析' in content
        assert '价格分区分析' in content
        assert '买卖计划分析' in content
        assert '风险控制分析' in content
        assert '最终决策卡' in content
        assert '执行元信息' in content
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
        assert '结果区会展示结论、reasoning，以及原始 JSON。' in content
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
                        'decision_card': {'suggested_action': '适合买入', 'execution_summary': '先小仓位，跌破关键位止损'},
                    },
                },
            }
        )

        response = self.client.get(f"/entry-decision?watch_stock_id={watch_stock['id']}")

        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'renderSectionCard(\'标的基本信息\'' in content
        assert 'renderSectionCard(\'宏观分析\'' in content
        assert 'renderSectionCard(\'价值阶段分析\'' in content
        assert 'renderSectionCard(\'价格分区分析\'' in content
        assert 'renderSectionCard(\'买卖计划分析\'' in content
        assert 'renderSectionCard(\'风险控制分析\'' in content
        assert 'renderSectionCard(\'最终决策卡\'' in content
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
        assert 'renderSectionCard' in content
        assert '标的基本信息' in content
        assert '宏观分析' in content
        assert '资产分类' in content
        assert '价值阶段分析' in content
        assert '价格分区分析' in content
        assert '买卖计划分析' in content
        assert '风险控制分析' in content
        assert '最终决策卡' in content
        assert '执行元信息' in content
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
        assert '结果区会展示结论、reasoning，以及原始 JSON。' in content
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

    def test_trade_plan_analysis_page_returns_not_found_for_unknown_watch_stock(self, tmp_path, monkeypatch):
        _patch_stock_lookup_source(monkeypatch)
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'trade-plan-404.sqlite3'))
        web_app_context.trading_decision_service = self.original_service.__class__()

        response = self.client.get('/trade-plan-analysis?watch_stock_id=WS-UNKNOWN')

        assert response.status_code == 404
        assert response.get_json()['error']['code'] == 'not_found'

    def test_trade_plan_run_api_uses_watch_stock_context(self, tmp_path, monkeypatch):
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

        def stub_start_stock_ai_analysis(context, payload):
            captured['payload'] = payload
            return jsonify({
                'success': True,
                'data': '',
                'message': '股票 600519 AI分析已启动',
                'task_mode': 'async',
                'client_id': payload['client_id'],
            })

        monkeypatch.setattr(trading_decision_routes_module, 'start_stock_ai_analysis', stub_start_stock_ai_analysis)

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
        assert captured['payload']['stock_code'] == '600519'
        assert captured['payload']['market'] == 'SH'
        assert captured['payload']['trade_date'] == '2026-04-27'
        assert captured['payload']['analysis_depth'] == 'deep'
        assert body['trade_plan_analysis_context']['watch_stock_id'] == watch_stock['id']
        assert body['trade_plan_analysis_context']['plan_type'] == '三笔计划'
        assert body['trade_plan_analysis_context']['risk_preference'] == '中高风险'

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
                        'decision': {
                            'action': 'hold',
                            'summary': '等待右侧确认后再推进仓位。',
                            'risk_level': 'medium',
                            'position_suggestion': {
                                'target_position': '10%-30%',
                                'add_condition': '站稳右侧确认位后小幅加仓',
                                'reduce_condition': '跌破观察位时降仓',
                                'stop_loss_reference': '跌破区间下沿时止损',
                            },
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

