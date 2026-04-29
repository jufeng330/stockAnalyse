from __future__ import annotations

from types import SimpleNamespace

from stock_analyse.application.dto.entry_decision_state import EntryDecisionState
from stock_analyse.interfaces.web.app import create_app, web_app_context
from stock_analyse.interfaces.web.routes import trading_decision as trading_decision_routes_module
from stock_analyse.interfaces.web.services.stock_analyzer_service import StockAnalyzerService
from stock_analyse.interfaces.web.services.trading_decision_service import TradingDecisionService


class TestTradePlanCacheWrites:
    def test_service_writes_entry_decision_cache_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'cache.sqlite3'))
        service = TradingDecisionService()
        service.trade_plan_cache_dir = tmp_path / 'tranding_plan'

        path = service.save_result_markdown_cache(
            'entry_decision',
            {
                'success': True,
                'data': {
                    'market': 'A股',
                    'stock_code': '600519',
                    'stock_name': '贵州茅台',
                    'trade_date': '2026-04-28',
                    'entry_decision_summary_markdown': '## 一、标的基本信息\n\n- 标的名称：贵州茅台',
                },
            },
        )

        assert path is not None
        assert path.endswith('A股_600519_贵州茅台_Strategy_20260428_.md')
        assert (tmp_path / 'tranding_plan' / 'A股_600519_贵州茅台_Strategy_20260428_.md').read_text(encoding='utf-8').startswith('## 一、标的基本信息')

    def test_service_reads_new_biz_filename_pattern(self, tmp_path, monkeypatch):
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'cache-read.sqlite3'))
        service = TradingDecisionService()
        service.trade_plan_cache_dir = tmp_path / 'tranding_plan'
        service.trade_plan_cache_dir.mkdir(parents=True, exist_ok=True)
        entry_file = service.trade_plan_cache_dir / 'A股_600519_贵州茅台_Strategy_20260428_.md'
        analysis_file = service.trade_plan_cache_dir / 'A股_600519_贵州茅台_analyse_20260428_.md'
        entry_file.write_text('## 进场决策\n\n- 结论：适合买入', encoding='utf-8')
        analysis_file.write_text('# 股票分析\n\n- 立场：偏多', encoding='utf-8')

        context = service._load_trade_plan_cache_context(
            {'stock_code': '600519', 'stock_name': '贵州茅台', 'market': 'A股'},
            '2026-04-28',
        )

        assert entry_file.name in context['cache_hits']
        assert analysis_file.name in context['cache_hits']
        assert context['entry_decision_markdown'].startswith('## 进场决策')
        assert context['stock_analysis_markdown'].startswith('# 股票分析')
        assert set(context['hit_types']) == {'entry_decision', 'stock_analysis'}

    def test_find_daily_result_cache_reads_exact_biz_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'cache-find.sqlite3'))
        service = TradingDecisionService()
        service.trade_plan_cache_dir = tmp_path / 'tranding_plan'
        service.trade_plan_cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = service.trade_plan_cache_dir / 'A股_600519_贵州茅台_analyse_20260428_.md'
        cache_file.write_text('# 股票分析\n\n- 立场：偏多', encoding='utf-8')

        result = service.find_daily_result_cache(
            market='A股',
            stock_code='600519',
            stock_name='贵州茅台',
            trade_date='2026-04-28',
            result_type='stock_analysis',
        )

        assert result['hit'] is True
        assert result['result_source'] == 'cache'
        assert result['file_name'] == cache_file.name
        assert result['markdown'].startswith('# 股票分析')

    def test_build_cached_entry_decision_result_marks_cache_source(self, tmp_path, monkeypatch):
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'cache-entry-result.sqlite3'))
        service = TradingDecisionService()
        service.trade_plan_cache_dir = tmp_path / 'tranding_plan'
        service.trade_plan_cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = service.trade_plan_cache_dir / 'A股_600519_贵州茅台_Strategy_20260428_.md'
        cache_file.write_text('## 进场决策\n\n- 结论：适合买入', encoding='utf-8')

        result = service.build_cached_entry_decision_result(
            watch_stock={'id': 'WS-1', 'stock_code': '600519', 'stock_name': '贵州茅台', 'market': 'A股'},
            trade_date='2026-04-28',
        )

        assert result is not None
        assert result['data']['result_source'] == 'cache'
        assert result['data']['cache_file'] == cache_file.name
        assert result['data']['entry_decision_summary_markdown'].startswith('## 进场决策')

    def test_run_entry_decision_task_writes_cache(self, tmp_path, monkeypatch):
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'entry-task.sqlite3'))
        service = TradingDecisionService()
        app = create_app()
        original_service = web_app_context.trading_decision_service
        web_app_context.trading_decision_service = service

        create_response = app.test_client().post(
            '/api/trading-decision/watch-stocks',
            json={
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
                'asset_type': '成长龙头',
            },
        )
        watch_stock = create_response.get_json()['data']
        session = service.create_entry_decision_session(watch_stock['id'], {'trade_date': '2026-04-28'})

        captured = {}

        class StubOrchestrator:
            def run(self, **kwargs):
                state = kwargs['state']
                state.status = 'completed'
                state.final_result = {
                    'success': True,
                    'data': {
                        'market': 'A股',
                        'stock_code': '600519',
                        'stock_name': '贵州茅台',
                        'trade_date': '2026-04-28',
                        'entry_decision_summary_markdown': '## 一、标的基本信息\n\n- 标的名称：贵州茅台',
                        'meta': {},
                    },
                }
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

        def stub_save(result_type, result, watch_stock_arg=None):
            captured['result_type'] = result_type
            captured['result'] = result
            captured['watch_stock'] = watch_stock_arg
            return str(tmp_path / 'A股_600519_贵州茅台_Strategy_20260428_.md')

        monkeypatch.setattr(trading_decision_routes_module, '_build_entry_decision_orchestrator', lambda: StubOrchestrator())
        monkeypatch.setattr(trading_decision_routes_module, 'StreamingAnalyzer', StubStreamer)
        monkeypatch.setattr(service, 'save_result_markdown_cache', stub_save)

        try:
            with app.app_context():
                context = trading_decision_routes_module._context()
                context.sse_manager = object()
                context.settings = SimpleNamespace(ai=SimpleNamespace(platform='mock', model_name='mock', api_key='key', system_prompt='prompt'))
                trading_decision_routes_module._run_entry_decision_session_task(session['id'], 'client-1', context, service)
        finally:
            web_app_context.trading_decision_service = original_service

        assert captured['result_type'] == 'entry_decision'
        assert captured['watch_stock']['stock_code'] == '600519'
        assert captured['result']['data']['entry_decision_summary_markdown'].startswith('## 一、标的基本信息')

    def test_run_trade_plan_task_writes_cache(self, tmp_path, monkeypatch):
        monkeypatch.setenv('STOCK_ANALYSE_TRADING_DECISION_DB_PATH', str(tmp_path / 'trade-task.sqlite3'))
        service = TradingDecisionService()
        original_service = web_app_context.trading_decision_service
        original_settings = web_app_context.settings
        web_app_context.trading_decision_service = service

        if not hasattr(web_app_context.settings, 'web'):
            web_app_context.settings = SimpleNamespace(
                web=SimpleNamespace(flask_secret_key='test-secret'),
                ai=getattr(original_settings, 'ai', SimpleNamespace()),
            )

        app = create_app()

        captured = {}

        class StubOrchestrator:
            def run(self, **kwargs):
                return {
                    'success': True,
                    'data': {
                        'market': 'A股',
                        'stock_code': '600519',
                        'stock_name': '贵州茅台',
                        'trade_date': '2026-04-28',
                        'trade_plan_markdown': '## 一、计划摘要\n\n- 标的名称：贵州茅台',
                        'decision': {'action': 'buy', 'position_suggestion': {}},
                        'meta': {},
                    },
                }

        class StubStreamer:
            def __init__(self, *args, **kwargs):
                pass

            def send_log(self, *args, **kwargs):
                pass

            def send_progress(self, *args, **kwargs):
                pass

            def send_final_result(self, *args, **kwargs):
                pass

            def send_completion(self, *args, **kwargs):
                pass

            def send_error(self, *args, **kwargs):
                pass

        def stub_save(result_type, result, watch_stock_arg=None):
            captured['result_type'] = result_type
            captured['result'] = result
            captured['watch_stock'] = watch_stock_arg
            return str(tmp_path / 'A股_600519_贵州茅台_plan_20260428_.md')

        monkeypatch.setattr(trading_decision_routes_module, '_build_trade_plan_analysis_orchestrator', lambda: StubOrchestrator())
        monkeypatch.setattr(trading_decision_routes_module, 'StreamingAnalyzer', StubStreamer)
        monkeypatch.setattr(service, 'save_result_markdown_cache', stub_save)

        trade_plan_context = {
            'watch_stock': {'id': 'WS-1', 'stock_code': '600519', 'stock_name': '贵州茅台', 'market': 'A股'},
            'request': {'trade_date': '2026-04-28'},
            'cache_context': {},
            'fallback_context': {},
            'data_source': 'fallback_only',
        }

        try:
            with app.app_context():
                context = trading_decision_routes_module._context()
                context.sse_manager = object()
                context.settings = SimpleNamespace(
                    ai=SimpleNamespace(platform='mock', model_name='mock', api_key='key', system_prompt='prompt'),
                    web=SimpleNamespace(flask_secret_key='test-secret'),
                )
                trading_decision_routes_module._run_trade_plan_analysis_task('WS-1', 'client-2', context, service, trade_plan_context)
        finally:
            web_app_context.trading_decision_service = original_service
            web_app_context.settings = original_settings

        assert captured['result_type'] == 'trade_plan'
        assert captured['watch_stock']['stock_name'] == '贵州茅台'
        assert captured['result']['data']['trade_plan_markdown'].startswith('## 一、计划摘要')

    def test_stock_ai_analysis_process_writes_cache(self, tmp_path, monkeypatch):
        captured = {}

        class StubStreaming:
            def send_log(self, *args, **kwargs):
                pass

            def send_progress(self, *args, **kwargs):
                pass

            def send_scores(self, payload):
                captured['scores'] = payload

            def send_final_result(self, payload):
                captured['final_result'] = payload

            def send_error(self, payload):
                captured['error'] = payload

        def stub_execute(**kwargs):
            return {
                'success': True,
                'data': {
                    'market': 'A股',
                    'stock_code': '600519',
                    'stock_name': '贵州茅台',
                    'trade_date': '2026-04-28',
                    'decision': {'logic': '趋势修复'},
                    'scores': {'technical': 81, 'sentiment': 70, 'composite': 76},
                    'risks': ['波动仍大'],
                    'time_horizon': '3-10 trading days',
                    'logic': '趋势修复',
                    'meta': {},
                    'snapshot': {},
                },
            }

        def stub_save(self, result_type, result, watch_stock=None):
            captured['result_type'] = result_type
            captured['cached_result'] = result
            captured['watch_stock'] = watch_stock
            return str(tmp_path / 'A股_600519_贵州茅台_analyse_20260428_.md')

        monkeypatch.setattr('stock_analyse.interfaces.web.services.stock_analyzer_service.analyze_single_stock_ai_use_case.execute', stub_execute)
        monkeypatch.setattr(TradingDecisionService, 'save_result_markdown_cache', stub_save)

        service = StockAnalyzerService()
        service.streaming = StubStreaming()

        result = service.stock_ai_analysis_process('600519', 'A股', '2026-01-01', '2026-04-28', trade_date='2026-04-28')

        assert result['success'] is True
        assert captured['result_type'] == 'stock_analysis'
        assert captured['cached_result']['data']['stock_code'] == '600519'
        assert captured['watch_stock'] is None
        assert captured['scores']['comprehensive'] == 76
        assert captured['final_result']['data']['trade_date'] == '2026-04-28'

    def test_stock_ai_analysis_process_saves_watch_stock_history_when_watch_stock_id_present(self, tmp_path, monkeypatch):
        captured = {}

        class StubStreaming:
            def send_log(self, *args, **kwargs):
                pass

            def send_progress(self, *args, **kwargs):
                pass

            def send_scores(self, payload):
                captured['scores'] = payload

            def send_final_result(self, payload):
                captured['final_result'] = payload

            def send_error(self, payload):
                captured['error'] = payload

        def stub_execute(**kwargs):
            return {
                'success': True,
                'data': {
                    'market': 'A股',
                    'stock_code': '600519',
                    'stock_name': '贵州茅台',
                    'trade_date': '2026-04-28',
                    'analysis_mode': 'agentic',
                    'stance': 'bullish',
                    'decision': {'summary': '趋势修复，适合继续跟踪。', 'risk_level': 'medium'},
                    'scores': {'technical': 81, 'sentiment': 70, 'composite': 76},
                    'signals': [{'signal': '放量回升', 'detail': '量价共振'}],
                    'risks': [{'risk': '波动仍大', 'detail': '短期回撤风险'}],
                    'evidence': [{'type': 'price_action', 'detail': '均线拐头向上'}],
                    'time_horizon': '3-10 trading days',
                    'logic': '趋势修复',
                    'meta': {},
                    'snapshot': {},
                },
            }

        def stub_save(self, result_type, result, watch_stock=None):
            captured['result_type'] = result_type
            captured['cached_result'] = result
            captured['watch_stock'] = watch_stock
            return str(tmp_path / 'A股_600519_贵州茅台_analyse_20260428_.md')

        def stub_save_record(self, payload):
            captured['saved_record_payload'] = payload
            return {'id': 'SAR-TEST001'}

        monkeypatch.setattr('stock_analyse.interfaces.web.services.stock_analyzer_service.analyze_single_stock_ai_use_case.execute', stub_execute)
        monkeypatch.setattr(TradingDecisionService, 'save_result_markdown_cache', stub_save)
        monkeypatch.setattr(TradingDecisionService, 'save_stock_analysis_record', stub_save_record)

        service = StockAnalyzerService()
        service.streaming = StubStreaming()

        result = service.stock_ai_analysis_process(
            '600519',
            'A股',
            '2026-01-01',
            '2026-04-28',
            trade_date='2026-04-28',
            watch_stock_id='WS-1',
            stock_name='贵州茅台',
        )

        assert result['success'] is True
        assert captured['result_type'] == 'stock_analysis'
        assert captured['watch_stock']['id'] == 'WS-1'
        assert captured['watch_stock']['stock_code'] == '600519'
        assert captured['watch_stock']['stock_name'] == '贵州茅台'
        assert captured['saved_record_payload']['watch_stock_id'] == 'WS-1'
        assert captured['saved_record_payload']['trade_date'] == '2026-04-28'
        assert captured['saved_record_payload']['raw_result']['data']['stock_code'] == '600519'
        assert captured['scores']['comprehensive'] == 76
        assert captured['final_result']['data']['trade_date'] == '2026-04-28'

    def test_stock_ai_analysis_process_saves_holding_reanalysis_history_when_holding_stock_id_present(self, tmp_path, monkeypatch):
        captured = {}

        class StubStreaming:
            def send_log(self, *args, **kwargs):
                pass

            def send_progress(self, *args, **kwargs):
                pass

            def send_scores(self, payload):
                captured['scores'] = payload

            def send_final_result(self, payload):
                captured['final_result'] = payload

            def send_error(self, payload):
                captured['error'] = payload

        def stub_execute(**kwargs):
            return {
                'success': True,
                'data': {
                    'market': 'A股',
                    'stock_code': '600519',
                    'stock_name': '贵州茅台',
                    'trade_date': '2026-04-28',
                    'analysis_mode': 'agentic',
                    'decision': {'summary': '继续持有，等待下一次验证。', 'risk_level': 'medium'},
                    'scores': {'technical': 81, 'sentiment': 70, 'composite': 76},
                    'signals': [{'signal': '放量回升', 'detail': '量价共振'}],
                    'risks': [{'risk': '波动仍大', 'detail': '短期回撤风险'}],
                    'evidence': [{'type': 'price_action', 'detail': '均线拐头向上'}],
                    'time_horizon': '3-10 trading days',
                    'logic': '趋势修复',
                    'meta': {},
                    'snapshot': {},
                },
            }

        def stub_save(self, result_type, result, watch_stock=None):
            captured['result_type'] = result_type
            captured['cached_result'] = result
            captured['watch_stock'] = watch_stock
            return str(tmp_path / 'A股_600519_贵州茅台_holding_reanalysis_20260428_.md')

        def stub_save_record(self, payload):
            captured['saved_record_payload'] = payload
            return {'id': 'SAR-TEST002'}

        monkeypatch.setattr('stock_analyse.interfaces.web.services.stock_analyzer_service.analyze_single_stock_ai_use_case.execute', stub_execute)
        monkeypatch.setattr(TradingDecisionService, 'save_result_markdown_cache', stub_save)
        monkeypatch.setattr(TradingDecisionService, 'save_stock_analysis_record', stub_save_record)

        service = StockAnalyzerService()
        service.streaming = StubStreaming()

        result = service.stock_ai_analysis_process(
            '600519',
            'A股',
            '2026-01-01',
            '2026-04-28',
            trade_date='2026-04-28',
            watch_stock_id='WS-1',
            holding_stock_id='HS-1',
            stock_name='贵州茅台',
            analysis_scene='holding_reanalysis',
        )

        assert result['success'] is True
        assert captured['result_type'] == 'holding_reanalysis'
        assert captured['watch_stock']['id'] == 'WS-1'
        assert captured['saved_record_payload']['holding_stock_id'] == 'HS-1'
        assert captured['saved_record_payload']['analysis_scene'] == 'holding_reanalysis'
        assert captured['saved_record_payload']['trade_date'] == '2026-04-28'
        assert captured['saved_record_payload']['raw_result']['data']['stock_code'] == '600519'
        assert captured['scores']['comprehensive'] == 76
        assert captured['final_result']['data']['trade_date'] == '2026-04-28'
