from __future__ import annotations

import logging
from datetime import datetime
from uuid import uuid4

from flask import current_app, jsonify, render_template, request

from stock_analyse.application.orchestrators.entry_decision_orchestrator import EntryDecisionOrchestrator
from stock_analyse.interfaces.web.routes.analysis import build_stock_ai_payload, start_stock_ai_analysis
from stock_analyse.interfaces.web.streaming.streaming_analyzer import StreamingAnalyzer
from stock_analyse.interfaces.web.services.trading_decision_service import TradingDecisionService


API_PREFIX = '/api/trading-decision/watch-stocks'
TRADE_PLAN_RECORDS_API = '/api/trading-decision/trade-plan-analysis-records'
ENTRY_DECISION_SESSIONS_API = '/api/trading-decision/entry-decisions'
ENTRY_DECISION_RECORDS_API = '/api/trading-decision/entry-decision-records'

logger = logging.getLogger(__name__)


def _context():
    return current_app.extensions['stock_analyse.context']


def _service() -> TradingDecisionService:
    return _context().trading_decision_service


def _json_success(data, message: str = ''):
    return jsonify({'success': True, 'data': data, 'message': message})


def _json_error(message: str, status_code: int, code: str):
    return jsonify({'success': False, 'message': message, 'error': {'code': code, 'message': message}}), status_code


def _map_ai_decision_action_to_label(action: str | None) -> str:
    normalized = (action or '').strip().lower()
    mapping = {
        'buy': '适合买入',
        'hold': '继续观察',
        'watch': '继续观察',
        'sell': '不适合买入',
    }
    return mapping.get(normalized, action or '')


def _build_entry_decision_summary_fields(result: dict, trade_date: str) -> dict:
    data = result.get('data') or {}
    decision_card = data.get('decision_card') or {}
    risk_control = data.get('risk_control_analysis') or {}
    buy_plan = data.get('buy_plan_analysis') or {}
    value_stage = data.get('value_stage_analysis') or {}
    price_zone = data.get('price_zone_analysis') or {}
    return {
        'current_stage': str(decision_card.get('current_stage') or value_stage.get('current_stage') or '').strip(),
        'current_price_zone': str(decision_card.get('current_price_zone') or price_zone.get('price_zone') or '').strip(),
        'suggested_action': str(decision_card.get('suggested_action') or buy_plan.get('suggested_action') or '').strip(),
        'last_conclusion_summary': str(risk_control.get('conclusion_summary') or decision_card.get('execution_summary') or '').strip(),
        'last_analysis_at': trade_date or data.get('trade_date') or '',
    }


def _build_entry_decision_orchestrator() -> EntryDecisionOrchestrator:
    return EntryDecisionOrchestrator()


def _build_client_id(payload: dict) -> str:
    provided = (payload.get('client_id') or '').strip()
    return provided or f'entry_decision_{uuid4().hex[:12]}'


def _run_entry_decision_session_task(session_id: str, client_id: str) -> None:
    context = _context()
    service = _service()
    streamer = StreamingAnalyzer(client_id, context.sse_manager)
    lock_name = f'entry_decision_{session_id}'
    session = service.get_entry_decision_session(session_id)
    if not session:
        return

    try:
        state = service.build_entry_decision_state(session_id)
        settings = context.settings.ai
        orchestrator = _build_entry_decision_orchestrator()
        streamer.send_log(f"🚀 开始进场决策分析: {state.watch_stock.get('stock_code', '')}", 'header')
        streamer.send_progress('singleProgress', 5, '正在初始化进场决策分析...')
        updated_state = orchestrator.run(
            state=state,
            llm_provider=settings.platform,
            llm_model=settings.model_name,
            api_code=settings.api_key,
            system_prompt=settings.system_prompt,
            callbacks={
                'send_log': streamer.send_log,
                'send_progress': streamer.send_progress,
                'send_role_result': streamer.send_role_result,
                'send_pause': streamer.send_pause,
            },
        )
        service.update_entry_decision_session_from_state(updated_state)
        if updated_state.status == 'paused':
            streamer.send_log('🚀 进场决策已暂停，等待人工补充输入', 'header')
            return
        streamer.send_final_result(updated_state.final_result)
        streamer.send_progress('singleProgress', 100, '进场决策分析完成')
        streamer.send_completion(f"进场决策分析完成: {state.watch_stock.get('stock_code', '')}")
        streamer.send_log(f"🚀 进场决策分析完成: {state.watch_stock.get('stock_code', '')}", 'header')
    except Exception as exc:
        logger.exception('进场决策分析失败: %s', exc)
        try:
            state = service.build_entry_decision_state(session_id)
            state.status = 'failed'
            state.add_error(state.current_role, str(exc))
            service.update_entry_decision_session_from_state(state)
        except Exception:
            logger.exception('更新失败状态时出错')
        streamer.send_progress('singleProgress', 100, '进场决策分析失败')
        streamer.send_error(f'进场决策分析失败: {exc}')
        streamer.send_completion(f'进场决策分析失败: {exc}')
    finally:
        with context.task_lock:
            context.analysis_tasks.pop(lock_name, None)


def _start_entry_decision_session(session_id: str, client_id: str):
    context = _context()
    lock_name = f'entry_decision_{session_id}'
    with context.task_lock:
        if lock_name in context.analysis_tasks:
            return False, jsonify({'success': False, 'error': '当前进场决策任务正在执行，请稍候'}), 429
        context.analysis_tasks[lock_name] = {
            'start_time': datetime.now(),
            'status': 'analyzing',
            'client_id': client_id,
        }

    try:
        context.executor.submit(_run_entry_decision_session_task, session_id, client_id)
        return True, None, 200
    except Exception as exc:
        logger.exception('启动进场决策任务失败: %s', exc)
        with context.task_lock:
            context.analysis_tasks.pop(lock_name, None)
        return False, jsonify({'success': False, 'error': str(exc)}), 500


def register_trading_decision_routes(app):
    @app.route('/index', methods=['GET'])
    @app.route('/watch-stocks', methods=['GET'])
    def watch_stocks_page():
        page_data = _service().build_watch_stocks_page_data(request.args.to_dict())
        return render_template('watch_stocks.html', **page_data)

    @app.route('/entry-decision', methods=['GET'])
    def entry_decision_page():
        watch_stock_id = (request.args.get('watch_stock_id') or '').strip()
        if not watch_stock_id:
            return _json_error('缺少 watch_stock_id', 400, 'bad_request')
        try:
            page_data = _service().build_entry_decision_page_data(watch_stock_id)
        except ValueError:
            return _json_error('关注股票不存在', 404, 'not_found')
        return render_template('entry_decision.html', **page_data)

    @app.route('/trade-plan-analysis', methods=['GET'])
    def trade_plan_analysis_page():
        watch_stock_id = (request.args.get('watch_stock_id') or '').strip()
        record_id = (request.args.get('record_id') or '').strip() or None
        if not watch_stock_id:
            return _json_error('缺少 watch_stock_id', 400, 'bad_request')
        try:
            page_data = _service().build_trade_plan_analysis_page_data(watch_stock_id, record_id)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)
        return render_template('trade_plan_analysis.html', **page_data)

    @app.route(f'{API_PREFIX}/stock-search', methods=['GET'])
    def search_stock_candidates_api():
        query = request.args.get('query', '')
        market = request.args.get('market', '')
        limit = request.args.get('limit', 20)
        return _json_success(_service().search_stock_candidates(query=query, market=market, limit=int(limit or 20)))

    @app.route(API_PREFIX, methods=['GET'])
    def list_watch_stocks_api():
        return _json_success(_service().list_watch_stocks(request.args.to_dict()))

    @app.route(API_PREFIX, methods=['POST'])
    def create_watch_stock_api():
        payload = request.get_json(silent=True) or {}
        try:
            created = _service().create_watch_stock(payload)
        except ValueError as error:
            return _json_error(str(error), 400, 'bad_request')
        return _json_success(created, '关注股票创建成功')

    @app.route(f'{API_PREFIX}/<watch_stock_id>', methods=['GET'])
    def get_watch_stock_api(watch_stock_id: str):
        watch_stock = _service().get_watch_stock(watch_stock_id)
        if not watch_stock:
            return _json_error('关注股票不存在', 404, 'not_found')
        return _json_success(watch_stock)

    @app.route(f'{API_PREFIX}/<watch_stock_id>', methods=['PUT'])
    def update_watch_stock_api(watch_stock_id: str):
        payload = request.get_json(silent=True) or {}
        updated = _service().update_watch_stock(watch_stock_id, payload)
        if not updated:
            return _json_error('关注股票不存在', 404, 'not_found')
        return _json_success(updated, '关注股票更新成功')

    @app.route(f'{API_PREFIX}/<watch_stock_id>/archive', methods=['POST'])
    def archive_watch_stock_api(watch_stock_id: str):
        archived = _service().archive_watch_stock(watch_stock_id)
        if not archived:
            return _json_error('关注股票不存在', 404, 'not_found')
        return _json_success(archived, '关注股票已归档')

    @app.route(f'{API_PREFIX}/<watch_stock_id>/entry-decision/analyze', methods=['POST'])
    def analyze_entry_decision_api(watch_stock_id: str):
        watch_stock = _service().get_watch_stock(watch_stock_id)
        if not watch_stock:
            return _json_error('关注股票不存在', 404, 'not_found')

        payload = request.get_json(silent=True) or {}
        client_id = _build_client_id(payload)
        payload['client_id'] = client_id
        try:
            session = _service().create_entry_decision_session(watch_stock_id, payload)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)

        ok, response, status_code = _start_entry_decision_session(session['id'], client_id)
        if not ok:
            return response, status_code

        return _json_success(
            {
                'session_id': session['id'],
                'status': 'running',
                'task_mode': 'async',
                'client_id': client_id,
                'entry_decision_context': {
                    'watch_stock_id': watch_stock_id,
                    'stock_code': watch_stock.get('stock_code', ''),
                    'stock_name': watch_stock.get('stock_name', ''),
                    'market': watch_stock.get('market', ''),
                    'trade_date': session.get('trade_date', ''),
                    'analysis_depth': (session.get('request_json') or {}).get('analysis_depth') or 'standard',
                    'session_status': session.get('status', 'running'),
                    'pending_save_fields': {
                        'current_stage': watch_stock.get('current_stage', ''),
                        'current_price_zone': watch_stock.get('current_price_zone', ''),
                        'suggested_action': watch_stock.get('suggested_action', ''),
                        'last_conclusion_summary': watch_stock.get('last_conclusion_summary', ''),
                    },
                    'generated_summary_fields': _build_entry_decision_summary_fields({'data': {}}, session.get('trade_date', '')),
                },
            },
            '进场决策任务已启动',
        )

    @app.route(f'{ENTRY_DECISION_SESSIONS_API}/<session_id>', methods=['GET'])
    def get_entry_decision_session_api(session_id: str):
        session = _service().get_entry_decision_session(session_id)
        if not session:
            return _json_error('进场决策会话不存在', 404, 'not_found')
        return _json_success(session)

    @app.route(f'{ENTRY_DECISION_SESSIONS_API}/<session_id>/resume', methods=['POST'])
    def resume_entry_decision_session_api(session_id: str):
        payload = request.get_json(silent=True) or {}
        client_id = _build_client_id(payload)
        payload['client_id'] = client_id
        try:
            session = _service().resume_entry_decision_session(session_id, payload)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)

        ok, response, status_code = _start_entry_decision_session(session_id, client_id)
        if not ok:
            return response, status_code

        return _json_success(
            {
                'session_id': session_id,
                'status': 'running',
                'task_mode': 'async',
                'client_id': client_id,
                'missing_fields': [],
            },
            '进场决策任务已继续执行',
        )

    @app.route(f'{API_PREFIX}/<watch_stock_id>/trade-plan-analysis/run', methods=['POST'])
    def run_trade_plan_analysis_api(watch_stock_id: str):
        watch_stock = _service().get_watch_stock(watch_stock_id)
        if not watch_stock:
            return _json_error('关注股票不存在', 404, 'not_found')

        payload = request.get_json(silent=True) or {}
        trade_date = (payload.get('trade_date') or '').strip()
        plan_type = (payload.get('plan_type') or '三笔计划').strip() or '三笔计划'
        risk_preference = (payload.get('risk_preference') or '中高风险').strip() or '中高风险'
        analysis_depth = (payload.get('analysis_depth') or 'standard').strip() or 'standard'
        client_id = (payload.get('client_id') or '').strip() or None

        stock_ai_payload = build_stock_ai_payload(
            stock_code=watch_stock.get('stock_code', ''),
            market=watch_stock.get('market', 'SH'),
            client_id=client_id,
            trade_date=trade_date,
            analysis_depth=analysis_depth,
        )
        response = start_stock_ai_analysis(_context(), stock_ai_payload)

        response_obj, status_code = response if isinstance(response, tuple) else (response, 200)
        body = response_obj.get_json()
        if status_code >= 400 or not body.get('success'):
            return response

        body['trade_plan_analysis_context'] = {
            'watch_stock_id': watch_stock_id,
            'stock_code': watch_stock.get('stock_code', ''),
            'stock_name': watch_stock.get('stock_name', ''),
            'market': watch_stock.get('market', ''),
            'trade_date': trade_date,
            'plan_type': plan_type,
            'risk_preference': risk_preference,
            'analysis_depth': analysis_depth,
        }
        return jsonify(body), status_code

    @app.route(ENTRY_DECISION_RECORDS_API, methods=['POST'])
    def create_entry_decision_record_api():
        payload = request.get_json(silent=True) or {}
        try:
            created = _service().save_entry_decision_record(payload)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)
        return _json_success(created, '进场决策记录保存成功')

    @app.route(ENTRY_DECISION_RECORDS_API, methods=['GET'])
    def list_entry_decision_records_api():
        watch_stock_id = (request.args.get('watch_stock_id') or '').strip()
        if not watch_stock_id:
            return _json_error('缺少 watch_stock_id', 400, 'bad_request')
        limit = request.args.get('limit', 10)
        return _json_success(_service().list_entry_decision_records(watch_stock_id, limit=int(limit or 10)))

    @app.route(f'{ENTRY_DECISION_RECORDS_API}/<record_id>', methods=['GET'])
    def get_entry_decision_record_api(record_id: str):
        record = _service().get_entry_decision_record(record_id)
        if not record:
            return _json_error('进场决策记录不存在', 404, 'not_found')
        return _json_success(record)

    @app.route(TRADE_PLAN_RECORDS_API, methods=['POST'])
    def create_trade_plan_analysis_record_api():
        payload = request.get_json(silent=True) or {}
        try:
            created = _service().save_trade_plan_analysis_record(payload)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)
        return _json_success(created, '持仓计划分析记录保存成功')

    @app.route(TRADE_PLAN_RECORDS_API, methods=['GET'])
    def list_trade_plan_analysis_records_api():
        watch_stock_id = (request.args.get('watch_stock_id') or '').strip()
        if not watch_stock_id:
            return _json_error('缺少 watch_stock_id', 400, 'bad_request')
        limit = request.args.get('limit', 10)
        return _json_success(_service().list_trade_plan_analysis_records(watch_stock_id, limit=int(limit or 10)))

    @app.route(f'{TRADE_PLAN_RECORDS_API}/<record_id>', methods=['GET'])
    def get_trade_plan_analysis_record_api(record_id: str):
        record = _service().get_trade_plan_analysis_record(record_id)
        if not record:
            return _json_error('计划分析记录不存在', 404, 'not_found')
        return _json_success(record)
