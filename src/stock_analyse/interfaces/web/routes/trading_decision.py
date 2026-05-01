"""交易决策 Web 路由层。

负责承接进场决策、持仓计划、买卖决策和持仓复盘等页面请求与异步运行请求。
"""

from __future__ import annotations

import logging
from datetime import datetime
from uuid import uuid4

from flask import current_app, jsonify, render_template, request

from stock_analyse.application.orchestrators.entry_decision_orchestrator import EntryDecisionOrchestrator
from stock_analyse.application.orchestrators.holding_review_orchestrator import HoldingReviewOrchestrator
from stock_analyse.application.orchestrators.position_decision_orchestrator import PositionDecisionOrchestrator
from stock_analyse.application.orchestrators.trade_plan_analysis_orchestrator import TradePlanAnalysisOrchestrator
from stock_analyse.interfaces.web.streaming.streaming_analyzer import StreamingAnalyzer
from stock_analyse.interfaces.web.services.trading_decision_service import TradingDecisionService


API_PREFIX = '/api/trading-decision/watch-stocks'
HOLDING_STOCKS_API = '/api/trading-decision/holding-stocks'
TRADE_PLAN_RECORDS_API = '/api/trading-decision/trade-plan-analysis-records'
POSITION_DECISION_RECORDS_API = '/api/trading-decision/position-decision-records'
HOLDING_REVIEW_RECORDS_API = '/api/trading-decision/holding-review-records'
STOCK_ANALYSIS_RECORDS_API = '/api/trading-decision/stock-analysis-records'
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


def _build_trade_plan_analysis_orchestrator() -> TradePlanAnalysisOrchestrator:
    return TradePlanAnalysisOrchestrator()


def _build_position_decision_orchestrator() -> PositionDecisionOrchestrator:
    return PositionDecisionOrchestrator()


def _build_holding_review_orchestrator() -> HoldingReviewOrchestrator:
    return HoldingReviewOrchestrator()


def _build_client_id(payload: dict, prefix: str = 'entry_decision') -> str:
    provided = (payload.get('client_id') or '').strip()
    return provided or f'{prefix}_{uuid4().hex[:12]}'


def _update_analysis_task_state(context, lock_name: str, **updates) -> None:
    with context.task_lock:
        task = context.analysis_tasks.get(lock_name)
        if task is not None:
            task.update(updates)


def _run_trade_plan_analysis_task(watch_stock_id: str, client_id: str, context, service, trade_plan_context: dict) -> None:
    logger.info(f'持仓计划任务开始执行: watch_stock_id={watch_stock_id}, client_id={client_id}')
    streamer = StreamingAnalyzer(client_id, context.sse_manager)
    lock_name = f'trade_plan_{watch_stock_id}'
    try:
        settings = context.settings.ai
        orchestrator = _build_trade_plan_analysis_orchestrator()
        watch_stock = trade_plan_context.get('watch_stock') or {}
        streamer.send_log(f"🚀 开始持仓计划分析: {watch_stock.get('stock_code', '')}", 'header')
        streamer.send_progress('singleProgress', 5, '正在初始化持仓计划分析...')
        result = orchestrator.run(
            context=trade_plan_context,
            llm_provider=settings.platform,
            llm_model=settings.model_name,
            api_code=settings.api_key,
            system_prompt=settings.system_prompt,
            callbacks={
                'send_log': streamer.send_log,
                'send_progress': streamer.send_progress,
            },
        )
        service.save_result_markdown_cache('trade_plan', result, watch_stock)
        streamer.send_final_result(result)
        streamer.send_progress('singleProgress', 100, '持仓计划分析完成')
        streamer.send_completion(f"持仓计划分析完成: {watch_stock.get('stock_code', '')}")
        streamer.send_log(f"🚀 持仓计划分析完成: {watch_stock.get('stock_code', '')}", 'header')
    except Exception as exc:
        logger.exception('持仓计划分析失败: %s', exc)
        streamer.send_progress('singleProgress', 100, '持仓计划分析失败')
        streamer.send_error(f'持仓计划分析失败: {exc}')
        streamer.send_completion(f'持仓计划分析失败: {exc}')
    finally:
        with context.task_lock:
            context.analysis_tasks.pop(lock_name, None)


def _run_position_decision_task(holding_stock_id: str, client_id: str, context, service, position_context: dict) -> None:
    logger.info(f'买卖决策任务开始执行: holding_stock_id={holding_stock_id}, client_id={client_id}')
    streamer = StreamingAnalyzer(client_id, context.sse_manager)
    lock_name = f'position_decision_{holding_stock_id}'
    try:
        settings = context.settings.ai
        orchestrator = _build_position_decision_orchestrator()
        holding_stock = position_context.get('holding_stock') or {}
        _update_analysis_task_state(context, lock_name, status='running', started_at=datetime.now(), error='')
        streamer.send_log(f"🚀 开始买卖决策分析: {holding_stock.get('stock_code', '')}", 'header')
        streamer.send_progress('singleProgress', 5, '正在初始化买卖决策分析...')
        result = orchestrator.run(
            context=position_context,
            llm_provider=settings.platform,
            llm_model=settings.model_name,
            api_code=settings.api_key,
            system_prompt=settings.system_prompt,
            callbacks={
                'send_log': streamer.send_log,
                'send_progress': streamer.send_progress,
            },
        )
        try:
            service.save_position_decision_record(
                {
                    'holding_stock_id': holding_stock_id,
                    'trade_date': (position_context.get('request') or {}).get('trade_date') or '',
                    'analysis_depth': (position_context.get('request') or {}).get('analysis_depth') or 'standard',
                    'raw_result': result,
                }
            )
        except Exception as save_exc:
            logger.exception('自动保存买卖决策历史记录失败: %s', save_exc)
        _update_analysis_task_state(context, lock_name, status='completed', completed_at=datetime.now(), error='')
        streamer.send_final_result(result)
        streamer.send_progress('singleProgress', 100, '买卖决策分析完成')
        streamer.send_completion(f"买卖决策分析完成: {holding_stock.get('stock_code', '')}")
        streamer.send_log(f"🚀 买卖决策分析完成: {holding_stock.get('stock_code', '')}", 'header')
    except Exception as exc:
        logger.exception('买卖决策分析失败: %s', exc)
        _update_analysis_task_state(context, lock_name, status='failed', completed_at=datetime.now(), error=str(exc))
        streamer.send_progress('singleProgress', 100, '买卖决策分析失败')
        streamer.send_error(f'买卖决策分析失败: {exc}')
        streamer.send_completion(f'买卖决策分析失败: {exc}')
    finally:
        with context.task_lock:
            context.analysis_tasks.pop(lock_name, None)


def _run_holding_review_task(holding_stock_id: str, client_id: str, context, service, holding_review_context: dict) -> None:
    logger.info(f'持仓复盘任务开始执行: holding_stock_id={holding_stock_id}, client_id={client_id}')
    streamer = StreamingAnalyzer(client_id, context.sse_manager)
    lock_name = f'holding_review_{holding_stock_id}'
    try:
        settings = context.settings.ai
        orchestrator = _build_holding_review_orchestrator()
        holding_stock = holding_review_context.get('holding_stock') or {}
        _update_analysis_task_state(context, lock_name, status='running', started_at=datetime.now(), error='')
        streamer.send_log(f"🚀 开始持仓复盘分析: {holding_stock.get('stock_code', '')}", 'header')
        streamer.send_progress('singleProgress', 5, '正在初始化持仓复盘分析...')
        result = orchestrator.run(
            context=holding_review_context,
            llm_provider=settings.platform,
            llm_model=settings.model_name,
            api_code=settings.api_key,
            system_prompt=settings.system_prompt,
            callbacks={
                'send_log': streamer.send_log,
                'send_progress': streamer.send_progress,
            },
        )
        try:
            service.save_holding_review_record(
                {
                    'holding_stock_id': holding_stock_id,
                    'trade_date': (holding_review_context.get('request') or {}).get('trade_date') or '',
                    'review_type': (holding_review_context.get('request') or {}).get('review_type') or 'general',
                    'period_key': (holding_review_context.get('request') or {}).get('period_key') or '',
                    'analysis_depth': (holding_review_context.get('request') or {}).get('analysis_depth') or 'standard',
                    'raw_result': result,
                }
            )
        except Exception as save_exc:
            logger.exception('自动保存持仓复盘历史记录失败: %s', save_exc)
        _update_analysis_task_state(context, lock_name, status='completed', completed_at=datetime.now(), error='')
        streamer.send_final_result(result)
        streamer.send_progress('singleProgress', 100, '持仓复盘分析完成')
        streamer.send_completion(f"持仓复盘分析完成: {holding_stock.get('stock_code', '')}")
        streamer.send_log(f"🚀 持仓复盘分析完成: {holding_stock.get('stock_code', '')}", 'header')
    except Exception as exc:
        logger.exception('持仓复盘分析失败: %s', exc)
        _update_analysis_task_state(context, lock_name, status='failed', completed_at=datetime.now(), error=str(exc))
        streamer.send_progress('singleProgress', 100, '持仓复盘分析失败')
        streamer.send_error(f'持仓复盘分析失败: {exc}')
        streamer.send_completion(f'持仓复盘分析失败: {exc}')
    finally:
        with context.task_lock:
            context.analysis_tasks.pop(lock_name, None)


def _start_position_decision_task(holding_stock_id: str, client_id: str, position_context: dict):
    context = _context()
    lock_name = f'position_decision_{holding_stock_id}'
    with context.task_lock:
        if lock_name in context.analysis_tasks:
            return False, jsonify({'success': False, 'error': '当前买卖决策任务正在执行，请稍候'}), 429
        context.analysis_tasks[lock_name] = {
            'start_time': datetime.now(),
            'status': 'analyzing',
            'client_id': client_id,
        }

    try:
        service = _service()
        future = context.executor.submit(_run_position_decision_task, holding_stock_id, client_id, context, service, position_context)

        def callback(fut):
            try:
                fut.result()
                with context.task_lock:
                    task_state = dict(context.analysis_tasks.get(lock_name) or {})
                status = task_state.get('status')
                error = task_state.get('error') or ''
                if status == 'failed':
                    logger.error(f'买卖决策任务失败: holding_stock_id={holding_stock_id}, error={error}')
                else:
                    logger.info(f'买卖决策任务完成: holding_stock_id={holding_stock_id}')
            except Exception as exc:
                logger.error(f'买卖决策任务异常: holding_stock_id={holding_stock_id}, error={exc}')
                with context.task_lock:
                    context.analysis_tasks.pop(lock_name, None)

        future.add_done_callback(callback)
        return True, None, 200
    except Exception as exc:
        logger.exception('启动买卖决策任务失败: %s', exc)
        with context.task_lock:
            context.analysis_tasks.pop(lock_name, None)
        return False, jsonify({'success': False, 'error': str(exc)}), 500


def _start_holding_review_task(holding_stock_id: str, client_id: str, holding_review_context: dict):
    context = _context()
    lock_name = f'holding_review_{holding_stock_id}'
    with context.task_lock:
        if lock_name in context.analysis_tasks:
            return False, jsonify({'success': False, 'error': '当前持仓复盘任务正在执行，请稍候'}), 429
        context.analysis_tasks[lock_name] = {
            'start_time': datetime.now(),
            'status': 'analyzing',
            'client_id': client_id,
        }

    try:
        service = _service()
        future = context.executor.submit(_run_holding_review_task, holding_stock_id, client_id, context, service, holding_review_context)

        def callback(fut):
            try:
                fut.result()
                with context.task_lock:
                    task_state = dict(context.analysis_tasks.get(lock_name) or {})
                status = task_state.get('status')
                error = task_state.get('error') or ''
                if status == 'failed':
                    logger.error(f'持仓复盘任务失败: holding_stock_id={holding_stock_id}, error={error}')
                else:
                    logger.info(f'持仓复盘任务完成: holding_stock_id={holding_stock_id}')
            except Exception as exc:
                logger.error(f'持仓复盘任务异常: holding_stock_id={holding_stock_id}, error={exc}')
                with context.task_lock:
                    context.analysis_tasks.pop(lock_name, None)

        future.add_done_callback(callback)
        return True, None, 200
    except Exception as exc:
        logger.exception('启动持仓复盘任务失败: %s', exc)
        with context.task_lock:
            context.analysis_tasks.pop(lock_name, None)
        return False, jsonify({'success': False, 'error': str(exc)}), 500


def _start_trade_plan_analysis_task(watch_stock_id: str, client_id: str, trade_plan_context: dict):
    context = _context()
    lock_name = f'trade_plan_{watch_stock_id}'
    with context.task_lock:
        if lock_name in context.analysis_tasks:
            return False, jsonify({'success': False, 'error': '当前持仓计划任务正在执行，请稍候'}), 429
        context.analysis_tasks[lock_name] = {
            'start_time': datetime.now(),
            'status': 'analyzing',
            'client_id': client_id,
        }

    try:
        service = _service()
        future = context.executor.submit(_run_trade_plan_analysis_task, watch_stock_id, client_id, context, service, trade_plan_context)

        def callback(fut):
            try:
                fut.result()
                logger.info(f'持仓计划任务完成: watch_stock_id={watch_stock_id}')
            except Exception as exc:
                logger.error(f'持仓计划任务异常: watch_stock_id={watch_stock_id}, error={exc}')
                with context.task_lock:
                    context.analysis_tasks.pop(lock_name, None)

        future.add_done_callback(callback)
        return True, None, 200
    except Exception as exc:
        logger.exception('启动持仓计划任务失败: %s', exc)
        with context.task_lock:
            context.analysis_tasks.pop(lock_name, None)
        return False, jsonify({'success': False, 'error': str(exc)}), 500


def _run_entry_decision_session_task(session_id: str, client_id: str, context, service) -> None:
    logger.info(f'进场决策任务开始执行: session_id={session_id}, client_id={client_id}')
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
        service.annotate_result_source(updated_state.final_result, result_source='live')
        service.save_result_markdown_cache('entry_decision', updated_state.final_result, state.watch_stock)
        try:
            service.save_entry_decision_record(
                {
                    'watch_stock_id': state.watch_stock_id,
                    'session_id': state.session_id,
                    'trade_date': (updated_state.request or {}).get('trade_date') or '',
                    'raw_result': updated_state.final_result,
                }
            )
        except Exception as save_exc:
            logger.exception('自动保存进场决策历史记录失败: %s', save_exc)
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
        # 预先获取服务引用，避免在异步线程中访问 current_app
        service = _service()
        logger.info(f'启动进场决策任务: session_id={session_id}, client_id={client_id}')
        future = context.executor.submit(_run_entry_decision_session_task, session_id, client_id, context, service)

        # 添加回调函数处理任务结果
        def callback(fut):
            try:
                result = fut.result()
                logger.info(f'进场决策任务完成: session_id={session_id}')
            except Exception as exc:
                logger.error(f'进场决策任务异常: session_id={session_id}, error={exc}')
                with context.task_lock:
                    context.analysis_tasks.pop(lock_name, None)

        future.add_done_callback(callback)

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

    @app.route('/holding-stocks', methods=['GET'])
    def holding_stocks_page():
        page_data = _service().build_holding_stocks_page_vm(request.args.to_dict())
        page_data['buy_form_context'] = None
        holding_stock_id = (request.args.get('holding_stock_id') or '').strip()
        watch_stock_id = (request.args.get('watch_stock_id') or '').strip()
        if holding_stock_id or watch_stock_id:
            try:
                page_data['buy_form_context'] = _service().build_holding_buy_form_data(
                    holding_stock_id=holding_stock_id,
                    watch_stock_id=watch_stock_id,
                )
            except ValueError:
                page_data['buy_form_context'] = None
        return render_template('holding_stocks.html', **page_data)

    @app.route('/history-center', methods=['GET'])
    def history_center_page():
        page_data = _service().build_history_center_page_data(request.args.to_dict())
        return render_template('history_center.html', **page_data)

    @app.route('/entry-decision', methods=['GET'])
    def entry_decision_page():
        watch_stock_id = (request.args.get('watch_stock_id') or '').strip()
        record_id = (request.args.get('record_id') or '').strip() or None
        if not watch_stock_id:
            return _json_error('缺少 watch_stock_id', 400, 'bad_request')
        try:
            page_data = _service().build_entry_decision_page_data(watch_stock_id, record_id)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)
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

    @app.route('/holding-review', methods=['GET'])
    def holding_review_page():
        holding_stock_id = (request.args.get('holding_stock_id') or '').strip()
        record_id = (request.args.get('record_id') or '').strip() or None
        if not holding_stock_id:
            return _json_error('缺少 holding_stock_id', 400, 'bad_request')
        try:
            page_data = _service().build_holding_review_page_data(holding_stock_id, record_id)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)
        return render_template('holding_review.html', **page_data)

    @app.route('/position-decision', methods=['GET'])
    def position_decision_page():
        holding_stock_id = (request.args.get('holding_stock_id') or '').strip()
        record_id = (request.args.get('record_id') or '').strip() or None
        if not holding_stock_id:
            return _json_error('缺少 holding_stock_id', 400, 'bad_request')
        try:
            page_data = _service().build_position_decision_page_data(holding_stock_id, record_id)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)
        return render_template('position_decision.html', **page_data)

    @app.route(f'{API_PREFIX}/stock-search', methods=['GET'])
    def search_stock_candidates_api():
        query = request.args.get('query', '')
        market = request.args.get('market', '')
        limit = request.args.get('limit', 20)
        return _json_success(_service().search_stock_candidates(query=query, market=market, limit=int(limit or 20)))

    @app.route(f'{HOLDING_STOCKS_API}/stock-search', methods=['GET'])
    def search_holding_stock_candidates_api():
        query = request.args.get('query', '')
        market = request.args.get('market', '')
        limit = request.args.get('limit', 20)
        return _json_success(_service().search_stock_candidates(query=query, market=market, limit=int(limit or 20)))

    @app.route(HOLDING_STOCKS_API, methods=['GET'])
    def list_holding_stocks_api():
        return _json_success(_service().list_holding_stocks(request.args.to_dict()))

    @app.route(HOLDING_STOCKS_API, methods=['POST'])
    def create_holding_stock_api():
        payload = request.get_json(silent=True) or {}
        try:
            created = _service().create_holding_stock_buy(payload)
        except ValueError as error:
            return _json_error(str(error), 400, 'bad_request')
        return _json_success(created, '持仓创建成功')

    @app.route(f'{HOLDING_STOCKS_API}/<holding_stock_id>', methods=['GET'])
    def get_holding_stock_api(holding_stock_id: str):
        holding_stock = _service().get_holding_stock(holding_stock_id)
        if not holding_stock:
            return _json_error('持仓不存在', 404, 'not_found')
        return _json_success(holding_stock)

    @app.route(f'{HOLDING_STOCKS_API}/<holding_stock_id>/buy', methods=['PUT'])
    def append_holding_stock_buy_api(holding_stock_id: str):
        payload = request.get_json(silent=True) or {}
        try:
            updated = _service().append_holding_stock_buy(holding_stock_id, payload)
        except ValueError as error:
            return _json_error(str(error), 400, 'bad_request')
        if not updated:
            return _json_error('持仓不存在', 404, 'not_found')
        return _json_success(updated, '持仓买入补录成功')

    @app.route(f'{HOLDING_STOCKS_API}/from-watch/<watch_stock_id>/buy', methods=['POST'])
    def convert_watch_stock_to_holding_buy_api(watch_stock_id: str):
        payload = request.get_json(silent=True) or {}
        try:
            converted = _service().convert_watch_stock_to_holding_buy(watch_stock_id, payload)
        except ValueError as error:
            message = str(error)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)
        return _json_success(converted, '已从关注股票转入持仓')

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
        service = _service()
        trade_date = (payload.get('trade_date') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        cached_result = service.build_cached_entry_decision_result(watch_stock=watch_stock, trade_date=trade_date)
        if cached_result:
            try:
                service.save_entry_decision_record(
                    {
                        'watch_stock_id': watch_stock_id,
                        'trade_date': trade_date,
                        'raw_result': cached_result,
                    }
                )
            except Exception:
                logger.exception('复用进场决策缓存时保存历史记录失败: %s', watch_stock_id)
            return _json_success(
                service.build_entry_decision_cached_response_payload(
                    watch_stock=watch_stock,
                    result=cached_result,
                    trade_date=trade_date,
                    client_id=client_id,
                ),
                '已复用当天进场决策缓存',
            )
        try:
            session = service.create_entry_decision_session(watch_stock_id, payload)
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
                'result_source': 'live',
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
        payload = request.get_json(silent=True) or {}
        client_id = _build_client_id(payload, prefix='trade_plan')
        payload['client_id'] = client_id
        try:
            trade_plan_context = _service().build_trade_plan_analysis_context(watch_stock_id, payload)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)

        ok, response, status_code = _start_trade_plan_analysis_task(watch_stock_id, client_id, trade_plan_context)
        if not ok:
            return response, status_code

        return _json_success(
            {
                'status': 'running',
                'task_mode': 'async',
                'client_id': client_id,
                'trade_plan_analysis_context': _service().build_trade_plan_response_context(trade_plan_context),
            },
            '持仓计划分析任务已启动',
        )

    @app.route(f'{HOLDING_STOCKS_API}/<holding_stock_id>/reviews/run', methods=['POST'])
    def run_holding_review_api(holding_stock_id: str):
        payload = request.get_json(silent=True) or {}
        client_id = _build_client_id(payload, prefix='holding_review')
        payload['client_id'] = client_id
        try:
            holding_review_context = _service().build_holding_review_context(holding_stock_id, payload)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)

        ok, response, status_code = _start_holding_review_task(holding_stock_id, client_id, holding_review_context)
        if not ok:
            return response, status_code

        request_payload = holding_review_context.get('request') or {}
        holding_stock = holding_review_context.get('holding_stock') or {}
        watch_stock = holding_review_context.get('watch_stock') or {}
        return _json_success(
            {
                'status': 'running',
                'task_mode': 'async',
                'client_id': client_id,
                'holding_review_context': {
                    'holding_stock_id': holding_stock_id,
                    'watch_stock_id': watch_stock.get('id', ''),
                    'stock_code': holding_stock.get('stock_code', ''),
                    'stock_name': holding_stock.get('stock_name', ''),
                    'market': holding_stock.get('market', ''),
                    'trade_date': request_payload.get('trade_date', ''),
                    'review_type': request_payload.get('review_type', 'general'),
                    'period_key': request_payload.get('period_key', ''),
                    'analysis_depth': request_payload.get('analysis_depth', 'standard'),
                    'role': '交易专家',
                    'data_sources': ['trade_history_context', 'entry_context', 'reanalysis_context', 'position_decision_context', 'financial_context', 'market_context'],
                },
            },
            '持仓复盘任务已启动',
        )

    @app.route(f'{HOLDING_STOCKS_API}/<holding_stock_id>/position-decisions/run', methods=['POST'])
    def run_position_decision_api(holding_stock_id: str):
        payload = request.get_json(silent=True) or {}
        client_id = _build_client_id(payload, prefix='position_decision')
        payload['client_id'] = client_id
        try:
            position_context = _service().build_position_decision_context(holding_stock_id, payload)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)

        ok, response, status_code = _start_position_decision_task(holding_stock_id, client_id, position_context)
        if not ok:
            return response, status_code

        request_payload = position_context.get('request') or {}
        holding_stock = position_context.get('holding_stock') or {}
        watch_stock = position_context.get('watch_stock') or {}
        return _json_success(
            {
                'status': 'running',
                'task_mode': 'async',
                'client_id': client_id,
                'position_decision_context': {
                    'holding_stock_id': holding_stock_id,
                    'watch_stock_id': watch_stock.get('id', ''),
                    'stock_code': holding_stock.get('stock_code', ''),
                    'stock_name': holding_stock.get('stock_name', ''),
                    'market': holding_stock.get('market', ''),
                    'trade_date': request_payload.get('trade_date', ''),
                    'analysis_depth': request_payload.get('analysis_depth', 'standard'),
                    'role': '股票分析师',
                    'data_sources': ['financial_context', 'trade_history_context', 'holding_plan_context'],
                },
            },
            '买卖决策任务已启动',
        )

    @app.route(HOLDING_REVIEW_RECORDS_API, methods=['POST'])
    def create_holding_review_record_api():
        payload = request.get_json(silent=True) or {}
        try:
            created = _service().save_holding_review_record(payload)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)
        return _json_success(created, '持仓复盘记录保存成功')

    @app.route(HOLDING_REVIEW_RECORDS_API, methods=['GET'])
    def list_holding_review_records_api():
        holding_stock_id = (request.args.get('holding_stock_id') or '').strip()
        if not holding_stock_id:
            return _json_error('缺少 holding_stock_id', 400, 'bad_request')
        limit = request.args.get('limit', 10)
        return _json_success(_service().list_holding_review_records(holding_stock_id, limit=int(limit or 10)))

    @app.route(f'{HOLDING_REVIEW_RECORDS_API}/<record_id>', methods=['GET'])
    def get_holding_review_record_api(record_id: str):
        record = _service().get_holding_review_record(record_id)
        if not record:
            return _json_error('持仓复盘记录不存在', 404, 'not_found')
        return _json_success(record)

    @app.route(POSITION_DECISION_RECORDS_API, methods=['POST'])
    def create_position_decision_record_api():
        payload = request.get_json(silent=True) or {}
        try:
            created = _service().save_position_decision_record(payload)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)
        return _json_success(created, '买卖决策记录保存成功')

    @app.route(POSITION_DECISION_RECORDS_API, methods=['GET'])
    def list_position_decision_records_api():
        holding_stock_id = (request.args.get('holding_stock_id') or '').strip()
        if not holding_stock_id:
            return _json_error('缺少 holding_stock_id', 400, 'bad_request')
        limit = request.args.get('limit', 10)
        return _json_success(_service().list_position_decision_records(holding_stock_id, limit=int(limit or 10)))

    @app.route(f'{POSITION_DECISION_RECORDS_API}/<record_id>', methods=['GET'])
    def get_position_decision_record_api(record_id: str):
        record = _service().get_position_decision_record(record_id)
        if not record:
            return _json_error('买卖决策记录不存在', 404, 'not_found')
        return _json_success(record)


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

    @app.route(STOCK_ANALYSIS_RECORDS_API, methods=['POST'])
    def create_stock_analysis_record_api():
        payload = request.get_json(silent=True) or {}
        try:
            created = _service().save_stock_analysis_record(payload)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return _json_error(message, status_code, code)
        return _json_success(created, '股票分析记录保存成功')

    @app.route(STOCK_ANALYSIS_RECORDS_API, methods=['GET'])
    def list_stock_analysis_records_api():
        watch_stock_id = (request.args.get('watch_stock_id') or '').strip()
        holding_stock_id = (request.args.get('holding_stock_id') or '').strip()
        if not watch_stock_id and not holding_stock_id:
            return _json_error('缺少 watch_stock_id 或 holding_stock_id', 400, 'bad_request')
        limit = request.args.get('limit', 10)
        return _json_success(
            _service().list_stock_analysis_records(
                watch_stock_id=watch_stock_id,
                holding_stock_id=holding_stock_id,
                limit=int(limit or 10),
            )
        )

    @app.route(f'{STOCK_ANALYSIS_RECORDS_API}/<record_id>', methods=['GET'])
    def get_stock_analysis_record_api(record_id: str):
        record = _service().get_stock_analysis_record(record_id)
        if not record:
            return _json_error('股票分析记录不存在', 404, 'not_found')
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