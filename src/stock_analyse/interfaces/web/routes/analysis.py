from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from queue import Empty, Queue

from flask import Response, current_app, jsonify, render_template, request

import stock_analyse.interfaces.web.services.stock_indicator_html_service as stockIndicatorHtml
import stock_analyse.application.services.quantitative_analysis_service as stockIndicatorQuantitative
from stock_analyse.interfaces.web.streaming.streaming_analyzer import StreamingAnalyzer

logger = logging.getLogger(__name__)


def _context():
    return current_app.extensions['stock_analyse.context']


def _require_scalar_string(value, field_name: str, default: str | None = None) -> str:
    if value is None:
        return (default or '').strip()
    if isinstance(value, (dict, list, tuple, set)):
        raise ValueError(f'{field_name} 必须是字符串')
    return str(value).strip()


def _normalize_analysis_market(value) -> str:
    market = _require_scalar_string(value, 'market', 'SH')
    lowered = market.lower()
    if lowered in {'cn', 'sh', 'sz', 'a股'}:
        return 'SH'
    if lowered in {'h', 'hk', '港股'}:
        return 'H'
    if lowered in {'usa', 'us', '美股'}:
        return 'usa'
    if lowered == 'zq':
        return 'zq'
    return market


def _parse_analyze_stock_ai_payload():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        raise ValueError('请求体必须是 JSON 对象')
    return _normalize_analyze_stock_ai_payload(data)


def _normalize_analyze_stock_ai_payload(data: dict):
    stock_code = _require_scalar_string(data.get('stock_code'), 'stock_code')
    if not stock_code:
        raise ValueError('stock_code 不能为空')

    market = _normalize_analysis_market(data.get('market', 'SH'))
    client_id = _require_scalar_string(data.get('client_id'), 'client_id') or None
    trade_date = _require_scalar_string(data.get('trade_date'), 'trade_date') or None
    analysis_depth = _require_scalar_string(data.get('analysis_depth'), 'analysis_depth', 'standard') or 'standard'
    start_date = _require_scalar_string(data.get('start_date'), 'start_date') or None
    end_date = _require_scalar_string(data.get('end_date'), 'end_date') or None

    return {
        'stock_code': stock_code,
        'market': market,
        'client_id': client_id,
        'trade_date': trade_date,
        'analysis_depth': analysis_depth,
        'start_date': start_date,
        'end_date': end_date,
    }


def _start_stock_ai_analysis_task(context, payload: dict):
    stock_code = payload['stock_code']
    market = payload['market']
    client_id = payload['client_id']
    trade_date = payload['trade_date']
    analysis_depth = payload['analysis_depth']
    start_date_str = payload['start_date'] or (datetime.now() - timedelta(days=100)).strftime('%Y-%m-%d')
    end_date_str = payload['end_date'] or datetime.now().strftime('%Y-%m-%d')
    lock_name = f'ai_{stock_code}'

    with context.task_lock:
        if lock_name in context.analysis_tasks:
            return jsonify({'success': False, 'error': f'股票 {stock_code} 正在AI分析中，请稍候'}), 429
        context.analysis_tasks[lock_name] = {
            'start_time': datetime.now(),
            'status': 'analyzing',
            'client_id': client_id,
        }

    try:
        def run_analysis():
            streamer = StreamingAnalyzer(client_id, context.sse_manager)
            try:
                streamer.send_log(f"🚀 开始AI个股分析: {stock_code}", 'header')
                streamer.send_progress('singleProgress', 5, '开始AI个股分析...')
                context.analyzer.streaming = streamer
                context.analyzer.stock_ai_analysis_process(stock_code, market, start_date_str, end_date_str, trade_date=trade_date, analysis_depth=analysis_depth)
                logger.info(f'AI个股分析完成: {stock_code}')
                streamer.send_log(f"🚀 AI个股分析完成: {stock_code}", 'header')
                streamer.send_progress('singleProgress', 100, 'AI个股分析完成...')
                streamer.send_completion(f'AI个股分析完成: {stock_code}')
            except Exception as exc:
                streamer.send_log(f"🚀 AI个股分析失败: {stock_code}", 'header')
                streamer.send_progress('singleProgress', 100, 'AI个股分析失败...')
                streamer.send_error(f'AI个股分析失败: {stock_code}, 错误: {exc}')
                streamer.send_completion(f'AI个股分析失败: {stock_code}, 错误: {exc}')
                logger.error(f'AI个股分析失败: {stock_code}, 错误: {exc}')
            finally:
                with context.task_lock:
                    context.analysis_tasks.pop(lock_name, None)

        context.executor.submit(run_analysis)
        return jsonify({'success': True, 'data': '', 'message': f'股票 {stock_code} AI分析已启动', 'task_mode': 'async', 'client_id': client_id})
    except Exception as exc:
        logger.error(f'AI个股分析失败: {exc}')
        with context.task_lock:
            context.analysis_tasks.pop(lock_name, None)
        return jsonify({'success': False, 'error': str(exc)}), 500


def start_stock_ai_analysis_from_payload(payload: dict):
    return _start_stock_ai_analysis_task(_context(), _normalize_analyze_stock_ai_payload(payload))


def normalize_analysis_market_value(value) -> str:
    return _normalize_analysis_market(value)


def require_scalar_string_value(value, field_name: str, default: str | None = None) -> str:
    return _require_scalar_string(value, field_name, default)


def default_stock_ai_date_range() -> tuple[str, str]:
    return (
        (datetime.now() - timedelta(days=100)).strftime('%Y-%m-%d'),
        datetime.now().strftime('%Y-%m-%d'),
    )


def default_trade_date_value() -> str:
    return datetime.now().strftime('%Y-%m-%d')


def build_stock_ai_payload(
    *,
    stock_code: str,
    market: str,
    client_id: str | None,
    trade_date: str | None,
    analysis_depth: str | None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    return _normalize_analyze_stock_ai_payload(
        {
            'stock_code': stock_code,
            'market': market,
            'client_id': client_id,
            'trade_date': trade_date,
            'analysis_depth': analysis_depth or 'standard',
            'start_date': start_date,
            'end_date': end_date,
        }
    )


def start_stock_ai_analysis(context, payload: dict):
    return _start_stock_ai_analysis_task(context, payload)


def _get_stock_analysis_defaults(context):
    settings = context.settings
    ai_platform = settings.ai.platform
    ai_model = settings.ai.model_name
    api_code = settings.ai.api_key
    system_prompt = settings.ai.system_prompt
    message_format = settings.ai.prompt_template
    current_date = datetime.now()
    current_date_str = current_date.strftime('%Y-%m-%d')
    previous_year = current_date - timedelta(days=100)
    previous_year_str = previous_year.strftime('%Y-%m-%d')
    all_strategies = [
        '均线策略', '布林带策略', '动量MACD策略', '突破策略', 'SAR策略', '均值回归策略',
        'RSI策略', 'KDJ策略', '威廉指标策略', 'ADX策略', '线性回归策略', 'K线形态策略', '神经网络多层感知回归策略',
    ]
    return {
        'start_date': previous_year_str,
        'end_date': current_date_str,
        'market': 'CN',
        'strategies': all_strategies,
        'ai_platform': ai_platform,
        'ai_model': ai_model,
        'api_code': api_code,
        'system_prompt': system_prompt,
        'message_format': message_format,
    }



def _build_stock_analysis_result_context(context, form_data):
    defaults = _get_stock_analysis_defaults(context)
    stock_code = form_data.get('stock_code')
    start_date_str = form_data.get('start_date')
    end_date_str = form_data.get('end_date')
    market = form_data.get('market')
    selected_strategies = form_data.getlist('strategies')
    ai_platform = form_data.get('ai_platform', defaults['ai_platform'])
    ai_model = form_data.get('ai_model', defaults['ai_model'])
    api_code = form_data.get('api_code', defaults['api_code'])
    system_prompt = form_data.get('system_prompt', defaults['system_prompt'])
    message_format = form_data.get('message_format', defaults['message_format'])

    image_paths, strategies_selected, stock_summary, stock_analysis_result, annual_report_analysis, sentiment_analysis, sentiment_score = context.analyzer.get_stock_analysis(
        stock_code,
        market,
        start_date_str,
        end_date_str,
        selected_strategies,
        system_prompt,
        message_format,
        ai_platform,
        ai_model,
        api_code,
    )

    return {
        'image_paths': image_paths,
        'strategies': strategies_selected,
        'stock_summary': stock_summary,
        'fundamental_analysis': stock_analysis_result,
        'annual_report_analysis': annual_report_analysis,
        'sentiment_analysis': sentiment_analysis,
        'sentiment_score': sentiment_score,
        'stock_code': stock_code,
        'start_date': start_date_str,
        'end_date': end_date_str,
        'market': market,
        'selected_strategies': selected_strategies,
        'ai_platform': ai_platform,
        'ai_model': ai_model,
        'api_code': api_code,
        'system_prompt': system_prompt,
        'message_format': message_format,
    }



def register_analysis_routes(app):
    @app.route('/api/select_stock', methods=['GET', 'POST'])
    def select_stock():
        context = _context()
        data = request.json
        strategy_code = data.get('strategy', '1').strip()
        market = data.get('market', 'SH').strip()
        client_id = data.get('client_id')
        lock_name = f'{strategy_code}_{market}'
        with context.task_lock:
            if lock_name in context.analysis_tasks:
                return jsonify({'success': False, 'error': f'股票 {lock_name} 正在分析中，请稍候'}), 429
            context.analysis_tasks[lock_name] = {
                'start_time': datetime.now(),
                'status': 'analyzing',
                'client_id': client_id,
            }

        try:
            def run_analysis():
                streamer = StreamingAnalyzer(client_id, context.sse_manager)
                try:
                    streamer.send_log(f"🚀 开始筛选股票: {strategy_code}", 'header')
                    streamer.send_progress('singleProgress', 5, '开始筛选股票...')
                    context.analyzer.streaming = streamer
                    context.analyzer.stock_select_process(strategy_code, market)
                    logger.info(f'股票流式分析完成: {strategy_code}')
                    streamer.send_log(f"🚀 筛选股票完成: {strategy_code}", 'header')
                    streamer.send_progress('singleProgress', 100, '筛选股票完成...')
                    streamer.send_completion(f'筛选股票完成: {strategy_code}')
                except Exception as exc:
                    streamer.send_log(f"🚀 筛选股票出错: {strategy_code}", 'header')
                    streamer.send_progress('singleProgress', 100, '筛选股票出错...')
                    streamer.send_error(f'筛选股票失败: {strategy_code}, 错误: {exc}')
                    streamer.send_completion(f'筛选股票失败: {strategy_code}, 错误: {exc}')
                    logger.error(f'筛选股票出错失败: {strategy_code}, 错误: {exc}')
                finally:
                    with context.task_lock:
                        context.analysis_tasks.pop(strategy_code, None)

            context.executor.submit(run_analysis)
            logger.info(f'股票分析完成: {lock_name}')
            return jsonify({'success': True, 'data': '', 'message': f'股票 {lock_name} 分析完成'})
        except Exception as exc:
            logger.error(f'股票分析失败: {exc}')
            with context.task_lock:
                context.analysis_tasks.pop(lock_name, None)
            return jsonify({'success': False, 'error': str(exc)}), 500
        finally:
            with context.task_lock:
                context.analysis_tasks.pop(lock_name, None)

    @app.route('/api/analyze_stock', methods=['GET', 'POST'])
    def analyze_stock():
        context = _context()
        data = request.json
        stock_code = data.get('stock_code', '').strip()
        market = data.get('market', 'SH').strip()
        client_id = data.get('client_id')
        start_date_str = (datetime.now() - timedelta(days=100)).strftime('%Y-%m-%d')
        end_date_str = datetime.now().strftime('%Y-%m-%d')

        with context.task_lock:
            if stock_code in context.analysis_tasks:
                return jsonify({'success': False, 'error': f'股票 {stock_code} 正在分析中，请稍候'}), 429
            context.analysis_tasks[stock_code] = {
                'start_time': datetime.now(),
                'status': 'analyzing',
                'client_id': client_id,
            }

        try:
            def run_analysis():
                streamer = StreamingAnalyzer(client_id, context.sse_manager)
                try:
                    streamer.send_log(f"🚀 开始流式分析股票: {stock_code}", 'header')
                    streamer.send_progress('singleProgress', 5, '正在获取股票基本信息...')
                    context.analyzer.streaming = streamer
                    context.analyzer.stock_analysis_process(stock_code, market, start_date_str, end_date_str)
                    logger.info(f'股票流式分析完成: {stock_code}')
                    streamer.send_log(f"🚀 股票流式分析完成: {stock_code}", 'header')
                    streamer.send_progress('singleProgress', 100, '股票流式分析完成...')
                    streamer.send_completion(f'股票流式分析完成: {stock_code}')
                except Exception as exc:
                    streamer.send_log(f"🚀 股票流式戳无完成: {stock_code}", 'header')
                    streamer.send_progress('singleProgress', 100, '股票流式分析戳无...')
                    streamer.send_error(f'股票流式分析失败: {stock_code}, 错误: {exc}')
                    streamer.send_completion(f'股票流式分析失败: {stock_code}, 错误: {exc}')
                    logger.error(f'股票流式分析失败: {stock_code}, 错误: {exc}')
                finally:
                    with context.task_lock:
                        context.analysis_tasks.pop(stock_code, None)

            context.executor.submit(run_analysis)
            logger.info(f'股票分析完成: {stock_code}')
            return jsonify({'success': True, 'data': '', 'message': f'股票 {stock_code} 分析完成'})
        except Exception as exc:
            logger.error(f'股票分析失败: {exc}')
            with context.task_lock:
                context.analysis_tasks.pop(stock_code, None)
            return jsonify({'success': False, 'error': str(exc)}), 500
        finally:
            with context.task_lock:
                context.analysis_tasks.pop(stock_code, None)

    @app.route('/api/analyze_stock_ai', methods=['GET', 'POST'])
    def analyze_stock_ai():
        try:
            payload = _parse_analyze_stock_ai_payload()
        except ValueError as exc:
            return jsonify({'success': False, 'error': str(exc)}), 400
        return start_stock_ai_analysis(_context(), payload)

    @app.route('/stock', methods=['GET', 'POST'])
    def stock_analysis():
        context = _context()

        if request.method == 'POST':
            result_context = _build_stock_analysis_result_context(context, request.form)
            return render_template('result.html', **result_context)

        return render_template('index.html', **_get_stock_analysis_defaults(context))

    @app.route('/single-stock-analysis-legacy', methods=['GET', 'POST'])
    def single_stock_analysis_legacy():
        context = _context()
        page_context = _get_stock_analysis_defaults(context)
        page_context['result_context'] = None

        if request.method == 'POST':
            result_context = _build_stock_analysis_result_context(context, request.form)
            page_context.update({
                'start_date': result_context['start_date'],
                'end_date': result_context['end_date'],
                'market': result_context['market'],
                'ai_platform': result_context['ai_platform'],
                'ai_model': result_context['ai_model'],
                'api_code': result_context['api_code'],
                'system_prompt': result_context['system_prompt'],
                'message_format': result_context['message_format'],
                'selected_strategies': result_context['selected_strategies'],
                'stock_code': result_context['stock_code'],
                'result_context': result_context,
            })
        else:
            page_context['selected_strategies'] = []
            page_context['stock_code'] = ''

        return render_template('single_stock_analysis_legacy.html', **page_context)


    @app.route('/stockSelector', methods=['GET', 'POST'])
    def stock_select():
        return render_template('stock.html')

    @app.route('/api/sse')
    def sse_stream():
        context = _context()
        client_id = request.args.get('client_id')
        if not client_id:
            return 'Missing client_id', 400

        def event_stream():
            client_queue = Queue()
            context.sse_manager.add_client(client_id, client_queue)
            try:
                yield f"data: {json.dumps({'event': 'connected', 'data': {'client_id': client_id}})}\n\n"
                while True:
                    try:
                        message = client_queue.get(timeout=30)
                        try:
                            json_data = json.dumps(message, ensure_ascii=False)
                            yield f'data: {json_data}\n\n'
                        except (TypeError, ValueError) as exc:
                            logger.error(f'SSE消息序列化失败: {exc}, 消息类型: {type(message)}')
                            error_message = {
                                'event': 'error',
                                'data': {'error': f'消息序列化失败: {str(exc)}'},
                                'timestamp': datetime.now().isoformat(),
                            }
                            yield f"data: {json.dumps(error_message)}\n\n"
                    except Empty:
                        yield f"data: {json.dumps({'event': 'heartbeat', 'data': {'timestamp': datetime.now().isoformat()}})}\n\n"
                    except GeneratorExit:
                        break
                    except Exception as exc:
                        logger.error(f'SSE流处理错误: {exc}')
                        try:
                            error_message = {
                                'event': 'error',
                                'data': {'error': f'流处理错误: {str(exc)}'},
                                'timestamp': datetime.now().isoformat(),
                            }
                            yield f"data: {json.dumps(error_message)}\n\n"
                        except Exception:
                            pass
                        break
            except Exception as exc:
                logger.error(f'SSE流错误: {exc}')
            finally:
                context.sse_manager.remove_client(client_id)

        return Response(
            event_stream(),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Access-Control-Allow-Origin': '*',
            },
        )

    @app.route('/api/query_select_history', methods=['GET', 'POST'])
    def query_select_history():
        context = _context()
        data = request.json
        strategy_code = data.get('strategy', '').strip()
        market = data.get('market', '').strip()
        date_str = data.get('date', '').strip()
        client_id = data.get('client_id')
        lock_name = f'history_{strategy_code}_{market}'
        with context.task_lock:
            if lock_name in context.analysis_tasks:
                return jsonify({'success': False, 'error': f'股票 {lock_name} 正在分析中，请稍候'}), 429
            context.analysis_tasks[lock_name] = {
                'start_time': datetime.now(),
                'status': 'analyzing',
                'client_id': client_id,
            }

        try:
            def run_analysis():
                streamer = StreamingAnalyzer(client_id, context.sse_manager)
                try:
                    streamer.send_log(f"🚀 开始查询股票: {strategy_code}", 'header')
                    streamer.send_progress('singleProgress', 5, '开始筛选股票...')
                    context.analyzer.streaming = streamer
                    context.analyzer.query_select_history(strategy_code, market, date_str)
                    logger.info(f'股票流式分析完成: {strategy_code}')
                    streamer.send_log(f"🚀 筛选股票完成: {strategy_code}", 'header')
                    streamer.send_progress('singleProgress', 100, '筛选股票完成...')
                    streamer.send_completion(f'筛选股票完成: {strategy_code}')
                except Exception as exc:
                    streamer.send_log(f"🚀 筛选股票出错: {strategy_code}", 'header')
                    streamer.send_progress('singleProgress', 100, '筛选股票出错...')
                    streamer.send_error(f'筛选股票失败: {strategy_code}, 错误: {exc}')
                    streamer.send_completion(f'筛选股票失败: {strategy_code}, 错误: {exc}')
                    logger.error(f'筛选股票出错失败: {strategy_code}, 错误: {exc}')
                finally:
                    with context.task_lock:
                        context.analysis_tasks.pop(strategy_code, None)

            context.executor.submit(run_analysis)
            logger.info(f'股票分析完成: {lock_name}')
            return jsonify({'success': True, 'data': '', 'message': f'股票 {lock_name} 分析完成'})
        except Exception as exc:
            logger.error(f'股票分析失败: {exc}')
            with context.task_lock:
                context.analysis_tasks.pop(lock_name, None)
            return jsonify({'success': False, 'error': str(exc)}), 500
        finally:
            with context.task_lock:
                context.analysis_tasks.pop(lock_name, None)

    @app.route('/api/query_analysis_history', methods=['GET', 'POST'])
    def query_analysis_history():
        context = _context()
        data = request.json
        stock_code = data.get('stock', '').strip()
        market = data.get('market', '').strip()
        date_str = data.get('date', '').strip()
        client_id = data.get('client_id')
        lock_name = f'history_{stock_code}_{market}'
        with context.task_lock:
            if lock_name in context.analysis_tasks:
                return jsonify({'success': False, 'error': f'股票 {lock_name} 正在分析中，请稍候'}), 429
            context.analysis_tasks[lock_name] = {
                'start_time': datetime.now(),
                'status': 'analyzing',
                'client_id': client_id,
            }

        try:
            def run_analysis():
                streamer = StreamingAnalyzer(client_id, context.sse_manager)
                try:
                    streamer.send_log(f"🚀 开始查询股票: {stock_code}", 'header')
                    streamer.send_progress('singleProgress', 5, '开始筛选股票...')
                    context.analyzer.streaming = streamer
                    context.analyzer.query_analysis_history(stock_code, market, date_str)
                    logger.info(f'股票流式分析完成: {stock_code}')
                    streamer.send_log(f"🚀 筛选股票完成: {stock_code}", 'header')
                    streamer.send_progress('singleProgress', 100, '筛选股票完成...')
                    streamer.send_completion(f'筛选股票完成: {stock_code}')
                except Exception as exc:
                    streamer.send_log(f"🚀 筛选股票出错: {stock_code}", 'header')
                    streamer.send_progress('singleProgress', 100, '筛选股票出错...')
                    streamer.send_error(f'筛选股票失败: {stock_code}, 错误: {exc}')
                    streamer.send_completion(f'筛选股票失败: {stock_code}, 错误: {exc}')
                    logger.error(f'筛选股票出错失败: {stock_code}, 错误: {exc}')
                finally:
                    with context.task_lock:
                        context.analysis_tasks.pop(stock_code, None)

            context.executor.submit(run_analysis)
            logger.info(f'股票分析完成: {lock_name}')
            return jsonify({'success': True, 'data': '', 'message': f'股票 {lock_name} 分析完成'})
        except Exception as exc:
            logger.error(f'股票分析失败: {exc}')
            with context.task_lock:
                context.analysis_tasks.pop(lock_name, None)
            return jsonify({'success': False, 'error': str(exc)}), 500
        finally:
            with context.task_lock:
                context.analysis_tasks.pop(lock_name, None)

    @app.route('/datacurve', methods=['GET', 'POST'])
    def datacurve():
        if request.method == 'POST':
            stock_code = request.form.get('stock_code')
            start_date = request.form.get('start_date')
            end_date = request.form.get('end_date')
            market = request.form.get('market')
            sq = stockIndicatorQuantitative.stockIndicatorQuantitative()
            df = sq.stock_day_data_code(stock_code, market, start_date.replace('-', ''), end_date.replace('-', ''))

            sma_html = ''
            fft_html = ''
            bollinger_html = ''
            wave_html = ''
            try:
                stockcurve = stockIndicatorHtml.stockIndicatorHtml()
                sma_html = stockcurve.plot_sma(df)
                fft_html = stockcurve.plot_stock_fft(df)
                bollinger_html = stockcurve.plot_stock_Bollinger(df)
                wave_html = stockcurve.plot_stock_wave(df)
            except Exception as exc:
                print(f'渲染数据时出现错误: {exc}')

            return f"""
            <!DOCTYPE html>
            <html lang='en'>
            <head>
                <meta charset='UTF-8'>
                <title>Stock Analysis Plots</title>
            </head>
            <body>
                <h1>布林带图</h1>
                {bollinger_html}
                <h1>移动平均线图</h1>
                {sma_html}
                <h1>傅里叶变换图</h1>
                {fft_html}
                <h1>小波分析图</h1>
                {wave_html}
            </body>
            </html>
            """

        return """
        <!DOCTYPE html>
        <html lang='en'>
        <head>
            <meta charset='UTF-8'>
            <title>Stock Analysis Input</title>
        </head>
        <body>
            <h1>股票分析输入</h1>
            <form method='post'>
                <label for='stock_code'>股票代码:</label>
                <input type='text' id='stock_code' name='stock_code' required><br><br>
                <label for='start_date'>开始日期 (YYYY-MM-DD):</label>
                <input type='text' id='start_date' name='start_date' required><br><br>
                <label for='end_date'>结束日期 (YYYY-MM-DD):</label>
                <input type='text' id='end_date' name='end_date' required><br><br>
                <label for='market'>市场类型 (usa/H/zq):</label>
                <input type='text' id='market' name='market' required><br><br>
                <input type='submit' value='提交'>
            </form>
        </body>
        </html>
        """
