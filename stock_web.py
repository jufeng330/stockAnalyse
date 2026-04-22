from flask import Flask, render_template, request, jsonify, session, redirect, url_for, Response
from functools import wraps
import logging
import json
from datetime import datetime, timedelta
from queue import Queue, Empty
import threading
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

import stocklib.stock_indicator_quantitative as stockIndicatorQuantitative
import matplotlib
import stocklib.stock_indicator_html as stockIndicatorHtml
from web_sse.stock_analyzer_service import StockAnalyzerService
from web_sse.sse_manager import SSEManager
from web_sse.streaminganalyzer import  StreamingAnalyzer
from stock_analyse.infrastructure.config.settings import get_settings

# 配置日志 - 只输出到命令行
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

matplotlib.use('Agg')
app = Flask(__name__)
settings = get_settings()
app.secret_key = settings.web.flask_secret_key


# 全局SSE管理器
sse_manager = SSEManager()
analyzer = StockAnalyzerService()

analysis_tasks = {}  # 存储分析任务状态
task_results = {}   # 存储任务结果
task_lock = threading.Lock()
sse_clients = {}    # 存储SSE客户端连接
sse_lock = threading.Lock()
executor = ThreadPoolExecutor(max_workers=4)


def require_auth(f):
    """鉴权装饰器"""

    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_enabled, auth_config = check_auth_config()

        if not auth_enabled:
            return f(*args, **kwargs)

        # 检查session中是否已认证
        if session.get('authenticated'):
            # 检查session是否过期
            login_time = session.get('login_time')
            if login_time:
                session_timeout = auth_config.get('session_timeout', 3600)
                if (datetime.now() - datetime.fromisoformat(login_time)).total_seconds() < session_timeout:
                    return f(*args, **kwargs)
                else:
                    session.pop('authenticated', None)
                    session.pop('login_time', None)

        return redirect(url_for('login'))

    return decorated_function


@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('stock.html')



@app.route('/api/select_stock', methods=['GET', 'POST'])
def select_stock():
    data = request.json
    strategy_code = data.get('strategy', '1').strip()
    market = data.get('market', 'SH').strip()
    enable_streaming = data.get('enable_streaming', False)
    client_id = data.get('client_id')
    lock_name = f'{strategy_code}_{market}'
    with task_lock:
        if lock_name in analysis_tasks:
            return jsonify({
                'success': False,
                'error': f'股票 {lock_name} 正在分析中，请稍候'
            }), 429

        analysis_tasks[lock_name] = {
            'start_time': datetime.now(),
            'status': 'analyzing',
            'client_id': client_id
        }

    try:
        # 执行分析
        def run_analysis():
            streamer = StreamingAnalyzer(client_id, sse_manager)
            try:
                streamer.send_log(f"🚀 开始筛选股票: {strategy_code}", 'header')
                streamer.send_progress('singleProgress', 5, "开始筛选股票...")
                analyzer.streaming = streamer
                global json_result
                json_result = analyzer.stock_select_process(strategy_code, market)
                logger.info(f"股票流式分析完成: {strategy_code}")
                streamer.send_log(f"🚀 筛选股票完成: {strategy_code}", 'header')
                streamer.send_progress('singleProgress', 100, "筛选股票完成...")
                streamer.send_completion(f'筛选股票完成: {strategy_code}')
            except Exception as e:
                streamer.send_log(f"🚀 筛选股票出错: {strategy_code}", 'header')
                streamer.send_progress('singleProgress', 100, "筛选股票出错...")
                streamer.send_error(f"筛选股票失败: {strategy_code}, 错误: {e}")
                streamer.send_completion(f'筛选股票失败: {strategy_code}, 错误: {e}')
                logger.error(f"筛选股票出错失败: {strategy_code}, 错误: {e}")
            finally:
                with task_lock:
                    analysis_tasks.pop(strategy_code, None)

        # 在线程池中执行
        executor.submit(run_analysis)


        # 清理数据中的NaN值
        # cleaned_report = sse_manager.clean_data_for_json(report)

        logger.info(f"股票分析完成: {lock_name}")

        return jsonify({
            'success': True,
            'data': '',
            'message': f'股票 {lock_name} 分析完成'
        })

    except Exception as e:
        logger.error(f"股票分析失败: {e}")
        with task_lock:
            analysis_tasks.pop(lock_name, None)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        with task_lock:
            analysis_tasks.pop(lock_name, None)

@app.route('/api/analyze_stock', methods=['GET', 'POST'])
def analyze_stock():
    data = request.json
    stock_code = data.get('stock_code', '').strip()
    market = data.get('market', 'SH').strip()
    enable_streaming = data.get('enable_streaming', False)
    client_id = data.get('client_id')

    start_date_str =  (datetime.now() - timedelta(days=100)).strftime("%Y-%m-%d")
    end_date_str = datetime.now().strftime("%Y-%m-%d")

    with task_lock:
        if stock_code in analysis_tasks:
            return jsonify({
                'success': False,
                'error': f'股票 {stock_code} 正在分析中，请稍候'
            }), 429

        analysis_tasks[stock_code] = {
            'start_time': datetime.now(),
            'status': 'analyzing',
            'client_id': client_id
        }

    try:
        # 执行分析
        def run_analysis():
            streamer = StreamingAnalyzer(client_id, sse_manager)
            try:
                streamer.send_log(f"🚀 开始流式分析股票: {stock_code}", 'header')
                streamer.send_progress('singleProgress', 5, "正在获取股票基本信息...")
                analyzer.streaming = streamer
                global json_result
                json_result = analyzer.stock_analysis_process(stock_code, market, start_date_str, end_date_str)
                logger.info(f"股票流式分析完成: {stock_code}")
                streamer.send_log(f"🚀 股票流式分析完成: {stock_code}", 'header')
                streamer.send_progress('singleProgress', 100, "股票流式分析完成...")
                streamer.send_completion(f'股票流式分析完成: {stock_code}')
            except Exception as e:
                streamer.send_log(f"🚀 股票流式戳无完成: {stock_code}", 'header')
                streamer.send_progress('singleProgress', 100, "股票流式分析戳无...")
                streamer.send_error(f"股票流式分析失败: {stock_code}, 错误: {e}")
                streamer.send_completion(f'股票流式分析失败: {stock_code}, 错误: {e}')
                logger.error(f"股票流式分析失败: {stock_code}, 错误: {e}")
            finally:
                with task_lock:
                    analysis_tasks.pop(stock_code, None)

        # 在线程池中执行
        executor.submit(run_analysis)


        # 清理数据中的NaN值
        # cleaned_report = sse_manager.clean_data_for_json(report)

        logger.info(f"股票分析完成: {stock_code}")

        return jsonify({
            'success': True,
            'data': '',
            'message': f'股票 {stock_code} 分析完成'
        })

    except Exception as e:
        logger.error(f"股票分析失败: {e}")
        with task_lock:
            analysis_tasks.pop(stock_code, None)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        with task_lock:
            analysis_tasks.pop(stock_code, None)


@app.route('/history', methods=['GET', 'POST'])
def history():
    return render_template('history.html')



@app.route('/api/history/analyse', methods=['GET', 'POST'])
def history_analyse():
    stock_code = request.args.get('stock', default='105.AMZN', type=str)
    date_str = request.args.get('date', default='20250717', type=str)
    report_technical, report_financial, report_technical_request = analyzer.find_history_stock_analysis(stock_code, date_str)


    return render_template('history_analyse.html',
                            stock_summary = f'###股票代码：{stock_code}\n ###日期：{date_str}',
                            fundamental_analysis = report_technical,
                            annual_report_analysis = report_financial,
                            sentiment_analysis = report_technical_request)

@app.route('/api/history/select', methods=['GET', 'POST'])
def history_selector():
    strategy_name = request.args.get('strategy', default='知名股票筛选策略', type=str)
    date_str = request.args.get('date', default='2025081414', type=str)
    market = request.args.get('market', default='H', type=str)
    report_high_score,report_all,report_summary = analyzer.find_history_strategy_analysis(strategy_name,date_str,market)

    return render_template('history_analyse.html',
                            stock_summary = f'###股票代码：{strategy_name}\n ###日期：{date_str}',
                            fundamental_analysis = report_high_score,
                            annual_report_analysis = report_all,
                            sentiment_analysis = report_summary)

@app.route('/stock', methods=['GET', 'POST'])
def stock_analysis():


    # 设置默认值
    ai_platform = request.form.get('ai_platform', settings.ai.platform)
    ai_model = request.form.get('ai_model', settings.ai.model_name)
    api_code = request.form.get('api_code', settings.ai.api_key)
    system_prompt = request.form.get('system_prompt', settings.ai.system_prompt)
    message_format = request.form.get('message_format', None) or settings.ai.prompt_template

    if request.method == 'POST':
        # 处理表单提交的逻辑
        stock_code = request.form.get('stock_code')
        start_date_str = request.form.get('start_date')
        end_date_str = request.form.get('end_date')
        market = request.form.get('market')
        selected_strategies = request.form.getlist('strategies')

        # 使用从表单获取的值，如果没有提供则使用默认值
        ai_platform = request.form.get('ai_platform', ai_platform)
        ai_model = request.form.get('ai_model', ai_model)
        api_code = request.form.get('api_code', api_code)
        system_prompt = request.form.get('system_prompt', system_prompt)
        message_format = request.form.get('message_format', message_format)

        image_paths, strategies_selected, stock_summary, stock_analysis_result, annual_report_analysis, sentiment_analysis,sentiment_score = analyzer.get_stock_analysis(
            stock_code, market, start_date_str, end_date_str,
            selected_strategies, system_prompt, message_format,
            ai_platform, ai_model, api_code)
        # 调用后续方法
        return render_template('result.html', 
                               image_paths=image_paths, 
                               strategies=strategies_selected,
                               stock_summary=stock_summary,
                               fundamental_analysis=stock_analysis_result,
                               annual_report_analysis=annual_report_analysis,
                               sentiment_analysis=sentiment_analysis)

    # 默认值
    current_date = datetime.now()
    current_date_str = current_date.strftime("%Y-%m-%d")
    previous_year = current_date - timedelta(days=100)
    previous_year_str = previous_year.strftime("%Y-%m-%d")

    all_strategies = [
        '均线策略',
        '布林带策略',
        '动量MACD策略',
        '突破策略',
        'SAR策略',
        '均值回归策略',
        'RSI策略',
        'KDJ策略',
        '威廉指标策略',
        'ADX策略',
        '线性回归策略',
        'K线形态策略',
        '神经网络多层感知回归策略'
    ]

    return render_template('index.html', 
                           start_date=previous_year_str, 
                           end_date=current_date_str, 
                           market='CN', 
                           strategies=all_strategies,
                           ai_platform=ai_platform, 
                           ai_model=ai_model, 
                           api_code=api_code, 
                           system_prompt=system_prompt, 
                           message_format=message_format)

@app.route('/datacurve', methods=['GET', 'POST'])
def datacurve():
    if request.method == 'POST':

        plt.switch_backend('Agg')

        stock_code = request.form.get('stock_code')
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
        market = request.form.get('market')

        sq = stockIndicatorQuantitative.stockIndicatorQuantitative()
        # 获取股票数据
        df = sq.stock_day_data_code(stock_code, market, start_date.replace('-', ''), end_date.replace('-', ''))

        sma_html = ''
        fft_html = ''
        bollinger_html = ''
        wave_html = ''
        try:
            stockcurve = stockIndicatorHtml.stockIndicatorHtml()
            # 生成四种图的 HTML 片段
            sma_html = stockcurve.plot_sma(df)
            fft_html = stockcurve.plot_stock_fft(df)
            bollinger_html = stockcurve.plot_stock_Bollinger(df)
            wave_html = stockcurve.plot_stock_wave(df)
        except Exception as e:
            print(f"渲染数据时出现错误: {e}")

        # 创建 HTML 页面
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
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
        return html_content

    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Stock Analysis Input</title>
    </head>
    <body>
        <h1>股票分析输入</h1>
        <form method="post">
            <label for="stock_code">股票代码:</label>
            <input type="text" id="stock_code" name="stock_code" required><br><br>
            <label for="start_date">开始日期 (YYYY-MM-DD):</label>
            <input type="text" id="start_date" name="start_date" required><br><br>
            <label for="end_date">结束日期 (YYYY-MM-DD):</label>
            <input type="text" id="end_date" name="end_date" required><br><br>
            <label for="market">市场类型 (usa/H/zq):</label>
            <input type="text" id="market" name="market" required><br><br>
            <input type="submit" value="提交">
        </form>
    </body>
    </html>
    """

@app.route('/stockSelector', methods=['GET', 'POST'])
def stock_select():
    return render_template('stock.html' )


@app.route('/login', methods=['GET', 'POST'])
def login():
    """登录页面"""
    auth_enabled, auth_config = check_auth_config()

    if not auth_enabled:
        return redirect(url_for('index'))

    if request.method == 'POST':
        password = request.form.get('password', '')
        config_password = auth_config.get('password', '')

        if not config_password:
            return render_template('login.html',
                                          error="系统未设置访问密码，请联系管理员配置",
                                          session_timeout=auth_config.get('session_timeout', 3600) // 60
                                          )

        if password == config_password:
            session['authenticated'] = True
            session['login_time'] = datetime.now().isoformat()
            logger.info("用户登录成功")
            return redirect(url_for('index'))
        else:
            logger.warning("用户登录失败：密码错误")
            return render_template('login.html',
                                          error="密码错误，请重试",
                                          session_timeout=auth_config.get('session_timeout', 3600) // 60
                                          )

    return render_template('login.html',
                                  session_timeout=auth_config.get('session_timeout', 3600) // 60
                                  )


@app.route('/api/sse')
def sse_stream():
    """SSE流接口"""
    client_id = request.args.get('client_id')
    if not client_id:
        return "Missing client_id", 400

    def event_stream():
        # 创建客户端队列
        client_queue = Queue()
        sse_manager.add_client(client_id, client_queue)

        try:
            # 发送连接确认
            yield f"data: {json.dumps({'event': 'connected', 'data': {'client_id': client_id}})}\n\n"

            while True:
                try:
                    # 获取消息（带超时，防止长时间阻塞）
                    message = client_queue.get(timeout=30)

                    # 确保消息可以JSON序列化
                    try:
                        json_data = json.dumps(message, ensure_ascii=False)
                        yield f"data: {json_data}\n\n"
                    except (TypeError, ValueError) as e:
                        logger.error(f"SSE消息序列化失败: {e}, 消息类型: {type(message)}")
                        # 发送错误消息
                        error_message = {
                            'event': 'error',
                            'data': {'error': f'消息序列化失败: {str(e)}'},
                            'timestamp': datetime.now().isoformat()
                        }
                        yield f"data: {json.dumps(error_message)}\n\n"

                except Empty:
                    # 发送心跳
                    yield f"data: {json.dumps({'event': 'heartbeat', 'data': {'timestamp': datetime.now().isoformat()}})}\n\n"
                except GeneratorExit:
                    break
                except Exception as e:
                    logger.error(f"SSE流处理错误: {e}")
                    try:
                        error_message = {
                            'event': 'error',
                            'data': {'error': f'流处理错误: {str(e)}'},
                            'timestamp': datetime.now().isoformat()
                        }
                        yield f"data: {json.dumps(error_message)}\n\n"
                    except:
                        pass
                    break

        except Exception as e:
            logger.error(f"SSE流错误: {e}")
        finally:
            sse_manager.remove_client(client_id)

    return Response(
        event_stream(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*',
        }
    )

@app.route('/api/query_select_history', methods=['GET', 'POST'])
def query_select_history():
    data = request.json
    strategy_code = data.get('strategy', '').strip()
    market = data.get('market', '').strip()
    date_str = data.get('date', '').strip()

    enable_streaming = data.get('enable_streaming', False)
    client_id = data.get('client_id')
    lock_name = f'history_{strategy_code}_{market}'
    with task_lock:
        if lock_name in analysis_tasks:
            return jsonify({
                'success': False,
                'error': f'股票 {lock_name} 正在分析中，请稍候'
            }), 429

        analysis_tasks[lock_name] = {
            'start_time': datetime.now(),
            'status': 'analyzing',
            'client_id': client_id
        }

    try:
        # 执行分析
        def run_analysis():
            streamer = StreamingAnalyzer(client_id, sse_manager)
            try:
                streamer.send_log(f"🚀 开始查询股票: {strategy_code}", 'header')
                streamer.send_progress('singleProgress', 5, "开始筛选股票...")
                analyzer.streaming = streamer

                json_result = analyzer.query_select_history(strategy_code, market,date_str)
                logger.info(f"股票流式分析完成: {strategy_code}")
                streamer.send_log(f"🚀 筛选股票完成: {strategy_code}", 'header')
                streamer.send_progress('singleProgress', 100, "筛选股票完成...")
                streamer.send_completion(f'筛选股票完成: {strategy_code}')
            except Exception as e:
                streamer.send_log(f"🚀 筛选股票出错: {strategy_code}", 'header')
                streamer.send_progress('singleProgress', 100, "筛选股票出错...")
                streamer.send_error(f"筛选股票失败: {strategy_code}, 错误: {e}")
                streamer.send_completion(f'筛选股票失败: {strategy_code}, 错误: {e}')
                logger.error(f"筛选股票出错失败: {strategy_code}, 错误: {e}")
            finally:
                with task_lock:
                    analysis_tasks.pop(strategy_code, None)

        # 在线程池中执行
        executor.submit(run_analysis)


        # 清理数据中的NaN值
        # cleaned_report = sse_manager.clean_data_for_json(report)

        logger.info(f"股票分析完成: {lock_name}")

        return jsonify({
            'success': True,
            'data': '',
            'message': f'股票 {lock_name} 分析完成'
        })

    except Exception as e:
        logger.error(f"股票分析失败: {e}")
        with task_lock:
            analysis_tasks.pop(lock_name, None)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        with task_lock:
            analysis_tasks.pop(lock_name, None)


@app.route('/api/query_analysis_history', methods=['GET', 'POST'])
def query_analysis_history():
    data = request.json
    stock_code = data.get('stock', '').strip()
    market = data.get('market', '').strip()
    date_str = data.get('date', '').strip()

    enable_streaming = data.get('enable_streaming', False)
    client_id = data.get('client_id')
    lock_name = f'history_{stock_code}_{market}'
    with task_lock:
        if lock_name in analysis_tasks:
            return jsonify({
                'success': False,
                'error': f'股票 {lock_name} 正在分析中，请稍候'
            }), 429

        analysis_tasks[lock_name] = {
            'start_time': datetime.now(),
            'status': 'analyzing',
            'client_id': client_id
        }

    try:
        # 执行分析
        def run_analysis():
            streamer = StreamingAnalyzer(client_id, sse_manager)
            try:
                streamer.send_log(f"🚀 开始查询股票: {stock_code}", 'header')
                streamer.send_progress('singleProgress', 5, "开始筛选股票...")
                analyzer.streaming = streamer

                json_result = analyzer.query_analysis_history(stock_code, market,date_str)
                logger.info(f"股票流式分析完成: {stock_code}")
                streamer.send_log(f"🚀 筛选股票完成: {stock_code}", 'header')
                streamer.send_progress('singleProgress', 100, "筛选股票完成...")
                streamer.send_completion(f'筛选股票完成: {stock_code}')
            except Exception as e:
                streamer.send_log(f"🚀 筛选股票出错: {stock_code}", 'header')
                streamer.send_progress('singleProgress', 100, "筛选股票出错...")
                streamer.send_error(f"筛选股票失败: {stock_code}, 错误: {e}")
                streamer.send_completion(f'筛选股票失败: {stock_code}, 错误: {e}')
                logger.error(f"筛选股票出错失败: {stock_code}, 错误: {e}")
            finally:
                with task_lock:
                    analysis_tasks.pop(stock_code, None)

        # 在线程池中执行
        executor.submit(run_analysis)


        # 清理数据中的NaN值
        # cleaned_report = sse_manager.clean_data_for_json(report)

        logger.info(f"股票分析完成: {lock_name}")

        return jsonify({
            'success': True,
            'data': '',
            'message': f'股票 {lock_name} 分析完成'
        })

    except Exception as e:
        logger.error(f"股票分析失败: {e}")
        with task_lock:
            analysis_tasks.pop(lock_name, None)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        with task_lock:
            analysis_tasks.pop(lock_name, None)


def check_auth_config():
    """检查鉴权配置"""
    if not analyzer:
        return False, {}

    web_auth_config = settings.as_service_config().get('web_auth', {})
    return web_auth_config.get('enabled', False), web_auth_config


if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port = 38080)
