from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, request, flash, redirect, url_for
import datetime
import matplotlib.pyplot as plt
import stocklib.stock_indicator_quantitative as stockIndicatorQuantitative
import matplotlib
import stocklib.stock_indicator_html as stockIndicatorHtml
import stockAI.stockAgent.stock_ai_analysis as stockAIAnalysis
from stocklib.stock_company import stockCompanyInfo

matplotlib.use('Agg')

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # 添加 secret_key 以支持 flash 功能


@app.route('/', methods=['GET', 'POST'])
def index():

    prompt_template = """
    '请基于以上收集到的实时的真实数据，发挥你的A股分析专业知识，对未来3天该股票的价格走势做出深度预测。\n在预测中请全面考虑主营业务、基本数据、所在行业数据、所在概念板块数据、历史行情、最近新闻以及资金流动等多方面因素。\n给出具体的涨跌百分比数据分析总结。'
                当前股票主营业务介绍:
    
                {stock_zyjs_ths_df}
                
                当前股票所在的行业资金流数据:
                {single_industry_df}
                
                当前股票所在的概念板块的数据:
                {concept_info_df}
                
                当前股票基本数据:
                {stock_individual_info_em_df}
                
                当前股票历史行情数据和K线技术指标::
                {stock_zh_a_hist_df}
                
                当前股票最近的新闻:
                {stock_news_em_df}
                
                当前股票历史的资金流动:
                {stock_individual_fund_flow_df}
                
                当前股票的财务指标数据:
                {stock_financial_analysis_indicator_df}
                
                """
    qwen_token =  'sk-969bede797ca4aa2b436835882efcd6c'
    # 设置默认值
    ai_platform = request.form.get('ai_platform', 'qwen')
    ai_model = request.form.get('ai_model', 'qwen3-8b')
    api_code = request.form.get('api_code',qwen_token)  # 默认值：'default_token'
    system_prompt = request.form.get('system_prompt', '你作为A股分析专家,请详细分析市场趋势、行业前景，揭示潜在投资机会,请确保提供充分的数据支持和专业见解。')  # 默认值：系统提示语
    message_format = request.form.get('message_format', prompt_template)  # 默认值：Message格式

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

        # 转换日期格式
        try:
            start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')
        except ValueError:
            flash('日期格式不正确，请使用 YYYY-MM-DD 格式。', 'error')
            return redirect(url_for('index'))

        sq = stockIndicatorQuantitative.stockIndicatorQuantitative()
        # 获取股票数据
        stock_data = sq.stock_day_data_code(stock_code, market, start_date_str, end_date_str)
        if stock_data is None or stock_data.empty:
            print("stock_data is null")
            flash('stock_data is null。', 'error')  # 确保 flash 函数正确调用
            return redirect(url_for('index'))

        strategy_functions = {
            # 均线策略：通过计算不同周期的移动平均线，根据均线的交叉情况生成交易信号
            'strategy_mac': sq.plot_strategy_mac,
            # 布林带策略：利用布林带指标（上轨、中轨、下轨）来判断股价的波动范围和趋势，进而生成交易信号
            'plot_strategy_bollinger': sq.plot_strategy_bollinger,
            # 动量策略：结合动量指标和 MACD 指标，根据指标的变化情况生成交易信号
            'plot_strategy_macd': sq.plot_strategy_macd,
            # 突破策略：当股价突破特定的阻力位或支撑位时，生成相应的交易信号
            'plot_strategy_breakout': sq.plot_strategy_breakout,
            # SAR 策略：使用抛物线转向指标（SAR）来跟踪股价趋势，根据 SAR 指标的变化生成交易信号
            'plot_strategy_sar': sq.plot_strategy_sar,
            # 均值回归策略：基于股价会围绕其均值波动的假设，当股价偏离均值较大时，生成交易信号，预期股价会回归均值
            'plot_mean_reversion_strategy': sq.plot_mean_reversion_strategy,
            # RSI 策略：通过相对强弱指数（RSI）判断市场的超买超卖情况，进而生成交易信号
            'strategy_rsi': sq.strategy_rsi,
            # KDJ 策略：利用随机指标（KDJ）来分析股价的短期走势，根据 KDJ 指标的交叉和数值范围生成交易信号
            'strategy_kdj': sq.strategy_kdj,
            # Williams %R 策略：通过威廉指标（Williams %R）判断市场的超买超卖情况，生成交易信号
            'strategy_williams_r': sq.strategy_williams_r,
            # ADX 策略：使用平均趋向指标（ADX）来判断市场趋势的强弱，结合正负趋向指标生成交易信号
            'strategy_adx': sq.strategy_adx,
            # 线性回归策略：利用线性回归模型对股价进行预测，根据预测结果生成交易信号
            'strategy_linear_regression': sq.strategy_linear_regression,
            # K 线策略：根据 K 线的形态（如吞没形态、锤子线等）来判断市场趋势，生成交易信号
            'strategy_kline_pattern': sq.strategy_kline_pattern,
            # 神经网络策略：使用多层感知机（MLP）神经网络对股价进行回归预测，根据预测结果生成交易信号
            'strategy_mlp_regression': sq.strategy_mlp_regression
        }

        strategy_functions = {
            '均线策略': sq.plot_strategy_mac,
            '布林带策略': sq.plot_strategy_bollinger,
            '动量MACD策略': sq.plot_strategy_macd,
            '突破策略': sq.plot_strategy_breakout,
            'SAR策略': sq.plot_strategy_sar,
            '均值回归策略': sq.plot_mean_reversion_strategy,
            'RSI策略': sq.strategy_rsi,
            'KDJ策略': sq.strategy_kdj,
            '威廉指标策略': sq.strategy_williams_r,
            'ADX策略': sq.strategy_adx,
            '线性回归策略': sq.strategy_linear_regression,
            'K线形态策略': sq.strategy_kline_pattern,
            '神经网络多层感知回归策略': sq.strategy_mlp_regression
        }

        image_paths = []
        strategies_selected = []

        for strategy in selected_strategies:
            if strategy in strategy_functions:
                plt.clf()  # 清空当前图形
                strategy_functions[strategy](stock_data)
                image_path = f'static/{strategy}_{stock_code}.png'
                plt.savefig(image_path)
                image_paths.append(image_path)
                strategies_selected.append(strategy)

        # "公司基本面分析结果内容"
        stock_analysis = stockAIAnalysis.StockAnalyzer(system_prompt=system_prompt,
                                                       prompt_template=message_format, ai_platform=ai_platform,
                                                       model=ai_model, api_token=api_code)

        stock_report_analysis = stockAIAnalysis.StockAnalyzer(system_prompt=system_prompt,
                                                              prompt_template=message_format, ai_platform=ai_platform,
                                                              model=ai_model, api_token=api_code)

        # 创建线程池执行器
        with ThreadPoolExecutor(max_workers=3) as executor:
            # 提交任务到线程池

            future_analysis = executor.submit(stock_analysis.stock_indicator_analyse, market=market, symbol=stock_code, start_date=start_date_str, end_date=end_date_str)



            future_report = executor.submit(stock_report_analysis.stock_report_analyse, market=market, symbol=stock_code)
            future_summary = executor.submit(stock_analysis.get_stock_summary, market=market, symbol=stock_code)

            # 等待所有任务完成并获取结果
            stock_analysis_result = future_analysis.result()
            annual_report_analysis = future_report.result()
            stock_summary = future_summary.result()

        # 调用后续方法
        return render_template('result.html', 
                               image_paths=image_paths, 
                               strategies=strategies_selected,
                               stock_summary=stock_summary,
                               fundamental_analysis=stock_analysis_result,
                               annual_report_analysis=annual_report_analysis,
                               sentiment_analysis="公司情绪分析内容 未实现")

    # 默认值
    current_date = datetime.datetime.now()
    current_date_str = current_date.strftime("%Y-%m-%d")
    previous_year = current_date - datetime.timedelta(days=100)
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
if __name__ == '__main__':
    app.run(debug=True,port = 5000)