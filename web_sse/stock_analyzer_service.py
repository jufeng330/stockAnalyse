"""
Web版增强股票分析系统 - 支持AI流式输出
基于最新 stock_analyzer.py 修正版本，新增AI流式返回功能
"""

import os
import sys
import logging
import warnings
import pandas as pd
import numpy as np
import json
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Callable
import time
import re
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
from scanner.stock_analyzer import  StockAnalyzer
from .streaminganalyzer import StreamingAnalyzer

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from stockAI.stockAgent.stock_ai_analyzer import  StockAiAnalyzer
from stocklib.stock_border import stockBorderInfo
from stocklib.stock_company import stockCompanyInfo
from stocklib.stock_sentiment_analysis import StockSentimentAnalysis
from stocklib.stock_indicator_quantitative import stockIndicatorQuantitative
from scanner.top_stock_scanner import TopStockScanner
from scanner.stock_result_utils import  StockFileUtils
from stock_analyse.application.use_cases import analyze_single_stock as analyze_single_stock_use_case
from stock_analyse.infrastructure.config.settings import get_settings, DEFAULT_PROMPT_TEMPLATE, DEFAULT_SYSTEM_PROMPT

# 忽略警告
warnings.filterwarnings('ignore')

# 设置日志 - 只输出到命令行
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # 只保留命令行输出
    ]
)


class StockAnalyzerService:
    """Web版增强股票分析器（基于最新 stock_analyzer.py 修正，支持AI流式输出）"""

    def __init__(self, config_file='config.json'):
        """初始化分析器"""
        self.logger = logging.getLogger(__name__)
        self.config_file = config_file
        self.settings = get_settings(config_file)
        self.config = self.settings.as_service_config()

        ai_config = self.config.get('ai', {})
        self.ai_config = {
            'max_tokens': ai_config.get('max_tokens', 4000),
            'temperature': ai_config.get('temperature', 0.7),
            'model_preference': ai_config.get('model_preference', 'openai'),
            'model_plat': ai_config.get("model_plat", "qwen"),
            'model_name': ai_config.get("model_name", "qwen-turbo-2025-07-15"),
            'api_key': ai_config.get("api_key", "")
        }
        self.message_format = ai_config.get("prompt_template", DEFAULT_PROMPT_TEMPLATE)
        self.system_prompt = ai_config.get("system_prompt", DEFAULT_SYSTEM_PROMPT)
        self.ai_platform = self.ai_config.get("model_plat", "qwen")
        self.ai_model = self.ai_config.get("model_name", "qwen-turbo-2025-07-15")
        self.api_code = self.ai_config.get("api_key", "")
        self.api_keys = self.config.get('api_keys', {})

        if self.api_code and self.ai_platform and not self.api_keys.get(self.ai_platform):
            self.api_keys[self.ai_platform] = self.api_code

        if not any(value for key, value in self.api_keys.items() if key != 'notes') and self.api_code:
            self.api_keys['active'] = self.api_code

        self.config['ai']['prompt_template'] = self.message_format
        self.config['ai']['system_prompt'] = self.system_prompt
        self.config['ai']['api_key'] = self.api_code
        self.config['web_auth'] = {
            'enabled': self.settings.web.auth_enabled,
            'password': self.settings.web.auth_password,
            'session_timeout': self.settings.web.session_timeout,
            'notes': self.config.get('web_auth', {}).get('notes', 'Web界面密码鉴权配置')
        }
        self.config['api_keys'] = self.api_keys

        self.logger.info(f"✅ 成功加载配置文件: {self.config_file}")
        self.logger.info("📝 当前配置已通过统一 settings 解析")

        self.stock_strategies = ['均线策略', '布林带策略', '动量MACD策略', '突破策略', 'SAR策略', '均值回归策略', 'RSI策略', 'KDJ策略', '威廉指标策略', 'ADX策略', '线性回归策略', 'K线形态策略', '神经网络多层感知回归策略']

        self.logger.info("Web版股票分析器初始化完成（支持AI流式输出）")
        self.streaming = None
        self._log_config_status()

        current_dir = os.path.dirname(__file__)
        parent_dir = os.path.dirname(current_dir)
        self.analyzer_path = os.path.join(parent_dir, 'cache/analyzer_result')
        self.select_path = os.path.join(parent_dir, 'cache/selector_result')


    def _load_config(self):
        return self.settings.as_service_config()

    def _get_default_config(self):
        return self.settings.as_service_config()

    def _save_config(self, config):
        self.logger.info("跳过自动写入配置文件，使用统一 settings 配置")
        return None

    def _log_config_status(self):
        """记录配置状态"""
        self.logger.info("=== Web版系统配置状态（支持AI流式输出）===")

        # 检查API密钥状态
        available_apis = []
        for api_name, api_key in self.api_keys.items():
            if api_name != 'notes' and api_key and api_key.strip():
                available_apis.append(api_name)

        if available_apis:
            self.logger.info(f"🤖 可用AI API: {', '.join(available_apis)}")
            primary = self.config.get('ai', {}).get('model_preference', 'openai')
            self.logger.info(f"🎯 主要API: {primary}")
            self.logger.info(f"🌊 AI流式输出: 支持")

            # 显示自定义配置
            api_base = self.config.get('ai', {}).get('api_base_urls', {}).get('openai')
            if api_base and api_base != 'https://api.openai.com/v1':
                self.logger.info(f"🔗 自定义API地址: {api_base}")
        else:
            self.logger.warning("⚠️ 未配置任何AI API密钥")

        # 检查Web鉴权配置
        web_auth = self.config.get('web_auth', {})
        if web_auth.get('enabled', False):
            self.logger.info(f"🔐 Web鉴权: 已启用")
        else:
            self.logger.info(f"🔓 Web鉴权: 未启用")

        self.logger.info("=" * 40)
        if self.streaming is not  None:
            self.streaming.send_log("🚀 系统已启动", 'header')

    def stock_select_process(self, strategy_code, market):
        file_utils = None
        try:
            type = int(strategy_code)
            scanner = TopStockScanner(max_workers=20, market=market, strategy_type=type)  # 已提升至20线程
            file_utils = scanner.file_utils
            self.logger.info(f"开始全盘扫描股票{market}_{type}……")
            self.streaming.send_log(f"\n开始全盘扫描股票{market}_{type}……")
            high_score_stocks = scanner.scan_high_score_stocks(batch_size=20, type=type, strategy_filter='avg')
            self.streaming.send_log(f"\n全盘扫描股票{market}_{type}完成……")
            self.streaming.send_progress('singleProgress', 95, "全盘扫描股票...")

            if not high_score_stocks:
                self.streaming.send_log("\n未找到得分大于等于85分的股票。")
                high_score_stocks_text = "未找到得分大于等于85分的股票。"
            else:
                # 将列表转换为 DataFrame 再转换为 markdown
                if isinstance(high_score_stocks, list):
                    high_score_stocks_text = pd.DataFrame(high_score_stocks).to_markdown()
                else:
                    high_score_stocks_text = high_score_stocks.to_markdown()

            file_utils.save_results_by_price(high_score_stocks)
            # df_high_score_stocks, stats = scanner.backtest_stocks(high_score_stocks, '2025-06-06')
            # file_utils.create_middle_file('回测结果', df_high_score_stocks)
            # file_utils.create_text_file('回测结果_统计', stats)
            #  self.streaming(f"\n回测结果：{stats}")
            self.logger.info(f"\n分析完成！结果已保存至 scanner 文件夹中：")
            self.logger.info("1. 按价格区间保存的详细分析文件（price_XX_YY.txt）")
            self.logger.info("2. 汇总报告（summary.txt）")
            self.logger.info("\n" + "=" * 80)
            summary = file_utils.read_text_file('summary.txt')
            all_results = file_utils.read_text_file('temp_results.txt')
            json_result = {
                'success': True,
                'high_score_text': high_score_stocks_text,
                'summary_text': summary,
                'all_results': all_results,
                'message': f'股票 {market}_{type} 分析完成'
            }
            self.streaming.send_select_result(json_result)

            return json_result
        except Exception as e:
            file_utils.save_error_log(e)
            self.logger.error("错误日志已保存至 scanner/error_log.txt")
            json_result = {
                'success': False,
                'data': f'{str(e)}',
                'message': f'股票 {market}_{type} 分析出错'
            }
            self.streaming.send_error(json_result)
            return json_result
    def stock_analysis_process_test(self, stock_code, market, start_date_str, end_date_str):

        selected_strategies = self.stock_strategies

        system_prompt = self.system_prompt
        message_format = self.message_format

        ai_platform = self.ai_platform
        ai_model = self.ai_model
        api_code = self.api_code

        try:

            self.streaming.send_log(f"🚀 开始技术指标分析: {stock_code}", 'header')
            score, df_summary_data = self.get_stock_technical_analysis(stock_code, market)
            if isinstance(df_summary_data, dict):
                df_summary_data = pd.DataFrame.from_dict(df_summary_data, orient='index')

            tec_data_markdown = df_summary_data.to_markdown(index=True)

            self.streaming.send_log(f"🚀 完成技术指标分析: {stock_code}", 'header')
            self.streaming.send_progress('singleProgress', 20, "完成技术指标分析...")

            image_paths, strategies_selected, stock_summary, stock_analysis_result, annual_report_analysis, sentiment_analysis, sentiment_score = '', '', '', '', '#old', '#old-2', 0
            self.streaming.send_scores({
                'technical': score,
                'fundamental': 50,
                'sentiment': sentiment_score,
                'comprehensive': (score + sentiment_score) / 2
            })
            json_result = {
                'success': True,
                'tec_score': score,
                'sentiment_score': sentiment_score,
                'image_paths': image_paths,
                'stock_summary': stock_summary,
                'stock_analysis_result': stock_analysis_result,
                'annual_report_analysis': annual_report_analysis,
                'sentiment_analysis': sentiment_analysis,
                'tec_data_analysis': tec_data_markdown,
            }
            self.streaming.send_final_result(json_result)
            return json_result
        except Exception as e:
            self.logger.error(f"分析股票 {stock_code} 时出错: {e}")
            json_result = {
                "success": False,
                "error": f"{str(e)}",
                "message": "服务器内部错误"
            }
            self.streaming.send_error(str(e))
            return json_result
    def stock_analysis_process(self,stock_code, market, start_date_str, end_date_str):

        selected_strategies = self.stock_strategies
        system_prompt = self.system_prompt
        message_format = self.message_format
        ai_platform = self.ai_platform
        ai_model = self.ai_model
        api_code = self.api_code

        callbacks = {}
        if self.streaming is not None:
            callbacks = {
                'send_log': self.streaming.send_log,
                'send_progress': self.streaming.send_progress,
            }

        json_result = analyze_single_stock_use_case.execute(
            stock_code=stock_code,
            market=market,
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            selected_strategies=selected_strategies,
            system_prompt=system_prompt,
            message_format=message_format,
            ai_platform=ai_platform,
            ai_model=ai_model,
            api_code=api_code,
            callbacks=callbacks,
        )

        if json_result.get('success'):
            self.streaming.send_scores({
                'technical': json_result['tec_score'],
                'fundamental': 50,
                'sentiment': json_result['sentiment_score'],
                'comprehensive': (json_result['tec_score'] + json_result['sentiment_score']) / 2
            })
            self.streaming.send_final_result(json_result)
            return json_result

        self.logger.error(f"分析股票 {stock_code} 时出错: {json_result.get('error')}")
        self.streaming.send_error(json_result.get('error'))
        return json_result

    def get_stock_analysis(self,stock_code, market, start_date_str, end_date_str,
                           selected_strategies, system_prompt, message_format,
                           ai_platform, ai_model, api_code):
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
        if message_format is None or message_format == 'None':
            message_format = prompt_template
        # 转换日期格式
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')
        except ValueError:
            raise ValueError("日期格式不正确，请使用 YYYY-MM-DD 格式。")

        if self.streaming is not None:
            self.streaming.send_log(f"🚀 开始技术指标图形绘制: {stock_code}", 'header')
            self.streaming.send_progress('singleProgress', 10, "开始技术指标图形绘制...")
        sq = stockIndicatorQuantitative()
        # 获取股票数据
        stock_data = sq.stock_day_data_code(stock_code, market, start_date_str, end_date_str)

        if self.streaming is not None:
            self.streaming.send_log(f"🚀 股票历史成交数据获取完成 : {stock_code}", 'header')
            self.streaming.send_progress('singleProgress', 20, "股票历史成交数据获取完成...")

        if stock_data is None or stock_data.empty:
            print("stock_data is null")
            raise ValueError('stock_data is null。', 'error')  # 确保 flash 函数正确调用

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

        if self.streaming is not None:
            self.streaming.send_log(f"🚀 技术指标图形绘制完成 : {stock_code}", 'header')
            self.streaming.send_progress('singleProgress', 30, "技术指标图形绘制完成...")

        sentiment_analysis = StockSentimentAnalysis()
        sentiment_score, sentiment_analysis = sentiment_analysis.get_sentiment_analysis()
        sentiment_analysis = f'Score:{sentiment_score}\n {sentiment_analysis}'

        if self.streaming is not None:
            self.streaming.send_log(f"🚀 股票情绪据获取完成 : {stock_code}", 'header')
            self.streaming.send_progress('singleProgress', 20, "股票情绪据获取完成...")
        # "公司基本面分析结果内容"
        stock_analysis = StockAiAnalyzer(system_prompt=system_prompt,
                                         prompt_template=message_format, ai_platform=ai_platform,
                                         model=ai_model, api_token=api_code)

        stock_report_analysis = StockAiAnalyzer(system_prompt=system_prompt,
                                                prompt_template=message_format, ai_platform=ai_platform,
                                                model=ai_model, api_token=api_code)

        if self.streaming is not None:
            self.streaming.send_log(f"🚀 股票AI分析开始 : {stock_code}", 'header')
        # 创建线程池执行器
        with ThreadPoolExecutor(max_workers=3) as executor:
            # 提交任务到线程池

            future_analysis = executor.submit(stock_analysis.stock_indicator_analyse, market=market, symbol=stock_code,
                                              start_date=start_date_str, end_date=end_date_str)

            future_report = executor.submit(stock_report_analysis.stock_report_analyse, market=market,
                                            symbol=stock_code)
            future_summary = executor.submit(stock_analysis.get_stock_summary, market=market, symbol=stock_code)

            # 等待所有任务完成并获取结果
            stock_analysis_result = future_analysis.result()
            annual_report_analysis = future_report.result()
            stock_summary = future_summary.result()

            if self.streaming is not None:
                self.streaming.send_log(f"🚀 股票AI分析完成 : {stock_code}", 'header')
                self.streaming.send_progress('singleProgress', 80, "股票AI分析完成...")

        return image_paths, strategies_selected, stock_summary, stock_analysis_result, annual_report_analysis, sentiment_analysis,sentiment_score


    def get_stock_technical_analysis(self, stock_code, market):
        stock_border = stockBorderInfo(market=market)
        df_stock = stock_border.get_stock_spot()
        df_stock = df_stock[df_stock['股票代码'] == stock_code]
        df_stock['market'] =  market

        stock_analyzer = StockAnalyzer(market=market)
        df_summary_data = stock_analyzer.analyze_stock(df_stock, market)

        score = df_summary_data['score']
        return score,df_summary_data

    def find_history_strategy_analysis(self, strategy_name,date_str,market):
        """
        读取目录中包含指定过滤字符串的所有文件，并返回其内容

        参数:
            dir_path: 目录路径
            filter_str: 用于过滤文件名的字符串

        返回:
            字典，键为文件名，值为文件内容
        """
        # 检查目录是否存在
        dir_path = self.select_path
        if not os.path.exists(dir_path):
            raise FileNotFoundError(f"目录不存在: {dir_path}")

        if not os.path.isdir(dir_path):
            raise NotADirectoryError(f"{dir_path} 不是一个目录")

        # 存储结果的字典
        full_dir_path = ''
        # 遍历目录中的所有文件
        for root, dirs, files in os.walk(dir_path):
            # 只处理子目录列表dirs
            for dir_name in dirs:
                # 检查目录名是否符合条件
                if (strategy_name in dir_name and
                        date_str in dir_name and
                        market in dir_name and
                        'analyse'  in dir_name):
                    # 拼接完整的目录路径
                    full_dir_path = os.path.join(root, dir_name)

        report_high_score =  ''
        report_all = ''
        report_summary = ''

        if full_dir_path != '' and  os.path.exists(full_dir_path):
            file_high_score = os.path.join(full_dir_path, 'results_high_score.txt')
            file_all = os.path.join(full_dir_path, 'results_all.txt')
            file_summary = os.path.join(full_dir_path, 'summary.txt')
            if os.path.isfile(file_high_score):
                with open(file_high_score, 'r', encoding='utf-8') as file:
                    content = file.read()
                    report_high_score = content
            if os.path.isfile(file_all):
                with open(file_all, 'r', encoding='utf-8') as file:
                    content = file.read()
                    report_all = content
            if os.path.isfile(file_summary):
                with open(file_summary, 'r', encoding='utf-8') as file:
                    content = file.read()
                    report_summary = content


        return report_high_score,report_all,report_summary

    def find_history_stock_analysis(self, stock_code, date_str):
        """
        读取目录中包含指定过滤字符串的所有文件，并返回其内容

        参数:
            dir_path: 目录路径
            filter_str: 用于过滤文件名的字符串

        返回:
            字典，键为文件名，值为文件内容
        """
        # 检查目录是否存在
        dir_path = self.analyzer_path
        if not os.path.exists(dir_path):
            raise FileNotFoundError(f"目录不存在: {dir_path}")

        if not os.path.isdir(dir_path):
            raise NotADirectoryError(f"{dir_path} 不是一个目录")

        # 存储结果的字典
        result = {}
        report_technical_file = ''
        report_financial_file = ''
        report_technical_request_file = ''

        report_technical = ''
        report_financial = ''
        report_technical_request = ''

        # 遍历目录中的所有文件
        for filename in os.listdir(dir_path):
            # 检查文件名是否包含过滤字符串
            if stock_code in filename:
                file_path = os.path.join(dir_path, filename)
                # 确保是文件而不是子目录
                if os.path.isfile(file_path):
                    try:
                        # 读取文件内容
                        if (stock_code in filename and date_str in filename):
                            if 'indicator' in filename and 'request' not in filename:
                                report_technical_file = file_path
                            if 'indicator' in filename and 'request' in filename:
                                report_technical_request_file = file_path
                            if 'report' in filename:
                                report_financial_file = file_path

                    except Exception as e:
                        print(f"读取文件 {filename} 时出错: {str(e)}")

        if report_technical_file != '' and os.path.isfile(report_technical_file):
            with open(report_technical_file, 'r', encoding='utf-8') as file:
                content = file.read()
                report_technical = content

        if report_financial_file != '' and os.path.isfile(report_financial_file):
            with open(report_financial_file, 'r', encoding='utf-8') as file:
                content = file.read()
                report_financial = content

        if report_technical_request_file != '' and os.path.isfile(report_technical_request_file):
            with open(report_technical_request_file, 'r', encoding='utf-8') as file:
                content = file.read()
                report_technical_request = content

        return report_technical, report_financial, report_technical_request

    def query_select_history(self, strategy_name, market,date_str):
        """
           遍历目录生成包含目录信息的DataFrame，并返回其markdown格式

           参数:
               strategy_name: 可选，策略名称，用于筛选
               market: 可选，市场标识，用于筛选

           返回:
               str: DataFrame的markdown格式字符串
           """
        # 检查目录是否存在
        dir_path = self.select_path
        if not os.path.exists(dir_path):
            raise FileNotFoundError(f"目录不存在: {dir_path}")

        if not os.path.isdir(dir_path):
            raise NotADirectoryError(f"{dir_path} 不是一个目录")

        # 存储结果的列表
        results = []

        # 遍历目录中的所有子目录
        for item in os.listdir(dir_path):
            item_path = os.path.join(dir_path, item)
            # 只处理目录
            if os.path.isdir(item_path):
                # 解析目录名
                parts = item.split('_')

                # 检查目录名格式是否正确
                if len(parts) >= 5 and parts[1] == 'analyse':
                    try:
                        # 提取各字段信息
                        market_info = parts[0]
                        strategy_name_info = parts[2]
                        time_info = parts[4]
                        time_info = time_info.rstrip(".txt")

                        # 如果提供了筛选条件，则只添加符合条件的记录

                        # 构建URL
                        url = f"/api/history/select?strategy={strategy_name_info}&market={market_info}&date={time_info}"

                        # 添加到结果列表
                        results.append({
                            '目录名': item,
                            'market': market_info,
                            '策略名': strategy_name_info,
                            '时间': time_info,
                            'URL':  f'[链接]({url})'
                        })
                    except IndexError:
                        # 处理格式不符合预期的目录名
                        print(f"警告: 目录名格式不符合预期 - {item}")
                else:
                    # 格式不符合的目录名，跳过处理
                    print(f"警告: 目录名格式不符合预期 - {item}")

        # 创建DataFrame
        df = pd.DataFrame(results)
        if strategy_name and len(strategy_name.strip()) > 0:
            # 使用str.contains()实现模糊匹配（包含关系）
            df = df[df['策略名'].str.contains(strategy_name, na=False)]
        if market != None and len(market.strip()) > 0:
            df = df[df['market'] == market]
        if date_str and len(date_str.strip()) > 0:
            # 使用str.contains()实现模糊匹配（包含关系）
            df = df[df['时间'].str.contains(date_str, na=False)]

        # 返回DataFrame的markdown格式
        text = df.to_markdown(index=True)
        json_result = {
            'success': True,
            'result': text
        }
        self.streaming.send_history_result(json_result)
        return json_result

    def query_analysis_history(self, stock_code, market,date_str):

        """
            遍历目录下的文件，解析文件名信息并生成DataFrame

            参数:
                stock_code: 股票代码，用于筛选
                market: 市场标识，用于筛选

            返回:
                str: DataFrame的markdown格式字符串
            """
        # 检查目录是否存在
        dir_path = self.analyzer_path
        if not os.path.exists(dir_path):
            raise FileNotFoundError(f"目录不存在: {dir_path}")

        if not os.path.isdir(dir_path):
            raise NotADirectoryError(f"{dir_path} 不是一个目录")

        # 存储结果的列表
        results = []

        # 遍历目录中的所有文件
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            # 只处理文件
            if os.path.isfile(file_path):
                if 'request' in filename:
                    continue
                # 解析文件名
                parts = filename.split('_')

                # 检查文件名格式是否正确（至少需要5个部分）
                if len(parts) >= 5:
                    try:
                        # 提取各字段信息
                        stock_name = parts[0]
                        indicator = parts[1]
                        market_info = parts[2]
                        model_name = parts[3]
                        analysis_time = parts[-1]  # 最后一个部分是分析时间
                        analysis_time = analysis_time.rstrip(".txt")

                        # 应用筛选条件

                        # 构建URL
                        url = f"/api/history/analyse?stock={stock_name}&market={market_info}&date={analysis_time}"
                        # 添加到结果列表
                        results.append({
                            '文件名': filename,
                            '股票名称': stock_name,
                            'indicator': indicator,
                            '市场': market_info,
                            'model_name': model_name,
                            '分析时间': analysis_time,
                            'URL': f'[链接]({url})'
                        })
                    except IndexError:
                        # 处理格式不符合预期的文件名
                        print(f"警告: 文件名格式不符合预期 - {filename}")
                else:
                    # 格式不符合的文件名，跳过处理
                    print(f"警告: 文件名格式不符合预期 - {filename}")

        # 创建DataFrame
        df = pd.DataFrame(results)

        if stock_code and len(stock_code.strip()) > 0:
            df = df[df['股票名称'].str.contains(stock_code, na=False)]
        if date_str and len(date_str.strip()) > 0:
            df = df[df['分析时间'].str.contains(date_str, na=False)]
        if market != None and len(market.strip()) > 0:
            df = df[df['市场'] == market]
        # 返回DataFrame的markdown格式
        text = df.to_markdown(index=True)
        json_result = {
            'success': True,
            'result': text
        }
        self.streaming.send_history_result(json_result)
        return json_result
