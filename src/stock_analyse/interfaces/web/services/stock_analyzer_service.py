"""
Web版增强股票分析系统 - 支持AI流式输出
基于最新 stock_analyzer.py 修正版本，新增AI流式返回功能
"""

import os
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

from stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer
from stock_analyse.application.orchestrators.stock_analysis_orchestrator import StockAnalysisOrchestrator
from stock_analyse.application.use_cases import analyze_single_stock as analyze_single_stock_use_case
from stock_analyse.application.use_cases import analyze_single_stock_ai as analyze_single_stock_ai_use_case
from stock_analyse.application.use_cases import find_history_stock_analysis as find_history_stock_analysis_use_case
from stock_analyse.application.use_cases import find_history_strategy_analysis as find_history_strategy_analysis_use_case
from stock_analyse.application.use_cases import query_analysis_history as query_analysis_history_use_case
from stock_analyse.application.use_cases import query_select_history as query_select_history_use_case
from stock_analyse.application.use_cases import run_stock_selection as run_stock_selection_use_case
from stock_analyse.infrastructure.config.settings import get_settings, DEFAULT_PROMPT_TEMPLATE, DEFAULT_SYSTEM_PROMPT
from stock_analyse.interfaces.web.streaming.streaming_analyzer import StreamingAnalyzer
from stock_analyse.interfaces.web.services.trading_decision_service import TradingDecisionService
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo
from stock_analyse.application.services.quantitative_analysis_service import stockIndicatorQuantitative
from stock_analyse.domain.services.sentiment_analysis import StockSentimentAnalysis

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)


class StockAnalyzerService:
    """Web版增强股票分析器（基于最新 stock_analyzer.py 修正，支持AI流式输出）"""

    @staticmethod
    def _require_string_param(value, field_name: str) -> str:
        if not isinstance(value, str):
            raise ValueError(f'{field_name} 必须是字符串，当前类型: {type(value).__name__}')
        normalized = value.strip()
        if not normalized:
            raise ValueError(f'{field_name} 不能为空')
        return normalized

    @classmethod
    def _normalize_market_param(cls, value: str) -> str:
        market = cls._require_string_param(value, 'market')
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

    @classmethod
    def _normalize_analysis_identity(cls, stock_code, market):
        normalized_stock_code = cls._require_string_param(stock_code, 'stock_code')
        normalized_market = cls._normalize_market_param(market)
        return normalized_stock_code, normalized_market

    def _send_streaming_error(self, message: str) -> None:
        if self.streaming is not None:
            self.streaming.send_error(message)

    def _normalize_analysis_identity_or_fail(self, stock_code, market):
        try:
            return self._normalize_analysis_identity(stock_code, market)
        except ValueError as exc:
            self.logger.error(str(exc))
            self._send_streaming_error(str(exc))
            return None

    def _coerce_market(self, market):
        return self._normalize_market_param(market)

    def _coerce_stock_code(self, stock_code):
        return self._require_string_param(stock_code, 'stock_code')

    def _coerce_identity(self, stock_code, market):
        return self._normalize_analysis_identity(stock_code, market)

    def _coerce_identity_or_error(self, stock_code, market):
        return self._normalize_analysis_identity_or_fail(stock_code, market)

    def __init__(self, config_file='config.json'):
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
            'api_key': ai_config.get("api_key", ""),
            'api_base_urls': ai_config.get('api_base_urls', {}),
        }
        self.message_format = ai_config.get("prompt_template", DEFAULT_PROMPT_TEMPLATE)
        self.system_prompt = ai_config.get("system_prompt", DEFAULT_SYSTEM_PROMPT)
        self.ai_platform = self.ai_config.get("model_plat", "qwen")
        self.ai_model = self.ai_config.get("model_name", "qwen-turbo-2025-07-15")
        self.api_code = self.ai_config.get("api_key", "")
        self.api_keys = self.config.get('api_keys', {})
        self.ai_base_urls = self.ai_config.get('api_base_urls', {})

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
        self.stock_analysis_orchestrator = StockAnalysisOrchestrator()

        current_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(current_dir, '../../../../..'))
        self.analyzer_path = os.path.join(project_root, 'cache/analyzer_result')
        self.select_path = os.path.join(project_root, 'cache/selector_result')

    def _load_config(self):
        return self.settings.as_service_config()

    def _get_default_config(self):
        return self.settings.as_service_config()

    def _save_config(self, config):
        self.logger.info("跳过自动写入配置文件，使用统一 settings 配置")
        return None

    def _log_config_status(self):
        self.logger.info("=== Web版系统配置状态（支持AI流式输出）===")
        masked_api_key = self.settings.mask_secret(self.api_code)
        resolved_base_url = self.ai_base_urls.get(self.ai_platform) or self.ai_base_urls.get('openai', '')
        self.logger.info(
            f"🤖 AI配置: platform={self.ai_platform}, model={self.ai_model}, max_tokens={self.ai_config.get('max_tokens')}, temperature={self.ai_config.get('temperature')}, base_url={resolved_base_url or 'default'}, api_key={masked_api_key or 'empty'}"
        )
        available_apis = []
        for api_name, api_key in self.api_keys.items():
            if api_name != 'notes' and api_key and api_key.strip():
                available_apis.append(api_name)

        if available_apis:
            self.logger.info(f"🤖 可用AI API: {', '.join(available_apis)}")
            primary = self.config.get('ai', {}).get('model_preference', 'openai')
            self.logger.info(f"🎯 主要API: {primary}")
            self.logger.info(f"🌊 AI流式输出: 支持")
            api_base = self.config.get('ai', {}).get('api_base_urls', {}).get('openai')
            if api_base and api_base != 'https://api.openai.com/v1':
                self.logger.info(f"🔗 自定义API地址: {api_base}")
        else:
            self.logger.warning("⚠️ 未配置任何AI API密钥")

        web_auth = self.config.get('web_auth', {})
        if web_auth.get('enabled', False):
            self.logger.info(f"🔐 Web鉴权: 已启用")
        else:
            self.logger.info(f"🔓 Web鉴权: 未启用")

        self.logger.info("=" * 40)
        if self.streaming is not None:
            self.streaming.send_log("🚀 系统已启动", 'header')

    def stock_select_process(self, strategy_code, market):
        try:
            strategy_type = int(strategy_code)
            self.logger.info(f"开始全盘扫描股票{market}_{strategy_type}……")
            self.streaming.send_log(f"\n开始全盘扫描股票{market}_{strategy_type}……")
            json_result = run_stock_selection_use_case.execute(market=market, strategy_code=strategy_code)
            self.streaming.send_log(f"\n全盘扫描股票{market}_{strategy_type}完成……")
            self.streaming.send_progress('singleProgress', 95, "全盘扫描股票...")
            if json_result.get('high_score_text') == '未找到得分大于等于85分的股票。':
                self.streaming.send_log("\n未找到得分大于等于85分的股票。")
            self.logger.info(f"\n分析完成！结果已保存至 scanner 文件夹中：")
            self.logger.info("1. 按价格区间保存的详细分析文件（price_XX_YY.txt）")
            self.logger.info("2. 汇总报告（summary.txt）")
            self.logger.info("\n" + "=" * 80)
            self.streaming.send_select_result(json_result)
            return json_result
        except Exception as e:
            self.logger.error("错误日志已保存至 scanner/error_log.txt")
            json_result = {
                'success': False,
                'data': f'{str(e)}',
                'message': f'股票 {market}_{strategy_type} 分析出错'
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

    def stock_analysis_process(self, stock_code, market, start_date_str, end_date_str):
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

    def stock_ai_analysis_process(
        self,
        stock_code,
        market,
        start_date_str,
        end_date_str,
        trade_date=None,
        analysis_depth='standard',
        watch_stock_id=None,
        stock_name=None,
        holding_stock_id=None,
        analysis_scene=None,
    ):
        normalized_identity = self._normalize_analysis_identity_or_fail(stock_code, market)
        if normalized_identity is None:
            return {'success': False, 'error': 'stock_code 或 market 参数无效'}
        stock_code, market = normalized_identity

        callbacks = {}
        if self.streaming is not None:
            callbacks = {
                'send_log': self.streaming.send_log,
                'send_progress': self.streaming.send_progress,
            }

        self.logger.info(
            f"开始AI个股分析: stock={stock_code}, market={market}, provider={self.ai_platform}, model={self.ai_model}, max_tokens={self.ai_config.get('max_tokens')}, temperature={self.ai_config.get('temperature')}, base_url={self.ai_base_urls.get(self.ai_platform) or self.ai_base_urls.get('openai', 'default')}"
        )

        json_result = analyze_single_stock_ai_use_case.execute(
            stock_code=stock_code,
            market=market,
            trade_date=trade_date,
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            analysis_depth=analysis_depth,
            include_technical=True,
            include_sentiment=True,
            llm_provider=self.ai_platform,
            llm_model=self.ai_model,
            api_code=self.api_code,
            system_prompt=self.system_prompt,
            callbacks=callbacks,
            analysis_scene=analysis_scene,
        )

        if json_result.get('success'):
            scores = json_result.get('data', {}).get('scores', {})
            trading_decision_service = TradingDecisionService()
            watch_stock_context = None
            if watch_stock_id:
                watch_stock_context = {
                    'id': str(watch_stock_id).strip(),
                    'stock_code': stock_code,
                    'stock_name': str(stock_name or (json_result.get('data', {}) or {}).get('stock_name') or '').strip(),
                    'market': (json_result.get('data', {}) or {}).get('market') or market,
                }
            cache_scene = 'holding_reanalysis' if str(analysis_scene or '').strip() == 'holding_reanalysis' else 'stock_analysis'
            trading_decision_service.save_result_markdown_cache(cache_scene, json_result, watch_stock_context)
            try:
                if str(analysis_scene or '').strip() == 'holding_reanalysis' or str(holding_stock_id or '').strip():
                    trading_decision_service.save_stock_analysis_record(
                        {
                            'holding_stock_id': str(holding_stock_id or '').strip(),
                            'analysis_scene': 'holding_reanalysis',
                            'trade_date': trade_date or '',
                            'raw_result': json_result,
                        }
                    )
                elif watch_stock_id:
                    trading_decision_service.save_stock_analysis_record(
                        {
                            'watch_stock_id': str(watch_stock_id).strip(),
                            'trade_date': trade_date or '',
                            'raw_result': json_result,
                        }
                    )
            except Exception as exc:
                self.logger.error(f"保存股票分析历史记录失败: {exc}")
            if self.streaming is not None:
                self.streaming.send_scores({
                    'technical': scores.get('technical', 0),
                    'fundamental': 50,
                    'sentiment': scores.get('sentiment', 0),
                    'comprehensive': scores.get('composite', 0),
                })
                self.streaming.send_final_result(json_result)
            return json_result

        self.logger.error(f"AI个股分析 {stock_code} 出错: {json_result.get('error')}")
        if self.streaming is not None:
            self.streaming.send_error(json_result.get('error'))
        return json_result

    def get_stock_analysis(self, stock_code, market, start_date_str, end_date_str,
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
        stock_data = sq.stock_day_data_code(stock_code, market, start_date_str, end_date_str)

        if self.streaming is not None:
            self.streaming.send_log(f"🚀 股票历史成交数据获取完成 : {stock_code}", 'header')
            self.streaming.send_progress('singleProgress', 20, "股票历史成交数据获取完成...")

        if stock_data is None or stock_data.empty:
            print("stock_data is null")
            raise ValueError('stock_data is null。', 'error')

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
                plt.clf()
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
        stock_analysis = StockAiAnalyzer(system_prompt=system_prompt,
                                         prompt_template=message_format, ai_platform=ai_platform,
                                         model=ai_model, api_token=api_code)

        stock_report_analysis = StockAiAnalyzer(system_prompt=system_prompt,
                                                prompt_template=message_format, ai_platform=ai_platform,
                                                model=ai_model, api_token=api_code)

        if self.streaming is not None:
            self.streaming.send_log(f"🚀 股票AI分析开始 : {stock_code}", 'header')
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_analysis = executor.submit(stock_analysis.stock_indicator_analyse, market=market, symbol=stock_code,
                                              start_date=start_date_str, end_date=end_date_str)

            future_report = executor.submit(stock_report_analysis.stock_report_analyse, market=market,
                                            symbol=stock_code)
            future_summary = executor.submit(stock_analysis.get_stock_summary, market=market, symbol=stock_code)

            stock_analysis_result = future_analysis.result()
            annual_report_analysis = future_report.result()
            stock_summary = future_summary.result()

            if self.streaming is not None:
                self.streaming.send_log(f"🚀 股票AI分析完成 : {stock_code}", 'header')
                self.streaming.send_progress('singleProgress', 80, "股票AI分析完成...")

        return image_paths, strategies_selected, stock_summary, stock_analysis_result, annual_report_analysis, sentiment_analysis, sentiment_score

    def get_stock_technical_analysis(self, stock_code, market):
        return self.stock_analysis_orchestrator.get_stock_technical_analysis(stock_code, market)

    def find_history_strategy_analysis(self, strategy_name, date_str, market):
        return find_history_strategy_analysis_use_case.execute(
            select_path=self.select_path,
            strategy_name=strategy_name,
            date_str=date_str,
            market=market,
        )

    def find_history_stock_analysis(self, stock_code, date_str):
        return find_history_stock_analysis_use_case.execute(
            analyzer_path=self.analyzer_path,
            stock_code=stock_code,
            date_str=date_str,
        )

    def query_select_history(self, strategy_name, market, date_str):
        json_result = query_select_history_use_case.execute(
            select_path=self.select_path,
            strategy_name=strategy_name,
            market=market,
            date_str=date_str,
        )
        self.streaming.send_history_result(json_result)
        return json_result

    def query_analysis_history(self, stock_code, market, date_str):
        json_result = query_analysis_history_use_case.execute(
            analyzer_path=self.analyzer_path,
            stock_code=stock_code,
            market=market,
            date_str=date_str,
        )
        self.streaming.send_history_result(json_result)
        return json_result
