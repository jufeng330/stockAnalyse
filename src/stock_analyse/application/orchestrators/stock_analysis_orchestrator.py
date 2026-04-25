from __future__ import annotations

from datetime import datetime
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd

from stock_analyse.application.use_cases import analyze_sentiment
from stock_analyse.infrastructure.llm.adapter import StockAiAdapter
from stock_analyse.application.services.quantitative_analysis_service import stockIndicatorQuantitative


class StockAnalysisOrchestrator:
    def __init__(self, technical_analysis_workflow: Any | None = None) -> None:
        self._technical_analysis_workflow = technical_analysis_workflow

    def _get_technical_analysis_workflow(self):
        if self._technical_analysis_workflow is None:
            from stock_analyse.application.workflows.technical_analysis_workflow import TechnicalAnalysisWorkflow

            self._technical_analysis_workflow = TechnicalAnalysisWorkflow()
        return self._technical_analysis_workflow

    def run(self, stock_code: str, market: str, start_date_str: str, end_date_str: str, selected_strategies: list[str], system_prompt: str, message_format: str, ai_platform: str, ai_model: str, api_code: str, callbacks=None) -> dict:
        callbacks = callbacks or {}
        send_log = callbacks.get('send_log')
        send_progress = callbacks.get('send_progress')

        if message_format in {None, 'None'}:
            message_format = """
请基于以上收集到的实时的真实数据，发挥你的A股分析专业知识，对未来3天该股票的价格走势做出深度预测。
在预测中请全面考虑主营业务、基本数据、所在行业数据、所在概念板块数据、历史行情、最近新闻以及资金流动等多方面因素。
给出具体的涨跌百分比数据分析总结。
"""

        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        start_compact = start_date.strftime('%Y%m%d')
        end_compact = end_date.strftime('%Y%m%d')

        if send_log:
            send_log(f"🚀 开始技术指标图形绘制: {stock_code}", 'header')
        if send_progress:
            send_progress('singleProgress', 10, '开始技术指标图形绘制...')

        sq = stockIndicatorQuantitative()
        stock_data = sq.stock_day_data_code(stock_code, market, start_compact, end_compact)
        if stock_data is None or stock_data.empty:
            raise ValueError('stock_data is null。')

        if send_log:
            send_log(f"🚀 股票历史成交数据获取完成 : {stock_code}", 'header')
        if send_progress:
            send_progress('singleProgress', 20, '股票历史成交数据获取完成...')

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
            '神经网络多层感知回归策略': sq.strategy_mlp_regression,
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

        if send_log:
            send_log(f"🚀 技术指标图形绘制完成 : {stock_code}", 'header')
        if send_progress:
            send_progress('singleProgress', 30, '技术指标图形绘制完成...')

        technical_score, technical_df = self.get_stock_technical_analysis(stock_code, market)
        if isinstance(technical_df, dict):
            technical_df = pd.DataFrame.from_dict(technical_df, orient='index')
        tec_data_markdown = technical_df.to_markdown(index=True)

        sentiment_result = analyze_sentiment.execute(market=market, symbol=stock_code, days=15)
        sentiment_score = sentiment_result.get('data', {}).get('sentiment_score', 0)
        sentiment_analysis = f"Score:{sentiment_score}\n {sentiment_result.get('data', {}).get('analysis', {})}"

        if send_log:
            send_log(f"🚀 股票情绪据获取完成 : {stock_code}", 'header')
        if send_progress:
            send_progress('singleProgress', 40, '股票情绪据获取完成...')

        ai_adapter = StockAiAdapter(
            system_prompt=system_prompt,
            prompt_template=message_format,
            ai_platform=ai_platform,
            ai_model=ai_model,
            api_code=api_code,
        )

        if send_log:
            send_log(f"🚀 股票AI分析开始 : {stock_code}", 'header')

        stock_summary, stock_analysis_result, annual_report_analysis = ai_adapter.analyze(
            market=market,
            symbol=stock_code,
            start_date=start_compact,
            end_date=end_compact,
        )

        if send_log:
            send_log(f"🚀 股票AI分析完成 : {stock_code}", 'header')
        if send_progress:
            send_progress('singleProgress', 80, '股票AI分析完成...')

        return {
            'tec_score': technical_score,
            'sentiment_score': sentiment_score,
            'image_paths': image_paths,
            'strategies_selected': strategies_selected,
            'stock_summary': stock_summary,
            'stock_analysis_result': stock_analysis_result,
            'annual_report_analysis': annual_report_analysis,
            'sentiment_analysis': sentiment_analysis,
            'tec_data_analysis': tec_data_markdown,
        }

    def get_stock_technical_analysis(self, stock_code, market):
        return self._get_technical_analysis_workflow().run(stock_code=stock_code, market=market)
