"""LLM 客户端薄封装。

负责把传统分析场景需要的摘要、指标分析和财报分析请求转发到统一 AI 访问入口。
"""

from __future__ import annotations

from stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer


class StockAiClient:
    """统一 AI 分析客户端。

    负责为传统分析流程提供稳定的单项能力调用入口，屏蔽底层模型初始化细节。
    """

    def __init__(self, system_prompt: str, prompt_template: str, ai_platform: str, model: str, api_token: str) -> None:
        """根据传入配置创建统一的 StockAiAnalyzer 实例。"""
        self.analyzer = StockAiAnalyzer(
            system_prompt=system_prompt,
            prompt_template=prompt_template,
            ai_platform=ai_platform,
            model=model,
            api_token=api_token,
        )

    def analyze_indicator(self, market: str, symbol: str, start_date: str, end_date: str) -> str:
        return self.analyzer.stock_indicator_analyse(market=market, symbol=symbol, start_date=start_date, end_date=end_date)

    def analyze_report(self, market: str, symbol: str) -> str:
        return self.analyzer.stock_report_analyse(market=market, symbol=symbol)

    def get_summary(self, market: str, symbol: str) -> str:
        return self.analyzer.get_stock_summary(market=market, symbol=symbol)
