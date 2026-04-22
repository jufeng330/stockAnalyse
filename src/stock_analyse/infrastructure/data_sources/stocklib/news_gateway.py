from __future__ import annotations

from stocklib.stock_news_data import stockNewsData
from stocklib.stock_sentiment_analysis import StockSentimentAnalysis


class NewsGateway:
    def get_stock_news(self, symbol: str, page_size: int = 20):
        return stockNewsData.stock_news_em(symbol=symbol, pageSize=page_size)

    def get_comprehensive_news_data(self, market: str, symbol: str, days: int = 15) -> dict:
        analyzer = StockSentimentAnalysis(market=market, symbol=symbol)
        return analyzer.get_comprehensive_news_data(symbol, days=days)

    def calculate_advanced_sentiment_analysis(self, market: str, symbol: str, comprehensive_news_data: dict) -> dict:
        analyzer = StockSentimentAnalysis(market=market, symbol=symbol)
        return analyzer.calculate_advanced_sentiment_analysis(comprehensive_news_data)

    def calculate_sentiment_score(self, market: str, symbol: str, sentiment_analysis: dict) -> float:
        analyzer = StockSentimentAnalysis(market=market, symbol=symbol)
        return analyzer.calculate_sentiment_score(sentiment_analysis)
