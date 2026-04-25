from __future__ import annotations

from stock_analyse.domain.services.sentiment_analysis import StockSentimentAnalysis


DEFAULT_SENTIMENT_SCORE = 50.0


def execute(market: str, symbol: str, days: int = 15, gateway: StockSentimentAnalysis | None = None) -> dict:
    try:
        gateway = gateway or StockSentimentAnalysis(market=market, symbol=symbol)
        comprehensive_news_data = gateway.get_comprehensive_news_data(symbol, days=days)
        sentiment_data = gateway.calculate_advanced_sentiment_analysis(comprehensive_news_data)
        sentiment_score = gateway.calculate_sentiment_score(sentiment_data)

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "sentiment_score": round(sentiment_score, 2),
                "trend": sentiment_data.get("sentiment_trend", "未知"),
                "confidence": sentiment_data.get("confidence_score", 0),
                "total_analyzed": sentiment_data.get("total_analyzed", 0),
                "positive_ratio": sentiment_data.get("positive_ratio", 0),
                "negative_ratio": sentiment_data.get("negative_ratio", 0),
                "analysis": sentiment_data,
            },
            "message": f"情绪得分: {round(sentiment_score, 2)}, 趋势: {sentiment_data.get('sentiment_trend', '未知')}",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"分析失败: {exc}"}
