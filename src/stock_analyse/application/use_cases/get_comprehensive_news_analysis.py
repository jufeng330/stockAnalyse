from __future__ import annotations

from stock_analyse.application.use_cases import analyze_sentiment, get_stock_news
from stock_analyse.domain.services.sentiment_analysis import StockSentimentAnalysis



def execute(market: str, symbol: str, days: int = 15, gateway: StockSentimentAnalysis | None = None) -> dict:
    try:
        gateway = gateway or StockSentimentAnalysis(market=market, symbol=symbol)
        news_result = get_stock_news.execute(market=market, symbol=symbol, days=days, gateway=gateway)
        sentiment_result = analyze_sentiment.execute(market=market, symbol=symbol, days=days, gateway=gateway)

        data = {
            "symbol": symbol,
            "market": market,
            "news": news_result.get("data", {}),
            "sentiment": sentiment_result.get("data", {}),
            "suggestions": [],
        }

        if sentiment_result.get("success"):
            score = sentiment_result["data"].get("sentiment_score", 50)
            if score > 70:
                data["suggestions"].append("市场情绪积极，新闻面利好")
            elif score < 30:
                data["suggestions"].append("市场情绪消极，注意风险")
            else:
                data["suggestions"].append("市场情绪中性")

        return {
            "success": True,
            "data": data,
            "message": "综合分析完成",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"分析失败: {exc}"}
