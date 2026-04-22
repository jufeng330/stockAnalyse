#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock News Skill - 股票新闻情绪
"""
import sys
import json
import argparse

sys.path.insert(0, '/home/inspur/codes/stockAnalyse')

from stocklib.stock_news_data import stockNewsData
from stocklib.stock_sentiment_analysis import StockSentimentAnalysis
from stocklib.stock_company import stockCompanyInfo


def get_news(market: str, symbol: str, days: int = 15) -> dict:
    """获取个股新闻"""
    try:
        # 获取新闻
        df = stockNewsData.stock_news_em(symbol=symbol, pageSize=20)

        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无新闻数据"}

        records = df.to_dict(orient='records')

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "days": days,
                "count": len(records),
                "news": records
            },
            "message": f"获取 {len(records)} 条新闻"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def analyze_sentiment(market: str, symbol: str, days: int = 15) -> dict:
    """情绪分析"""
    try:
        analyzer = StockSentimentAnalysis(market=market, symbol=symbol)

        score, analysis = analyzer.get_sentiment_analysis()

        if isinstance(analysis, dict):
            sentiment_data = analysis
        else:
            sentiment_data = {"raw": str(analysis)}

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "sentiment_score": round(score, 2),
                "trend": sentiment_data.get('sentiment_trend', '未知'),
                "confidence": sentiment_data.get('confidence_score', 0),
                "total_analyzed": sentiment_data.get('total_analyzed', 0),
                "positive_ratio": sentiment_data.get('positive_ratio', 0),
                "negative_ratio": sentiment_data.get('negative_ratio', 0)
            },
            "message": f"情绪得分: {round(score, 2)}, 趋势: {sentiment_data.get('sentiment_trend', '未知')}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"分析失败: {str(e)}"}


def comprehensive_analysis(market: str, symbol: str, days: int = 15) -> dict:
    """综合分析"""
    try:
        # 获取新闻
        news_result = get_news(market, symbol, days)

        # 情绪分析
        sentiment_result = analyze_sentiment(market, symbol, days)

        # 整合结果
        data = {
            "symbol": symbol,
            "market": market,
            "news": news_result.get('data', {}),
            "sentiment": sentiment_result.get('data', {})
        }

        # 生成建议
        suggestions = []
        if sentiment_result.get('success'):
            score = sentiment_result['data'].get('sentiment_score', 50)
            if score > 70:
                suggestions.append("市场情绪积极，新闻面利好")
            elif score < 30:
                suggestions.append("市场情绪消极，注意风险")
            else:
                suggestions.append("市场情绪中性")

        data['suggestions'] = suggestions

        return {
            "success": True,
            "data": data,
            "message": "综合分析完成"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"分析失败: {str(e)}"}


def main():
    parser = argparse.ArgumentParser(description='Stock News Skill')
    parser.add_argument('--action', type=str, required=True,
                        choices=['news', 'sentiment', 'comprehensive'],
                        help='操作类型')
    parser.add_argument('--market', type=str, required=True,
                        choices=['SH', 'SZ', 'H', 'usa', 'zq'],
                        help='市场代码')
    parser.add_argument('--symbol', type=str, required=True, help='股票代码')
    parser.add_argument('--days', type=int, default=15, help='查询天数')

    args = parser.parse_args()

    # 路由到对应处理函数
    if args.action == 'news':
        result = get_news(args.market, args.symbol, args.days)
    elif args.action == 'sentiment':
        result = analyze_sentiment(args.market, args.symbol, args.days)
    elif args.action == 'comprehensive':
        result = comprehensive_analysis(args.market, args.symbol, args.days)
    else:
        result = {"success": False, "data": {}, "message": "未知的 action"}

    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
