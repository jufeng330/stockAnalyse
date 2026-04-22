#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock News Skill - 股票新闻情绪
"""
import sys
import json
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from stock_analyse.application.use_cases import analyze_sentiment as analyze_sentiment_use_case
from stock_analyse.application.use_cases import get_comprehensive_news_analysis as get_comprehensive_news_analysis_use_case
from stock_analyse.application.use_cases import get_stock_news as get_stock_news_use_case


def get_news(market: str, symbol: str, days: int = 15) -> dict:
    """获取个股新闻"""
    return get_stock_news_use_case.execute(market=market, symbol=symbol, days=days)


def analyze_sentiment(market: str, symbol: str, days: int = 15) -> dict:
    """情绪分析"""
    return analyze_sentiment_use_case.execute(market=market, symbol=symbol, days=days)


def comprehensive_analysis(market: str, symbol: str, days: int = 15) -> dict:
    """综合分析"""
    return get_comprehensive_news_analysis_use_case.execute(market=market, symbol=symbol, days=days)


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
