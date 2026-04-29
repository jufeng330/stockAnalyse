#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Strategy Skill - 股票策略评分
"""
import sys
import json
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from stock_analyse.application.use_cases import select_stocks as select_stocks_use_case


def calculate_score(market: str, symbol: str) -> dict:
    return select_stocks_use_case.calculate_score(market=market, symbol=symbol)


def get_signals(market: str, symbol: str) -> dict:
    return select_stocks_use_case.get_signals(market=market, symbol=symbol)


def get_recommendation(market: str, symbol: str) -> dict:
    return select_stocks_use_case.get_recommendation(market=market, symbol=symbol)


def batch_analyze(market: str, min_score: int = 30) -> dict:
    return select_stocks_use_case.batch_analyze(market=market, min_score=min_score)


def main():
    parser = argparse.ArgumentParser(description='Stock Strategy Skill')
    parser.add_argument('--action', type=str, required=True,
                        choices=['score', 'signals', 'profitable', 'recommend', 'batch'],
                        help='操作类型')
    parser.add_argument('--market', type=str, required=True,
                        choices=['SH', 'SZ', 'H', 'usa', 'zq'],
                        help='市场代码')
    parser.add_argument('--symbol', type=str, help='股票代码')
    parser.add_argument('--date', type=str, help='报告日期(YYYYMMDD)')
    parser.add_argument('--min_score', type=int, default=30, help='最低评分阈值')

    args = parser.parse_args()

    if args.action != 'batch' and not args.symbol:
        print(json.dumps({"success": False, "data": {}, "message": "缺少 symbol 参数"}, ensure_ascii=False))
        sys.exit(1)

    if args.action == 'score':
        result = calculate_score(args.market, args.symbol)
    elif args.action == 'signals':
        result = get_signals(args.market, args.symbol)
    elif args.action == 'recommend':
        result = get_recommendation(args.market, args.symbol)
    elif args.action == 'batch':
        result = batch_analyze(args.market, args.min_score)
    else:
        result = {"success": False, "data": {}, "message": "功能开发中"}

    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
