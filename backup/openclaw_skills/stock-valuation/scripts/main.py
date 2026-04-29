#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Valuation Skill - 股票估值模型
"""
import sys
import json
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from stock_analyse.application.use_cases import calculate_dcf as calculate_dcf_use_case
from stock_analyse.application.use_cases import compare_valuation as compare_valuation_use_case
from stock_analyse.application.use_cases import get_price_range as get_price_range_use_case


def calculate_dcf(market: str, symbol: str, discount_rate: float = 0.1, growth_rate: float = 0.03) -> dict:
    """计算 DCF 估值"""
    return calculate_dcf_use_case.execute(
        market=market,
        symbol=symbol,
        discount_rate=discount_rate,
        growth_rate=growth_rate,
    )


def calculate_price_range(market: str, symbol: str) -> dict:
    """计算股价区间"""
    return get_price_range_use_case.execute(market=market, symbol=symbol)


def compare_valuation(market: str, symbol: str) -> dict:
    """估值比较分析"""
    return compare_valuation_use_case.execute(market=market, symbol=symbol)


def main():
    parser = argparse.ArgumentParser(description='Stock Valuation Skill')
    parser.add_argument('--action', type=str, required=True,
                        choices=['dcf', 'price_range', 'compare'],
                        help='操作类型')
    parser.add_argument('--market', type=str, required=True,
                        choices=['SH', 'SZ', 'H', 'usa'],
                        help='市场代码')
    parser.add_argument('--symbol', type=str, required=True, help='股票代码')
    parser.add_argument('--discount_rate', type=float, default=0.1, help='折现率')
    parser.add_argument('--growth_rate', type=float, default=0.03, help='永续增长率')

    args = parser.parse_args()

    # 路由到对应处理函数
    if args.action == 'dcf':
        result = calculate_dcf(args.market, args.symbol, args.discount_rate, args.growth_rate)
    elif args.action == 'price_range':
        result = calculate_price_range(args.market, args.symbol)
    elif args.action == 'compare':
        result = compare_valuation(args.market, args.symbol)
    else:
        result = {"success": False, "data": {}, "message": "未知的 action"}

    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
