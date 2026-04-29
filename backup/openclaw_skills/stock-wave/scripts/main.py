#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Wave Skill - 股票波浪分析
"""
import sys
import json
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from stock_analyse.application.use_cases import analyze_wave_trend as analyze_wave_trend_use_case
from stock_analyse.application.use_cases import analyze_waves as analyze_waves_use_case
from stock_analyse.application.use_cases import prepare_wave_visualization as prepare_wave_visualization_use_case


def analyze_waves(market: str, symbol: str, days: int = 200) -> dict:
    """波浪分析"""
    return analyze_waves_use_case.execute(market=market, symbol=symbol, days=days)


def analyze_trend(market: str, symbol: str, days: int = 200) -> dict:
    """趋势分析"""
    return analyze_wave_trend_use_case.execute(market=market, symbol=symbol, days=days)


def visualize(market: str, symbol: str, days: int = 200) -> dict:
    """可视化"""
    return prepare_wave_visualization_use_case.execute(market=market, symbol=symbol, days=days)


def main():
    parser = argparse.ArgumentParser(description='Stock Wave Skill')
    parser.add_argument('--action', type=str, required=True,
                        choices=['analyze', 'trend', 'visualize'],
                        help='操作类型')
    parser.add_argument('--market', type=str, required=True,
                        choices=['SH', 'SZ', 'H', 'usa', 'zq'],
                        help='市场代码')
    parser.add_argument('--symbol', type=str, required=True, help='股票代码')
    parser.add_argument('--days', type=int, default=200, help='分析天数')

    args = parser.parse_args()

    # 路由到对应处理函数
    if args.action == 'analyze':
        result = analyze_waves(args.market, args.symbol, args.days)
    elif args.action == 'trend':
        result = analyze_trend(args.market, args.symbol, args.days)
    elif args.action == 'visualize':
        result = visualize(args.market, args.symbol, args.days)
    else:
        result = {"success": False, "data": {}, "message": "未知的 action"}

    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
