#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Technical Skill - 股票技术指标计算
"""
import sys
import json
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from stock_analyse.application.use_cases import analyze_technical_indicators as analyze_technical_indicators_use_case


SUPPORTED_ACTIONS = {
    'ma', 'macd', 'rsi', 'kdj', 'bollinger', 'breakout', 'sar', 'williams', 'adx', 'all'
}


def main():
    parser = argparse.ArgumentParser(description='Stock Technical Skill')
    parser.add_argument('--action', type=str, required=True,
                        choices=['ma', 'macd', 'rsi', 'kdj', 'bollinger', 'breakout', 'sar',
                                 'mean_reversion', 'williams', 'adx', 'obv', 'all'],
                        help='指标类型')
    parser.add_argument('--market', type=str, required=True,
                        choices=['SH', 'SZ', 'H', 'usa', 'zq'],
                        help='市场代码')
    parser.add_argument('--symbol', type=str, required=True, help='股票代码')
    parser.add_argument('--start_date', type=str, help='开始日期(YYYYMMDD)')
    parser.add_argument('--end_date', type=str, help='结束日期(YYYYMMDD)')
    parser.add_argument('--params', type=str, help='指标参数(JSON格式)')

    args = parser.parse_args()

    params = None
    if args.params:
        try:
            params = json.loads(args.params)
        except Exception:
            pass

    if args.action not in SUPPORTED_ACTIONS:
        result = {"success": False, "data": {}, "message": "未知的 action"}
    else:
        result = analyze_technical_indicators_use_case.execute(
            action=args.action,
            market=args.market,
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            params=params,
        )

    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
