#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Data Skill - 股票基础数据获取
"""
import sys
import json
import argparse
from pathlib import Path
# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from stock_analyse.application.use_cases import (
    get_dividend as get_dividend_use_case,
    get_financial_indicator as get_financial_indicator_use_case,
    get_fund_flow as get_fund_flow_use_case,
    get_holders as get_holders_use_case,
    get_market_spot as get_market_spot_use_case,
    get_stock_history as get_stock_history_use_case,
    get_stock_info as get_stock_info_use_case,
    get_stock_report as get_stock_report_use_case,
)


def get_stock_info(market: str, symbol: str) -> dict:
    """获取个股基本信息"""
    return get_stock_info_use_case.execute(market=market, symbol=symbol)


def get_stock_history(market: str, symbol: str, start_date: str = None, end_date: str = None) -> dict:
    """获取历史行情数据"""
    return get_stock_history_use_case.execute(
        market=market,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )


def get_market_spot(market: str) -> dict:
    """获取市场实时行情"""
    return get_market_spot_use_case.execute(market=market)


def get_stock_report(market: str, symbol: str, years: int = 5) -> dict:
    """获取三大财务报表"""
    return get_stock_report_use_case.execute(market=market, symbol=symbol, years=years)


def get_financial_indicator(market: str, symbol: str, start_year: str = None) -> dict:
    """获取财务分析指标"""
    return get_financial_indicator_use_case.execute(market=market, symbol=symbol, start_year=start_year)


def get_dividend(market: str, date: str = None) -> dict:
    """获取分红配送信息"""
    return get_dividend_use_case.execute(market=market, date=date)


def get_fund_flow(market: str, symbol: str = None) -> dict:
    """获取资金流向数据"""
    return get_fund_flow_use_case.execute(market=market, symbol=symbol)


def get_holders(market: str, symbol: str) -> dict:
    """获取股东信息"""
    return get_holders_use_case.execute(market=market, symbol=symbol)


def main():
    parser = argparse.ArgumentParser(description='Stock Data Skill')
    parser.add_argument('--action', type=str, required=True,
                        choices=['info', 'history', 'spot', 'report', 'financial', 'dividend', 'fund_flow', 'holders'],
                        help='操作类型')
    parser.add_argument('--market', type=str, required=True,
                        choices=['SH', 'SZ', 'H', 'usa', 'zq'],
                        help='市场代码')
    parser.add_argument('--symbol', type=str, help='股票代码')
    parser.add_argument('--start_date', type=str, help='开始日期(YYYYMMDD)')
    parser.add_argument('--end_date', type=str, help='结束日期(YYYYMMDD)')
    parser.add_argument('--years', type=int, default=5, help='历史年份数')
    parser.add_argument('--date', type=str, help='报告日期')

    args = parser.parse_args()

    # 参数校验
    if args.action != 'spot' and args.action != 'dividend' and not args.symbol:
        print(json.dumps({"success": False, "data": {}, "message": "缺少 symbol 参数"}, ensure_ascii=False))
        sys.exit(1)

    # 路由到对应处理函数
    if args.action == 'info':
        result = get_stock_info(args.market, args.symbol)
    elif args.action == 'history':
        result = get_stock_history(args.market, args.symbol, args.start_date, args.end_date)
    elif args.action == 'spot':
        result = get_market_spot(args.market)
    elif args.action == 'report':
        result = get_stock_report(args.market, args.symbol, args.years)
    elif args.action == 'financial':
        result = get_financial_indicator(args.market, args.symbol)
    elif args.action == 'dividend':
        result = get_dividend(args.market, args.date)
    elif args.action == 'fund_flow':
        result = get_fund_flow(args.market, args.symbol)
    elif args.action == 'holders':
        result = get_holders(args.market, args.symbol)
    else:
        result = {"success": False, "data": {}, "message": "未知的 action"}

    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
