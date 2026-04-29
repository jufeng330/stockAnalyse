#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Concept Skill - 股票概念板块
"""

import sys
import json
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from stock_analyse.application.use_cases import get_sector_components as get_sector_components_use_case
from stock_analyse.application.use_cases import get_sector_detail as get_sector_detail_use_case
from stock_analyse.application.use_cases import get_stock_sectors as get_stock_sectors_use_case
from stock_analyse.application.use_cases import list_concepts as list_concepts_use_case
from stock_analyse.application.use_cases import list_industries as list_industries_use_case


def list_concepts(market: str) -> dict:
    """获取概念板块列表"""
    return list_concepts_use_case.execute(market=market)


def list_industries(market: str) -> dict:
    """获取行业板块列表"""
    return list_industries_use_case.execute(market=market)


def get_components(market: str, name: str) -> dict:
    """获取板块成分股"""
    return get_sector_components_use_case.execute(market=market, name=name)


def get_by_stock(market: str, symbol: str) -> dict:
    """查询股票所属板块"""
    return get_stock_sectors_use_case.execute(market=market, symbol=symbol)


def get_detail(market: str, name: str) -> dict:
    """获取板块详情"""
    return get_sector_detail_use_case.execute(market=market, name=name)


def main():
    parser = argparse.ArgumentParser(description='Stock Concept Skill')
    parser.add_argument('--action', type=str, required=True,
                        choices=['list_concept', 'list_industry', 'components', 'by_stock', 'detail'],
                        help='操作类型')
    parser.add_argument('--market', type=str, required=True,
                        choices=['SH', 'SZ', 'H', 'usa', 'zq'],
                        help='市场代码')
    parser.add_argument('--name', type=str, help='板块名称')
    parser.add_argument('--symbol', type=str, help='股票代码')

    args = parser.parse_args()

    # 参数校验
    if args.action in ['components', 'detail'] and not args.name:
        print(json.dumps({"success": False, "data": {}, "message": "缺少 name 参数"}, ensure_ascii=False))
        sys.exit(1)

    if args.action == 'by_stock' and not args.symbol:
        print(json.dumps({"success": False, "data": {}, "message": "缺少 symbol 参数"}, ensure_ascii=False))
        sys.exit(1)

    # 路由到对应处理函数
    if args.action == 'list_concept':
        result = list_concepts(args.market)
    elif args.action == 'list_industry':
        result = list_industries(args.market)
    elif args.action == 'components':
        result = get_components(args.market, args.name)
    elif args.action == 'by_stock':
        result = get_by_stock(args.market, args.symbol)
    elif args.action == 'detail':
        result = get_detail(args.market, args.name)
    else:
        result = {"success": False, "data": {}, "message": "未知的 action"}

    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
