#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Concept Skill - 股票概念板块
"""


import sys
import json
import argparse

sys.path.insert(0, '/home/inspur/codes/stockAnalyse')

from stocklib import stockConceptData
from stocklib.stock_concept_service import stockConcepService
from stocklib.stock_company import stockCompanyInfo


def list_concepts(market: str) -> dict:
    """获取概念板块列表"""
    try:
        concept_data = stockConceptData()
        df = concept_data.stock_board_concept_name_ths()

        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无数据"}

        records = df.to_dict(orient='records')

        return {
            "success": True,
            "data": {
                "market": market,
                "type": "concept",
                "count": len(records),
                "sectors": records[:50]  # 限制数量
            },
            "message": f"共 {len(records)} 个概念板块"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def list_industries(market: str) -> dict:
    """获取行业板块列表"""
    try:
        service = stockConcepService(market=market)
        _, df = service.get_all_sectors_and_stocks()

        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无数据"}

        records = df.to_dict(orient='records')

        return {
            "success": True,
            "data": {
                "market": market,
                "type": "industry",
                "count": len(records),
                "sectors": records[:50]
            },
            "message": f"共 {len(records)} 个行业板块"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def get_components(market: str, name: str) -> dict:
    """获取板块成分股"""
    try:
        concept_data = stockConceptData()

        # 获取概念板块列表
        board_df = concept_data.stock_board_concept_name_ths()

        # 获取成分股
        df = concept_data.stock_board_concept_cons_ths(symbol=name, stock_board_ths_map_df=board_df)

        if df is None or df.empty:
            # 尝试行业板块
            service = stockConcepService(market=market)
            concept_df, industry_df = service.get_all_sectors_and_stocks()

            # 这里简化处理，实际应该调用行业成分股接口
            return {"success": False, "data": {}, "message": "暂不支持行业板块成分股查询"}

        records = df.to_dict(orient='records')

        return {
            "success": True,
            "data": {
                "sector_name": name,
                "market": market,
                "type": "concept",
                "count": len(records),
                "stocks": records
            },
            "message": f"共 {len(records)} 只成分股"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def get_by_stock(market: str, symbol: str) -> dict:
    """查询股票所属板块"""
    try:
        stock = stockCompanyInfo(marker=market, symbol=symbol)

        # 获取所属概念
        concepts = stock.get_stock_concept_by_code(symbol)

        # 获取所属行业
        industry = stock.get_stock_industry_by_code(symbol)

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "concepts": concepts.split(',') if concepts else [],
                "industry": industry
            },
            "message": f"所属行业: {industry}, 概念数: {len(concepts.split(',')) if concepts else 0}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def get_detail(market: str, name: str) -> dict:
    """获取板块详情"""
    try:
        concept_data = stockConceptData()
        board_df = concept_data.stock_board_concept_name_ths()

        df = concept_data.stock_board_concept_info_ths(symbol=name, stock_board_ths_map_df=board_df)

        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无板块详情"}

        info = dict(zip(df['项目'], df['值']))

        return {
            "success": True,
            "data": {
                "sector_name": name,
                "market": market,
                "info": info
            },
            "message": "获取成功"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


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
