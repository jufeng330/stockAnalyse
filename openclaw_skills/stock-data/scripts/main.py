#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Data Skill - 股票基础数据获取
"""
import sys
import json
import argparse
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, '/home/inspur/codes/stockAnalyse')

from stocklib.stock_company import stockCompanyInfo
from stocklib.stock_border import stockBorderInfo
from stocklib.stock_annual_report import stockAnnualReport


def get_stock_info(market: str, symbol: str) -> dict:
    """获取个股基本信息"""
    try:
        stock = stockCompanyInfo(marker=market, symbol=symbol)

        # 获取基本信息
        info_df = stock.get_stock_individual_info()
        name = stock.get_stock_name()

        # 获取行业和上市日期
        _, list_date, industry = stock.get_stock_individual_info_em()

        # 获取所属板块
        concept = stock.get_stock_concept_by_code(symbol)
        border = stock.get_stock_industry_by_code(symbol)

        result = {
            "symbol": symbol,
            "name": name,
            "market": market,
            "industry": industry,
            "concept": concept,
            "sector": border,
            "list_date": list_date,
            "detail": info_df.to_dict() if info_df is not None else {}
        }

        return {"success": True, "data": result, "message": "获取成功"}
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def get_stock_history(market: str, symbol: str, start_date: str = None, end_date: str = None) -> dict:
    """获取历史行情数据"""
    try:
        stock = stockCompanyInfo(marker=market, symbol=symbol)

        # 默认获取最近一年
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        if not start_date:
            start = datetime.strptime(end_date, "%Y%m%d") - __import__('datetime').timedelta(days=120)
            start_date = start.strftime("%Y%m%d")

        df = stock.get_stock_history_data(start_date_str=start_date, end_date_str=end_date)

        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无历史数据"}

        # 转换 DataFrame 为字典列表
        records = df.to_dict(orient='records')

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "records": records,
                "count": len(records)
            },
            "message": f"获取 {len(records)} 条记录"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def get_market_spot(market: str) -> dict:
    """获取市场实时行情"""
    try:
        border = stockBorderInfo(market=market)
        df = border.get_stock_spot()

        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无市场数据"}

        # 只返回前100条，避免数据过大
        records = df.head(100).to_dict(orient='records')

        return {
            "success": True,
            "data": {
                "market": market,
                "records": records,
                "total": len(df),
                "returned": len(records)
            },
            "message": f"共 {len(df)} 只股票，返回前 {len(records)} 只"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def get_stock_report(market: str, symbol: str, years: int = 5) -> dict:
    """获取三大财务报表"""
    try:
        report = stockAnnualReport()
        zcfz, lrb, xjll = report.get_stock_report(
            stock_code=symbol,
            market=market,
            years=years
        )

        result = {
            "symbol": symbol,
            "market": market,
            "balance_sheet": zcfz.to_dict(orient='records') if zcfz is not None else [],
            "income_statement": lrb.to_dict(orient='records') if lrb is not None else [],
            "cash_flow": xjll.to_dict(orient='records') if xjll is not None else []
        }

        return {"success": True, "data": result, "message": "获取成功"}
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def get_financial_indicator(market: str, symbol: str, start_year: str = None) -> dict:
    """获取财务分析指标"""
    try:
        stock = stockCompanyInfo(marker=market, symbol=symbol)

        if not start_year:
            start_year = str(datetime.now().year - 5)

        df = stock.get_stock_financial_analysis_indicator(start_year=start_year)

        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无财务指标数据"}

        records = df.to_dict(orient='records')

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "records": records,
                "count": len(records)
            },
            "message": f"获取 {len(records)} 条记录"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def get_dividend(market: str, date: str = None) -> dict:
    """获取分红配送信息"""
    try:
        border = stockBorderInfo(market=market)

        if not date:
            from stock_analyse.stocklib.utils_report_date import ReportDateUtils
            utils = ReportDateUtils()
            date = utils.get_current_report_year_st(market=market)

        df = border.get_stock_fhps_info(date=date)

        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无分红数据"}

        records = df.head(100).to_dict(orient='records')

        return {
            "success": True,
            "data": {
                "market": market,
                "date": date,
                "records": records,
                "count": len(records)
            },
            "message": f"获取 {len(records)} 条记录"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def get_fund_flow(market: str, symbol: str = None) -> dict:
    """获取资金流向数据"""
    try:
        if symbol:
            # 个股资金流
            stock = stockCompanyInfo(marker=market, symbol=symbol)
            df = stock.get_stock_individual_fund_flow()
            target = symbol
        else:
            # 市场资金流
            border = stockBorderInfo(market=market)
            df = border.get_stock_all_info()
            target = market

        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无资金流数据"}

        records = df.head(100).to_dict(orient='records')

        return {
            "success": True,
            "data": {
                "target": target,
                "market": market,
                "records": records,
                "count": len(records)
            },
            "message": f"获取 {len(records)} 条记录"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def get_holders(market: str, symbol: str) -> dict:
    """获取股东信息"""
    try:
        stock = stockCompanyInfo(marker=market, symbol=symbol)

        # 获取十大流通股东
        df = stock.get_stock_gdzjc()

        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无股东数据"}

        records = df.to_dict(orient='records')

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "records": records,
                "count": len(records)
            },
            "message": f"获取 {len(records)} 条记录"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


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
