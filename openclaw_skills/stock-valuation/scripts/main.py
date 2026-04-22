#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Valuation Skill - 股票估值模型
"""
import sys
import json
import argparse

sys.path.insert(0, '/home/inspur/codes/stockAnalyse')

from stocklib.dcf_model import stockDCFSimpleModel
from stocklib.stock_annual_report import stockAnnualReport
from stocklib.stock_company import stockCompanyInfo
from stocklib.stock_border import stockBorderInfo


def get_current_price(market: str, symbol: str) -> float:
    """获取当前股价"""
    try:
        border = stockBorderInfo(market=market)
        df = border.get_stock_spot()

        if df is None or df.empty:
            return -1

        # 查找对应股票
        stock_row = df[df['股票代码'] == symbol]
        if stock_row.empty:
            return -1

        price = stock_row.iloc[0].get('最新价', -1)
        return float(price) if price else -1
    except:
        return -1


def calculate_dcf(market: str, symbol: str, discount_rate: float = 0.1, growth_rate: float = 0.03) -> dict:
    """计算 DCF 估值"""
    try:
        # 获取三大报表
        report = stockAnnualReport()
        zcfz, lrb, xjll = report.get_stock_report(
            stock_code=symbol,
            market=market,
            years=5
        )

        if zcfz is None or lrb is None or xjll is None:
            return {"success": False, "data": {}, "message": "无法获取财务报表"}

        # 计算 DCF
        dcf = stockDCFSimpleModel(market=market)
        dcf_value = dcf.calculate_dcf(xjll, discount_rate=discount_rate, growth_rate=growth_rate)

        # 获取总股本
        total_shares = zcfz['资产-总股本'].iloc[0] if '资产-总股本' in zcfz.columns else 0

        # 计算每股价值
        if total_shares > 0:
            value_per_share = dcf_value / total_shares
        else:
            value_per_share = 0

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "dcf_value": round(dcf_value, 2),
                "total_shares": round(total_shares, 2),
                "value_per_share": round(value_per_share, 2),
                "discount_rate": discount_rate,
                "growth_rate": growth_rate
            },
            "message": f"DCF估值: {round(value_per_share, 2)} 元/股"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


def calculate_price_range(market: str, symbol: str) -> dict:
    """计算股价区间"""
    try:
        # 获取三大报表
        report = stockAnnualReport()
        zcfz, lrb, xjll = report.get_stock_report(
            stock_code=symbol,
            market=market,
            years=5
        )

        if zcfz is None or lrb is None or xjll is None:
            return {"success": False, "data": {}, "message": "无法获取财务报表"}

        # 计算股价区间
        dcf = stockDCFSimpleModel(market=market)
        result_df = dcf.calculate_stock_price_range(zcfz, lrb, xjll)

        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}

        latest = result_df.iloc[0]

        price_range = {
            "conservative": round(latest.get('dcf_lower_stock_price', 0), 2),
            "normal": round(latest.get('dcf_normal_stock_price', 0), 2),
            "optimistic": round(latest.get('dcf_upper_stock_price', 0), 2)
        }

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "price_range": price_range,
                "midpoint": round((price_range['conservative'] + price_range['optimistic']) / 2, 2)
            },
            "message": f"股价区间: {price_range['conservative']} - {price_range['optimistic']} 元"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


def compare_valuation(market: str, symbol: str) -> dict:
    """估值比较分析"""
    try:
        # 获取股价区间
        range_result = calculate_price_range(market, symbol)
        if not range_result['success']:
            return range_result

        # 获取当前价格
        current_price = get_current_price(market, symbol)

        price_range = range_result['data']['price_range']
        normal_price = price_range['normal']

        # 计算安全边际
        if current_price > 0 and normal_price > 0:
            margin = (normal_price - current_price) / current_price * 100
        else:
            margin = 0

        # 判断估值状态
        if current_price < price_range['conservative']:
            status = "严重低估"
            suggestion = "当前价格低于保守估值，具备较高安全边际"
        elif current_price < normal_price:
            status = "轻度低估"
            suggestion = "当前价格低于正常估值，具备一定安全边际"
        elif current_price < price_range['optimistic']:
            status = "合理估值"
            suggestion = "当前价格在合理区间内"
        else:
            status = "高估"
            suggestion = "当前价格高于乐观估值，注意风险"

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "current_price": current_price if current_price > 0 else None,
                "price_range": price_range,
                "margin_of_safety": f"{round(margin, 2)}%",
                "status": status,
                "suggestion": suggestion
            },
            "message": f"估值状态: {status}, 安全边际: {round(margin, 2)}%"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"分析失败: {str(e)}"}


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
