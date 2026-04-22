#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stock Financial Skill - 股票财务分析 Skill
提供财务报表获取、财务指标分析、DCF估值计算、业绩报表等财务分析功能
"""

from stocklib import stockAnnualReport, stockCompanyInfo, stockDCFSimpleModel


def get_financial_reports(market: str, symbol: str, years: int = 5) -> dict:
    """
    获取三大报表（资产负债表、利润表、现金流量表）

    Args:
        market: 市场代码 (SH/SZ/H/usa)
        symbol: 股票代码
        years: 获取最近几年的数据

    Returns:
        dict: 包含三大报表的字典
    """
    try:
        stock = stockCompanyInfo(market=market, symbol=symbol)
        zcfz, lrb, xjll = stock.get_stock_report(indicator='20240331', years=years)

        return {
            "股票代码": symbol,
            "资产负债表": zcfz.to_dict('records') if zcfz is not None else [],
            "利润表": lrb.to_dict('records') if lrb is not None else [],
            "现金流量表": xjll.to_dict('records') if xjll is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_financial_indicators(market: str, symbol: str, start_year: int = 2020) -> dict:
    """
    获取财务分析指标

    Args:
        market: 市场代码
        symbol: 股票代码
        start_year: 起始年份

    Returns:
        dict: 财务指标数据
    """
    try:
        stock = stockCompanyInfo(market=market, symbol=symbol)
        df_indicators = stock.get_stock_zycwzb()
        df_analysis = stock.get_stock_financial_analysis_indicator(start_year=start_year)

        return {
            "股票代码": symbol,
            "主要财务指标": df_indicators.to_dict('records') if df_indicators is not None else [],
            "财务分析指标": df_analysis.to_dict('records') if df_analysis is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def calculate_dcf_valuation(market: str, symbol: str, discount_rate: float = 0.1,
                           growth_rate: float = 0.03) -> dict:
    """
    计算DCF估值

    Args:
        market: 市场代码
        symbol: 股票代码
        discount_rate: 折现率 (默认10%)
        growth_rate: 永续增长率 (默认3%)

    Returns:
        dict: DCF估值结果
    """
    try:
        stock = stockCompanyInfo(market=market, symbol=symbol)
        zcfz, lrb, xjll = stock.get_stock_report(indicator='20240331', years=5)

        dcf = stockDCFSimpleModel(market=market)
        df_price_range = dcf.calculate_stock_price_range(zcfz, lrb, xjll)

        return {
            "股票代码": symbol,
            "折现率": discount_rate,
            "永续增长率": growth_rate,
            "估值结果": df_price_range.to_dict('records') if df_price_range is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_performance_reports(market: str, symbol: str) -> dict:
    """
    获取业绩报表、业绩快报、业绩预告

    Args:
        market: 市场代码
        symbol: 股票代码

    Returns:
        dict: 业绩数据
    """
    try:
        stock = stockCompanyInfo(market=market, symbol=symbol)

        df_yjbb = stock.get_stock_yjbb()
        df_yjkb = stock.get_stock_yjkb()
        df_yjyg = stock.get_stock_yjyg()

        return {
            "股票代码": symbol,
            "业绩报表": df_yjbb.to_dict('records') if df_yjbb is not None else [],
            "业绩快报": df_yjkb.to_dict('records') if df_yjkb is not None else [],
            "业绩预告": df_yjyg.to_dict('records') if df_yjyg is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_business_composition(market: str, symbol: str) -> dict:
    """
    获取主营构成

    Args:
        market: 市场代码
        symbol: 股票代码

    Returns:
        dict: 主营构成数据
    """
    try:
        annual_report = stockAnnualReport()
        df_business = annual_report.get_stock_zygc(stock_code=symbol, market=market)

        return {
            "股票代码": symbol,
            "主营构成": df_business.to_dict('records') if df_business is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def analyze_profitability(market: str, symbol: str, start_year: int = 2020) -> dict:
    """
    分析盈利能力

    Args:
        market: 市场代码
        symbol: 股票代码
        start_year: 起始年份

    Returns:
        dict: 盈利能力分析结果
    """
    try:
        stock = stockCompanyInfo(market=market, symbol=symbol)
        df_analysis = stock.get_stock_financial_analysis_indicator(start_year=start_year)

        if df_analysis is None or len(df_analysis) == 0:
            return {"error": "无财务分析数据"}

        # 提取盈利能力指标
        profitability = {}

        if '净资产收益率(%)' in df_analysis.columns:
            roe_series = df_analysis['净资产收益率(%)']
            profitability['平均ROE'] = roe_series.mean()
            profitability['最新ROE'] = roe_series.iloc[0]

        if '毛利率(%)' in df_analysis.columns:
            gross_series = df_analysis['毛利率(%)']
            profitability['平均毛利率'] = gross_series.mean()
            profitability['最新毛利率'] = gross_series.iloc[0]

        if '净利率(%)' in df_analysis.columns:
            net_series = df_analysis['净利率(%)']
            profitability['平均净利率'] = net_series.mean()
            profitability['最新净利率'] = net_series.iloc[0]

        return {
            "股票代码": symbol,
            "盈利能力分析": profitability
        }
    except Exception as e:
        return {"error": str(e)}


def analyze_growth(market: str, symbol: str, start_year: int = 2020) -> dict:
    """
    分析成长能力

    Args:
        market: 市场代码
        symbol: 股票代码
        start_year: 起始年份

    Returns:
        dict: 成长能力分析结果
    """
    try:
        stock = stockCompanyInfo(market=market, symbol=symbol)
        df_analysis = stock.get_stock_financial_analysis_indicator(start_year=start_year)

        if df_analysis is None or len(df_analysis) == 0:
            return {"error": "无财务分析数据"}

        growth = {}

        if '营业收入同比增长率(%)' in df_analysis.columns:
            revenue_series = df_analysis['营业收入同比增长率(%)']
            growth['平均营收增长率'] = revenue_series.mean()
            growth['最新营收增长率'] = revenue_series.iloc[0]

        if '净利润同比增长率(%)' in df_analysis.columns:
            profit_series = df_analysis['净利润同比增长率(%)']
            growth['平均净利润增长率'] = profit_series.mean()
            growth['最新净利润增长率'] = profit_series.iloc[0]

        return {
            "股票代码": symbol,
            "成长能力分析": growth
        }
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    # 测试代码
    print("=== Stock Financial Skill 测试 ===")

    # 测试获取三大报表
    print("\n1. 获取三大报表:")
    reports = get_financial_reports('SH', '601318', years=5)
    if 'error' not in reports:
        print(f"资产负债表条数: {len(reports['资产负债表'])}")
        print(f"利润表条数: {len(reports['利润表'])}")
        print(f"现金流量表条数: {len(reports['现金流量表'])}")
    else:
        print(f"错误: {reports['error']}")

    # 测试获取财务指标
    print("\n2. 获取财务指标:")
    indicators = get_financial_indicators('SH', '601318', start_year=2020)
    if 'error' not in indicators:
        print(f"主要财务指标条数: {len(indicators['主要财务指标'])}")
        print(f"财务分析指标条数: {len(indicators['财务分析指标'])}")
    else:
        print(f"错误: {indicators['error']}")

    # 测试DCF估值
    print("\n3. 计算DCF估值:")
    dcf = calculate_dcf_valuation('SH', '601318')
    if 'error' not in dcf:
        print(f"估值结果条数: {len(dcf['估值结果'])}")
    else:
        print(f"错误: {dcf['error']}")

    # 测试盈利能力分析
    print("\n4. 盈利能力分析:")
    profit = analyze_profitability('SH', '601318', start_year=2020)
    if 'error' not in profit:
        print("盈利能力指标:")
        for k, v in profit['盈利能力分析'].items():
            print(f"  {k}: {v:.2f}%")
    else:
        print(f"错误: {profit['error']}")
