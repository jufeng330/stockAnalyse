#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stock Basic Skill - 股票基础数据 Skill
提供个股信息查询、历史行情数据获取、市场全景数据、资金流向分析等基础数据服务
"""

from stocklib import stockCompanyInfo, stockBorderInfo


def get_stock_info(market: str, symbol: str) -> dict:
    """
    获取股票基本信息

    Args:
        market: 市场代码 (SH/SZ/H/usa/zq)
        symbol: 股票代码

    Returns:
        dict: 包含股票名称、基本信息等的字典
    """
    try:
        stock = stockCompanyInfo(market=market, symbol=symbol)

        stock_name = stock.get_stock_name()
        df_info = stock.get_stock_individual_info()

        # 获取所属行业和概念
        industry = stock.get_stock_industry_by_code(symbol, '20241231')
        concepts = stock.get_stock_concept_by_code(symbol, '20241231')

        return {
            "股票代码": symbol,
            "股票名称": stock_name,
            "所属行业": industry,
            "所属概念": concepts,
            "基本信息": df_info.to_dict('records') if df_info is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_stock_history(market: str, symbol: str, start_date: str, end_date: str) -> dict:
    """
    获取股票历史行情数据

    Args:
        market: 市场代码
        symbol: 股票代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)

    Returns:
        dict: 历史行情数据
    """
    try:
        stock = stockCompanyInfo(market=market, symbol=symbol)
        df_history = stock.get_stock_history_data(
            start_date_str=start_date,
            end_date_str=end_date
        )

        return {
            "股票代码": symbol,
            "数据条数": len(df_history),
            "历史数据": df_history.to_dict('records') if df_history is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_market_spot(market: str) -> dict:
    """
    获取市场实时行情

    Args:
        market: 市场代码

    Returns:
        dict: 实时行情数据
    """
    try:
        border = stockBorderInfo(market=market)
        df_spot = border.get_stock_spot()

        return {
            "市场": market,
            "股票数量": len(df_spot),
            "实时行情": df_spot.to_dict('records') if df_spot is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_all_codes(market: str) -> dict:
    """
    获取市场所有股票代码

    Args:
        market: 市场代码

    Returns:
        dict: 股票代码列表
    """
    try:
        border = stockBorderInfo(market=market)
        df_codes = border.get_stock_all_code()

        return {
            "市场": market,
            "股票数量": len(df_codes),
            "股票代码": df_codes.to_dict('records') if df_codes is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_fund_flow(market: str, symbol: str) -> dict:
    """
    获取个股资金流数据

    Args:
        market: 市场代码
        symbol: 股票代码

    Returns:
        dict: 资金流数据
    """
    try:
        stock = stockCompanyInfo(market=market, symbol=symbol)
        df_fund_flow = stock.get_stock_individual_fund_flow()

        return {
            "股票代码": symbol,
            "数据条数": len(df_fund_flow),
            "资金流数据": df_fund_flow.to_dict('records') if df_fund_flow is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_concept_boards() -> dict:
    """
    获取所有概念板块

    Returns:
        dict: 概念板块列表
    """
    try:
        stock = stockCompanyInfo(market='SH', symbol='000001')
        df_concepts = stock.get_stock_board_all_concept_name()

        return {
            "概念板块数量": len(df_concepts),
            "概念板块": df_concepts.to_dict('records') if df_concepts is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_industry_boards() -> dict:
    """
    获取所有行业板块

    Returns:
        dict: 行业板块列表
    """
    try:
        stock = stockCompanyInfo(market='SH', symbol='000001')
        df_industries = stock.get_stock_board_all_industry_name()

        return {
            "行业板块数量": len(df_industries),
            "行业板块": df_industries.to_dict('records') if df_industries is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_hsgt_holdings(market: str) -> dict:
    """
    获取北向资金持仓

    Args:
        market: 市场代码

    Returns:
        dict: 北向资金持仓数据
    """
    try:
        border = stockBorderInfo(market=market)
        df_hsgt = border.get_stock_hsgt_hold_stock_em()

        return {
            "市场": market,
            "持仓数量": len(df_hsgt),
            "北向资金持仓": df_hsgt.to_dict('records') if df_hsgt is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    # 测试代码
    print("=== Stock Basic Skill 测试 ===")

    # 测试获取股票信息
    print("\n1. 获取股票信息:")
    info = get_stock_info('SH', '601318')
    if 'error' not in info:
        print(f"股票名称: {info['股票名称']}")
        print(f"所属行业: {info['所属行业']}")
        print(f"所属概念: {info['所属概念']}")
    else:
        print(f"错误: {info['error']}")

    # 测试获取历史数据
    print("\n2. 获取历史数据:")
    history = get_stock_history('SH', '601318', '20240101', '20241231')
    if 'error' not in history:
        print(f"数据条数: {history['数据条数']}")
    else:
        print(f"错误: {history['error']}")

    # 测试获取市场实时行情
    print("\n3. 获取市场实时行情:")
    spot = get_market_spot('SH')
    if 'error' not in spot:
        print(f"股票数量: {spot['股票数量']}")
    else:
        print(f"错误: {spot['error']}")

    # 测试获取概念板块
    print("\n4. 获取概念板块:")
    concepts = get_concept_boards()
    if 'error' not in concepts:
        print(f"概念板块数量: {concepts['概念板块数量']}")
    else:
        print(f"错误: {concepts['error']}")
