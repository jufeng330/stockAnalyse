#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stock Sector Skill - 股票板块分析 Skill
提供概念板块查询、行业板块分析、成分股查询、板块关联分析等板块分析功能
"""

from stocklib import stockConceptData, stockConcepService, stockCompanyInfo


def get_stock_sectors(market: str, symbol: str, date: str = '20241231') -> dict:
    """
    获取股票所属板块

    Args:
        market: 市场代码 (SH/SZ/H/usa)
        symbol: 股票代码
        date: 查询日期

    Returns:
        dict: 股票所属板块信息
    """
    try:
        stock = stockCompanyInfo(market=market, symbol=symbol)
        stock_name = stock.get_stock_name()

        # 获取所属行业
        industry = stock.get_stock_industry_by_code(symbol, date)

        # 获取所属概念
        concepts = stock.get_stock_concept_by_code(symbol, date)

        return {
            "股票代码": symbol,
            "股票名称": stock_name,
            "所属行业": industry,
            "所属概念": concepts
        }
    except Exception as e:
        return {"error": str(e)}


def get_all_concepts() -> dict:
    """
    获取所有概念板块

    Returns:
        dict: 概念板块列表
    """
    try:
        concept_data = stockConceptData()
        df_concepts = concept_data.stock_board_concept_name_ths()

        return {
            "概念板块数量": len(df_concepts),
            "概念板块": df_concepts.to_dict('records') if df_concepts is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_all_industries() -> dict:
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


def get_concept_stocks(concept_name: str) -> dict:
    """
    获取概念成分股

    Args:
        concept_name: 概念名称

    Returns:
        dict: 概念成分股列表
    """
    try:
        stock = stockCompanyInfo(market='SH', symbol='000001')
        df_industries = stock.get_stock_board_all_industry_name()
        df_concept_stocks = stock.get_stock_concept_by_name(
            concept_name=concept_name,
            industry_sectors=df_industries
        )

        return {
            "概念名称": concept_name,
            "成分股数量": len(df_concept_stocks),
            "成分股": df_concept_stocks.to_dict('records') if df_concept_stocks is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_industry_stocks(industry_name: str) -> dict:
    """
    获取行业成分股

    Args:
        industry_name: 行业名称

    Returns:
        dict: 行业成分股列表
    """
    try:
        stock = stockCompanyInfo(market='SH', symbol='000001')
        df_industries = stock.get_stock_board_all_industry_name()
        df_industry_stocks = stock.get_stock_industry_by_name(
            concept_name=industry_name,
            industry_sectors=df_industries
        )

        return {
            "行业名称": industry_name,
            "成分股数量": len(df_industry_stocks),
            "成分股": df_industry_stocks.to_dict('records') if df_industry_stocks is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_all_sectors_and_stocks(market: str) -> dict:
    """
    获取所有板块和成分股

    Args:
        market: 市场代码

    Returns:
        dict: 板块和成分股数据
    """
    try:
        sector_service = stockConcepService(
            max_workers=20,
            min_score=60,
            market=market
        )

        concept_sectors, industry_sectors = sector_service.get_all_sectors_and_stocks()

        # 统计成分股数量
        concept_stock_count = sum(len(stocks) for stocks in concept_sectors.values())
        industry_stock_count = sum(len(stocks) for stocks in industry_sectors.values())

        # 查看股票最多的概念板块
        sorted_concepts = sorted(concept_sectors.items(), key=lambda x: len(x[1]), reverse=True)

        # 查看股票最多的行业板块
        sorted_industries = sorted(industry_sectors.items(), key=lambda x: len(x[1]), reverse=True)

        return {
            "市场": market,
            "概念板块数量": len(concept_sectors),
            "行业板块数量": len(industry_sectors),
            "概念板块成分股总数": concept_stock_count,
            "行业板块成分股总数": industry_stock_count,
            "概念板块示例": sorted_concepts[:5],
            "行业板块示例": sorted_industries[:5]
        }
    except Exception as e:
        return {"error": str(e)}


def find_multi_concept_stocks(hot_concepts: list) -> dict:
    """
    查找跨多个热门概念的股票

    Args:
        hot_concepts: 热门概念列表

    Returns:
        dict: 跨概念股票列表
    """
    try:
        stock = stockCompanyInfo(market='SH', symbol='000001')
        df_industries = stock.get_stock_board_all_industry_name()

        # 获取各概念的成分股
        concept_stocks_map = {}
        for concept in hot_concepts:
            try:
                df_stocks = stock.get_stock_concept_by_name(
                    concept_name=concept,
                    industry_sectors=df_industries
                )
                if df_stocks is not None and len(df_stocks) > 0:
                    stocks = df_stocks['股票代码'].tolist() if '股票代码' in df_stocks.columns else []
                    concept_stocks_map[concept] = set(stocks)
            except Exception:
                continue

        # 统计每个股票出现在多少个概念中
        stock_concept_count = {}
        for concept, stocks in concept_stocks_map.items():
            for s in stocks:
                if s not in stock_concept_count:
                    stock_concept_count[s] = []
                stock_concept_count[s].append(concept)

        # 筛选出出现在多个概念的股票
        multi_concept_stocks = {s: concepts for s, concepts in stock_concept_count.items()
                                 if len(concepts) >= 2}

        # 按概念数量排序
        sorted_stocks = sorted(multi_concept_stocks.items(), key=lambda x: len(x[1]), reverse=True)

        return {
            "热门概念": hot_concepts,
            "跨概念股票数量": len(multi_concept_stocks),
            "跨概念股票": [
                {"股票代码": code, "所属概念": concepts}
                for code, concepts in sorted_stocks[:20]
            ]
        }
    except Exception as e:
        return {"error": str(e)}


def get_concept_history(symbol: str, start_year: int = 2024) -> dict:
    """
    获取概念历史数据

    Args:
        symbol: 概念代码
        start_year: 起始年份

    Returns:
        dict: 概念历史数据
    """
    try:
        concept_data = stockConceptData()
        df_hist = concept_data.stock_board_concept_hist_ths(
            start_year=start_year,
            symbol=symbol
        )

        # 计算涨跌幅
        if df_hist is not None and len(df_hist) > 0:
            df_hist['涨跌幅%'] = df_hist['收盘'].pct_change() * 100

            # 计算累计涨跌幅
            total_change = (df_hist['收盘'].iloc[-1] / df_hist['收盘'].iloc[0] - 1) * 100

            return {
                "概念代码": symbol,
                "数据条数": len(df_hist),
                "累计涨跌幅": total_change,
                "最新指数": df_hist['收盘'].iloc[-1],
                "年初指数": df_hist['收盘'].iloc[0],
                "历史数据": df_hist.to_dict('records')
            }
        else:
            return {"error": "无历史数据"}
    except Exception as e:
        return {"error": str(e)}


def analyze_sector_concentration(industry_name: str) -> dict:
    """
    分析板块集中度

    Args:
        industry_name: 行业名称

    Returns:
        dict: 板块集中度分析结果
    """
    try:
        stock = stockCompanyInfo(market='SH', symbol='000001')
        df_industries = stock.get_stock_board_all_industry_name()
        df_industry_stocks = stock.get_stock_industry_by_name(
            concept_name=industry_name,
            industry_sectors=df_industries
        )

        if df_industry_stocks is None or len(df_industry_stocks) == 0:
            return {"error": "无成分股数据"}

        # 分析市值分布（如果有市值数据）
        if '总市值' in df_industry_stocks.columns:
            total_market_cap = df_industry_stocks['总市值'].sum()
            top_market_cap = df_industry_stocks.nlargest(5, '总市值')

            top_stocks = []
            for idx, row in top_market_cap.iterrows():
                market_cap_ratio = row['总市值'] / total_market_cap * 100
                top_stocks.append({
                    "股票代码": row.get('股票代码', ''),
                    "股票名称": row.get('股票名称', ''),
                    "总市值": row['总市值'] / 100000000,
                    "占比": market_cap_ratio
                })

            return {
                "行业名称": industry_name,
                "成分股数量": len(df_industry_stocks),
                "行业总市值": total_market_cap / 100000000,
                "市值最大的5只股票": top_stocks
            }
        else:
            return {
                "行业名称": industry_name,
                "成分股数量": len(df_industry_stocks),
                "提示": "无市值数据"
            }
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    # 测试代码
    print("=== Stock Sector Skill 测试 ===")

    # 测试获取股票所属板块
    print("\n1. 获取股票所属板块:")
    sectors = get_stock_sectors('SH', '601318')
    if 'error' not in sectors:
        print(f"股票名称: {sectors['股票名称']}")
        print(f"所属行业: {sectors['所属行业']}")
        print(f"所属概念: {sectors['所属概念']}")
    else:
        print(f"错误: {sectors['error']}")

    # 测试获取所有概念板块
    print("\n2. 获取所有概念板块:")
    concepts = get_all_concepts()
    if 'error' not in concepts:
        print(f"概念板块数量: {concepts['概念板块数量']}")
    else:
        print(f"错误: {concepts['error']}")

    # 测试获取概念成分股
    print("\n3. 获取概念成分股:")
    concept_stocks = get_concept_stocks('人工智能')
    if 'error' not in concept_stocks:
        print(f"成分股数量: {concept_stocks['成分股数量']}")
    else:
        print(f"错误: {concept_stocks['error']}")

    # 测试获取所有板块和成分股
    print("\n4. 获取所有板块和成分股:")
    all_sectors = get_all_sectors_and_stocks('SH')
    if 'error' not in all_sectors:
        print(f"概念板块数量: {all_sectors['概念板块数量']}")
        print(f"行业板块数量: {all_sectors['行业板块数量']}")
    else:
        print(f"错误: {all_sectors['error']}")

    # 测试查找跨概念股票
    print("\n5. 查找跨概念股票:")
    multi = find_multi_concept_stocks(['人工智能', '芯片', '新能源'])
    if 'error' not in multi:
        print(f"跨概念股票数量: {multi['跨概念股票数量']}")
    else:
        print(f"错误: {multi['error']}")
