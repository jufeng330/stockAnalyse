#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stock Strategy Skill - 股票交易策略 Skill
提供股票综合策略评分、买入信号生成、波浪分析、趋势判断等交易策略功能
"""

from stocklib import StockStrategy, StockWaveAnalyzer, stockCompanyInfo


def calculate_strategy_score(market: str, symbol: str) -> dict:
    """
    计算股票综合策略评分

    Args:
        market: 市场代码 (SH/SZ/H/usa)
        symbol: 股票代码

    Returns:
        dict: 综合评分和买入信号
    """
    try:
        strategy = StockStrategy(market=market)
        stock = stockCompanyInfo(market=market, symbol=symbol)

        # 获取数据
        df_history = stock.get_stock_history_data(
            start_date_str='20240101',
            end_date_str='20241231'
        )
        df_stock = stock.get_stock_individual_info()

        # 计算综合评分
        score, buy_signal = strategy.calculate_score(
            df_history_data=df_history,
            df_stock=df_stock,
            df_summary_data=None
        )

        # 获取投资建议
        recommendation = strategy.get_recommendation(score)

        return {
            "股票代码": symbol,
            "综合评分": score,
            "买入信号": buy_signal,
            "投资建议": recommendation
        }
    except Exception as e:
        return {"error": str(e)}


def analyze_stock_wave(market: str, symbol: str, days: int = 200) -> dict:
    """
    分析股票波浪

    Args:
        market: 市场代码
        symbol: 股票代码
        days: 获取历史数据天数

    Returns:
        dict: 波浪分析结果
    """
    try:
        wave = StockWaveAnalyzer(market=market, symbol=symbol)
        stock_df = wave.get_stock_data(days=days)

        # 分析波浪
        df_wave = wave.analysis_stock_wave(stock_df)

        # 分析趋势
        _, total_trend, last_trend = wave.analysis_stock_trend(stock_df)

        return {
            "股票代码": symbol,
            "数据天数": days,
            "整体趋势": total_trend,
            "最后趋势": last_trend,
            "波浪数据": df_wave.to_dict('records') if df_wave is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def analyze_stock_trend(market: str, symbol: str, days: int = 200) -> dict:
    """
    分析股票趋势

    Args:
        market: 市场代码
        symbol: 股票代码
        days: 获取历史数据天数

    Returns:
        dict: 趋势分析结果
    """
    try:
        wave = StockWaveAnalyzer(market=market, symbol=symbol)
        stock_df = wave.get_stock_data(days=days)

        # 分析趋势
        df_wave, total_trend, last_trend = wave.analysis_stock_trend(stock_df)

        # 统计波峰波谷
        if df_wave is not None and len(df_wave) > 0:
            peaks_count = len(df_wave[df_wave['类型'] == 'peak'])
            troughs_count = len(df_wave[df_wave['类型'] == 'trough'])
        else:
            peaks_count = 0
            troughs_count = 0

        return {
            "股票代码": symbol,
            "数据天数": days,
            "整体趋势": total_trend,
            "最后趋势": last_trend,
            "波峰数量": peaks_count,
            "波谷数量": troughs_count
        }
    except Exception as e:
        return {"error": str(e)}


def calculate_simple_score(market: str, symbol: str) -> dict:
    """
    计算简单评分（仅基于历史数据）

    Args:
        market: 市场代码
        symbol: 股票代码

    Returns:
        dict: 简单评分结果
    """
    try:
        strategy = StockStrategy(market=market)
        stock = stockCompanyInfo(market=market, symbol=symbol)

        df_history = stock.get_stock_history_data(
            start_date_str='20240101',
            end_date_str='20241231'
        )

        score, signal = strategy.calculate_score_simple(df_history)
        recommendation = strategy.get_recommendation(score)

        return {
            "股票代码": symbol,
            "简单评分": score,
            "信号": signal,
            "投资建议": recommendation
        }
    except Exception as e:
        return {"error": str(e)}


def multi_stock_comparison(stocks: list, market: str) -> dict:
    """
    多股票评分比较

    Args:
        stocks: 股票代码列表
        market: 市场代码

    Returns:
        dict: 多股票评分对比结果
    """
    try:
        strategy = StockStrategy(market=market)
        results = []

        for symbol in stocks:
            try:
                stock = stockCompanyInfo(market=market, symbol=symbol)
                stock_name = stock.get_stock_name()

                df_history = stock.get_stock_history_data(
                    start_date_str='20240101',
                    end_date_str='20241231'
                )
                df_stock = stock.get_stock_individual_info()

                score, buy_signal = strategy.calculate_score(
                    df_history_data=df_history,
                    df_stock=df_stock,
                    df_summary_data=None
                )

                recommendation = strategy.get_recommendation(score)

                results.append({
                    "股票代码": symbol,
                    "股票名称": stock_name,
                    "综合评分": score,
                    "买入信号": buy_signal,
                    "投资建议": recommendation
                })
            except Exception as e:
                results.append({
                    "股票代码": symbol,
                    "错误": str(e)
                })

        return {"比较结果": results}
    except Exception as e:
        return {"error": str(e)}


def get_trading_points(market: str, symbol: str, days: int = 200) -> dict:
    """
    识别关键买卖点

    Args:
        market: 市场代码
        symbol: 股票代码
        days: 获取历史数据天数

    Returns:
        dict: 关键买卖点
    """
    try:
        from stock_analyse.stocklib import stockAKIndicator
        import pandas as pd

        wave = StockWaveAnalyzer(market=market, symbol=symbol)
        indicator = stockAKIndicator()

        # 获取数据
        stock_df = wave.get_stock_data(days=days)

        # 转换为indicator需要的格式
        df_indicator = indicator.stock_day_data_code(
            stock_code=symbol,
            market=market,
            start_date_str='20240101',
            end_date_str='20241231'
        )

        # 计算技术指标
        df_mac = indicator.strategy_mac(df_indicator)
        df_macd = indicator.strategy_macd(df_indicator)

        # 分析波浪
        df_wave, total_trend, last_trend = wave.analysis_stock_trend(stock_df)

        # 查找波峰和波谷
        if df_wave is not None and len(df_wave) > 0:
            peaks = df_wave[df_wave['类型'] == 'peak'].tail(3).to_dict('records')
            troughs = df_wave[df_wave['类型'] == 'trough'].tail(3).to_dict('records')
        else:
            peaks = []
            troughs = []

        # 获取技术指标信号
        if len(df_mac) > 0:
            mac_signal = df_mac['mac_signal'].iloc[-1]
        else:
            mac_signal = 0

        if len(df_macd) > 0:
            macd_signal = df_macd['macd_signal'].iloc[-1]
        else:
            macd_signal = 0

        return {
            "股票代码": symbol,
            "整体趋势": total_trend,
            "最后趋势": last_trend,
            "波峰（潜在卖出点）": peaks,
            "波谷（潜在买入点）": troughs,
            "MA信号": mac_signal,
            "MACD信号": macd_signal
        }
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    # 测试代码
    print("=== Stock Strategy Skill 测试 ===")

    # 测试计算策略评分
    print("\n1. 计算策略评分:")
    score = calculate_strategy_score('SH', '601318')
    if 'error' not in score:
        print(f"综合评分: {score['综合评分']}")
        print(f"买入信号: {score['买入信号']}")
        print(f"投资建议: {score['投资建议']}")
    else:
        print(f"错误: {score['error']}")

    # 测试波浪分析
    print("\n2. 波浪分析:")
    wave = analyze_stock_wave('SH', '600519', days=200)
    if 'error' not in wave:
        print(f"整体趋势: {wave['整体趋势']}")
        print(f"最后趋势: {wave['最后趋势']}")
    else:
        print(f"错误: {wave['error']}")

    # 测试多股票比较
    print("\n3. 多股票比较:")
    comparison = multi_stock_comparison(['601318', '600519', '600036'], 'SH')
    if 'error' not in comparison:
        for result in comparison['比较结果']:
            if '错误' not in result:
                print(f"{result['股票代码']} {result['股票名称']}: "
                      f"评分={result['综合评分']}, 建议={result['投资建议']}")
    else:
        print(f"错误: {comparison['error']}")

    # 测试识别关键买卖点
    print("\n4. 识别关键买卖点:")
    points = get_trading_points('SH', '601318', days=200)
    if 'error' not in points:
        print(f"整体趋势: {points['整体趋势']}")
        print(f"MA信号: {points['MA信号']}")
        print(f"MACD信号: {points['MACD信号']}")
    else:
        print(f"错误: {points['error']}")
