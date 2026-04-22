#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stock Technical Skill - 股票技术指标 Skill
提供各类技术指标计算和交易信号生成，包括MACD、RSI、KDJ、布林带、威廉指标、ADX等
"""

from stocklib import stockAKIndicator


def calculate_ma_signal(market: str, symbol: str, start_date: str, end_date: str,
                        window: int = 20) -> dict:
    """
    计算移动平均线策略信号

    Args:
        market: 市场代码
        symbol: 股票代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        window: MA周期

    Returns:
        dict: MA策略信号数据
    """
    try:
        indicator = stockAKIndicator()
        df_data = indicator.stock_day_data_code(
            stock_code=symbol,
            market=market,
            start_date_str=start_date,
            end_date_str=end_date
        )
        df_mac = indicator.strategy_mac(data=df_data, window=window)

        return {
            "股票代码": symbol,
            "MA周期": window,
            "数据条数": len(df_mac),
            "MA信号": df_mac.to_dict('records') if df_mac is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def calculate_macd_signal(market: str, symbol: str, start_date: str, end_date: str,
                          momentum_window: int = 20) -> dict:
    """
    计算MACD策略信号

    Args:
        market: 市场代码
        symbol: 股票代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        momentum_window: 动量窗口

    Returns:
        dict: MACD策略信号数据
    """
    try:
        indicator = stockAKIndicator()
        df_data = indicator.stock_day_data_code(
            stock_code=symbol,
            market=market,
            start_date_str=start_date,
            end_date_str=end_date
        )
        df_macd = indicator.strategy_macd(data=df_data, momentum_window=momentum_window)

        return {
            "股票代码": symbol,
            "数据条数": len(df_macd),
            "MACD信号": df_macd.to_dict('records') if df_macd is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def calculate_rsi_signal(market: str, symbol: str, start_date: str, end_date: str,
                         period: int = 14, overbought: int = 70, oversold: int = 30) -> dict:
    """
    计算RSI策略信号

    Args:
        market: 市场代码
        symbol: 股票代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        period: RSI周期
        overbought: 超买阈值
        oversold: 超卖阈值

    Returns:
        dict: RSI策略信号数据
    """
    try:
        indicator = stockAKIndicator()
        df_data = indicator.stock_day_data_code(
            stock_code=symbol,
            market=market,
            start_date_str=start_date,
            end_date_str=end_date
        )
        df_rsi = indicator.strategy_rsi(data=df_data, period=period,
                                       overbought=overbought, oversold=oversold)

        return {
            "股票代码": symbol,
            "RSI周期": period,
            "数据条数": len(df_rsi),
            "RSI信号": df_rsi.to_dict('records') if df_rsi is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def calculate_kdj_signal(market: str, symbol: str, start_date: str, end_date: str,
                        fastk_period: int = 9, slowk_period: int = 3, slowd_period: int = 3) -> dict:
    """
    计算KDJ策略信号

    Args:
        market: 市场代码
        symbol: 股票代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        fastk_period: K值周期
        slowk_period: 慢速K周期
        slowd_period: 慢速D周期

    Returns:
        dict: KDJ策略信号数据
    """
    try:
        indicator = stockAKIndicator()
        df_data = indicator.stock_day_data_code(
            stock_code=symbol,
            market=market,
            start_date_str=start_date,
            end_date_str=end_date
        )
        df_kdj = indicator.strategy_kdj(data=df_data, fastk_period=fastk_period,
                                       slowk_period=slowk_period, slowd_period=slowd_period)

        return {
            "股票代码": symbol,
            "数据条数": len(df_kdj),
            "KDJ信号": df_kdj.to_dict('records') if df_kdj is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def calculate_bollinger_signal(market: str, symbol: str, start_date: str, end_date: str,
                               short_window: int = 10, long_window: int = 30) -> dict:
    """
    计算布林带策略信号

    Args:
        market: 市场代码
        symbol: 股票代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        short_window: 短期MA周期
        long_window: 长期MA周期

    Returns:
        dict: 布林带策略信号数据
    """
    try:
        indicator = stockAKIndicator()
        df_data = indicator.stock_day_data_code(
            stock_code=symbol,
            market=market,
            start_date_str=start_date,
            end_date_str=end_date
        )
        df_boll = indicator.strategy_bollinger(data=df_data, short_window=short_window,
                                               long_window=long_window)

        return {
            "股票代码": symbol,
            "数据条数": len(df_boll),
            "布林带信号": df_boll.to_dict('records') if df_boll is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def calculate_adx_signal(market: str, symbol: str, start_date: str, end_date: str,
                         time_period: int = 14, adx_threshold: int = 25) -> dict:
    """
    计算ADX策略信号

    Args:
        market: 市场代码
        symbol: 股票代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        time_period: 计算周期
        adx_threshold: ADX阈值

    Returns:
        dict: ADX策略信号数据
    """
    try:
        indicator = stockAKIndicator()
        df_data = indicator.stock_day_data_code(
            stock_code=symbol,
            market=market,
            start_date_str=start_date,
            end_date_str=end_date
        )
        df_adx = indicator.strategy_adx(data=df_data, time_period=time_period,
                                        adx_threshold=adx_threshold)

        return {
            "股票代码": symbol,
            "ADX阈值": adx_threshold,
            "数据条数": len(df_adx),
            "ADX信号": df_adx.to_dict('records') if df_adx is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def calculate_all_signals(market: str, symbol: str, start_date: str, end_date: str) -> dict:
    """
    计算所有技术指标信号

    Args:
        market: 市场代码
        symbol: 股票代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)

    Returns:
        dict: 所有技术指标信号
    """
    try:
        import pandas as pd

        indicator = stockAKIndicator()
        df_data = indicator.stock_day_data_code(
            stock_code=symbol,
            market=market,
            start_date_str=start_date,
            end_date_str=end_date
        )

        # 计算各种指标
        df_mac = indicator.strategy_mac(df_data)
        df_macd = indicator.strategy_macd(df_data)
        df_rsi = indicator.strategy_rsi(df_data)
        df_kdj = indicator.strategy_kdj(df_data)
        df_boll = indicator.strategy_bollinger(df_data)
        df_adx = indicator.strategy_adx(df_data)

        # 合并信号
        df_signals = pd.DataFrame({
            '日期': df_data['日期'],
            'MA信号': df_mac['mac_signal'],
            'MACD信号': df_macd['macd_signal'],
            'RSI信号': df_rsi['rsi_signal'],
            'KDJ信号': df_kdj['kdj_signal'],
            '布林信号': df_boll['bollinger_signal'],
            'ADX信号': df_adx['adx_signal']
        })

        return {
            "股票代码": symbol,
            "数据条数": len(df_signals),
            "综合信号": df_signals.to_dict('records') if df_signals is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_latest_signals(market: str, symbol: str, start_date: str, end_date: str) -> dict:
    """
    获取最新技术指标信号

    Args:
        market: 市场代码
        symbol: 股票代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)

    Returns:
        dict: 最新信号
    """
    try:
        indicator = stockAKIndicator()
        df_data = indicator.stock_day_data_code(
            stock_code=symbol,
            market=market,
            start_date_str=start_date,
            end_date_str=end_date
        )

        # 计算各种指标
        df_mac = indicator.strategy_mac(df_data)
        df_macd = indicator.strategy_macd(df_data)
        df_rsi = indicator.strategy_rsi(df_data)
        df_kdj = indicator.strategy_kdj(df_data)
        df_boll = indicator.strategy_bollinger(df_data)

        # 获取最新信号
        latest = {
            "股票代码": symbol,
            "最新日期": df_data['日期'].iloc[-1],
            "收盘价": df_data['收盘'].iloc[-1],
            "MA信号": df_mac['mac_signal'].iloc[-1],
            "MACD信号": df_macd['macd_signal'].iloc[-1],
            "RSI信号": df_rsi['rsi_signal'].iloc[-1],
            "KDJ信号": df_kdj['kdj_signal'].iloc[-1],
            "布林信号": df_boll['bollinger_signal'].iloc[-1],
            "RSI数值": df_rsi['RSI'].iloc[-1],
            "KDJ-K": df_kdj['K'].iloc[-1],
            "KDJ-D": df_kdj['D'].iloc[-1]
        }

        return latest
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    # 测试代码
    print("=== Stock Technical Skill 测试 ===")

    # 测试计算MA信号
    print("\n1. 计算MA信号:")
    ma = calculate_ma_signal('SH', '601318', '20240101', '20241231')
    if 'error' not in ma:
        print(f"数据条数: {ma['数据条数']}")
    else:
        print(f"错误: {ma['error']}")

    # 测试计算MACD信号
    print("\n2. 计算MACD信号:")
    macd = calculate_macd_signal('SH', '601318', '20240101', '20241231')
    if 'error' not in macd:
        print(f"数据条数: {macd['数据条数']}")
    else:
        print(f"错误: {macd['error']}")

    # 测试获取最新信号
    print("\n3. 获取最新信号:")
    latest = get_latest_signals('SH', '601318', '20240101', '20241231')
    if 'error' not in latest:
        print(f"最新日期: {latest['最新日期']}")
        print(f"收盘价: {latest['收盘价']}")
        print(f"MA信号: {latest['MA信号']}")
        print(f"RSI数值: {latest['RSI数值']:.2f}")
    else:
        print(f"错误: {latest['error']}")
