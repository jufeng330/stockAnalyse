#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Technical Skill - 股票技术指标计算
"""
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from stock_analyse.application.use_cases import analyze_technical_indicators as analyze_technical_indicators_use_case
from stocklib.stock_ak_indicator import stockAKIndicator
from stocklib.stock_company import stockCompanyInfo


def get_history_data(market: str, symbol: str, start_date: str = None, end_date: str = None):
    """获取历史数据"""
    stock = stockCompanyInfo(marker=market, symbol=symbol)

    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    if not start_date:
        start = datetime.strptime(end_date, "%Y%m%d") - timedelta(days=365)
        start_date = start.strftime("%Y%m%d")

    return stock.get_stock_history_data(start_date_str=start_date, end_date_str=end_date)


def calculate_ma(market: str, symbol: str, df, params: dict = None) -> dict:
    """计算移动平均线"""
    return analyze_technical_indicators_use_case.execute(
        action='ma',
        market=market,
        symbol=symbol,
        params=params,
    )


def calculate_macd(market: str, symbol: str, df, params: dict = None) -> dict:
    """计算 MACD"""
    return analyze_technical_indicators_use_case.execute(
        action='macd',
        market=market,
        symbol=symbol,
        params=params,
    )


def calculate_rsi(market: str, symbol: str, df, params: dict = None) -> dict:
    """计算 RSI"""
    return analyze_technical_indicators_use_case.execute(
        action='rsi',
        market=market,
        symbol=symbol,
        params=params,
    )


def calculate_kdj(market: str, symbol: str, df, params: dict = None) -> dict:
    """计算 KDJ"""
    try:
        indicator = stockAKIndicator()
        result_df = indicator.strategy_kdj(df)

        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}

        latest = result_df.iloc[-1]
        signal = "buy" if latest.get('kdj_signal') == 1 else "sell" if latest.get('kdj_signal') == -1 else "neutral"

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "indicator": "kdj",
                "k": latest.get('K'),
                "d": latest.get('D'),
                "j": latest.get('J'),
                "signal": signal,
                "last_price": latest.get('收盘')
            },
            "message": f"KDJ信号: {signal}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


def calculate_bollinger(market: str, symbol: str, df, params: dict = None) -> dict:
    """计算布林带"""
    return analyze_technical_indicators_use_case.execute(
        action='bollinger',
        market=market,
        symbol=symbol,
        params=params,
    )


def calculate_breakout(market: str, symbol: str, df, params: dict = None) -> dict:
    """计算突破策略"""
    try:
        window = params.get('window', 20) if params else 20
        indicator = stockAKIndicator()
        result_df = indicator.strategy_breakout(df, window=window)

        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}

        latest = result_df.iloc[-1]
        signal = "buy" if latest.get('breakout_signal') == 1 else "sell" if latest.get('breakout_signal') == -1 else "neutral"

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "indicator": "breakout",
                "resistance": latest.get('residence'),
                "support": latest.get('support'),
                "signal": signal,
                "last_price": latest.get('收盘')
            },
            "message": f"突破信号: {signal}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


def calculate_sar(market: str, symbol: str, df, params: dict = None) -> dict:
    """计算 SAR"""
    try:
        indicator = stockAKIndicator()
        result_df = indicator.strategy_sar(df)

        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}

        latest = result_df.iloc[-1]
        signal = "buy" if latest.get('sar_signal') == 1 else "sell" if latest.get('sar_signal') == -1 else "neutral"

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "indicator": "sar",
                "sar": latest.get('SAR'),
                "signal": signal,
                "last_price": latest.get('收盘')
            },
            "message": f"SAR信号: {signal}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


def calculate_williams(market: str, symbol: str, df, params: dict = None) -> dict:
    """计算威廉指标"""
    try:
        indicator = stockAKIndicator()
        result_df = indicator.strategy_williams_r(df)

        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}

        latest = result_df.iloc[-1]
        williams = latest.get('Williams_R')

        if williams < -80:
            signal = "oversold"
        elif williams > -20:
            signal = "overbought"
        else:
            signal = "neutral"

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "indicator": "williams",
                "williams_r": williams,
                "signal": signal,
                "last_price": latest.get('收盘')
            },
            "message": f"威廉指标: {williams:.2f}, 信号: {signal}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


def calculate_adx(market: str, symbol: str, df, params: dict = None) -> dict:
    """计算 ADX"""
    try:
        indicator = stockAKIndicator()
        result_df = indicator.strategy_adx(df)

        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}

        latest = result_df.iloc[-1]
        signal = "buy" if latest.get('adx_signal') == 1 else "sell" if latest.get('adx_signal') == -1 else "neutral"

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "indicator": "adx",
                "adx": latest.get('ADX'),
                "di_plus": latest.get('DI+'),
                "di_minus": latest.get('DI-'),
                "signal": signal,
                "last_price": latest.get('收盘')
            },
            "message": f"ADX信号: {signal}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


def calculate_all(market: str, symbol: str, df, params: dict = None) -> dict:
    """计算所有指标"""
    return analyze_technical_indicators_use_case.execute(
        action='all',
        market=market,
        symbol=symbol,
        params=params,
    )


def main():
    parser = argparse.ArgumentParser(description='Stock Technical Skill')
    parser.add_argument('--action', type=str, required=True,
                        choices=['ma', 'macd', 'rsi', 'kdj', 'bollinger', 'breakout', 'sar',
                                'mean_reversion', 'williams', 'adx', 'obv', 'all'],
                        help='指标类型')
    parser.add_argument('--market', type=str, required=True,
                        choices=['SH', 'SZ', 'H', 'usa', 'zq'],
                        help='市场代码')
    parser.add_argument('--symbol', type=str, required=True, help='股票代码')
    parser.add_argument('--start_date', type=str, help='开始日期(YYYYMMDD)')
    parser.add_argument('--end_date', type=str, help='结束日期(YYYYMMDD)')
    parser.add_argument('--params', type=str, help='指标参数(JSON格式)')

    args = parser.parse_args()

    # 解析参数
    params = None
    if args.params:
        try:
            params = json.loads(args.params)
        except:
            pass

    # 获取历史数据
    df = get_history_data(args.market, args.symbol, args.start_date, args.end_date)

    if df is None or df.empty:
        print(json.dumps({"success": False, "data": {}, "message": "无法获取历史数据"}, ensure_ascii=False))
        sys.exit(1)

    # 路由到对应处理函数
    handlers = {
        'ma': calculate_ma,
        'macd': calculate_macd,
        'rsi': calculate_rsi,
        'kdj': calculate_kdj,
        'bollinger': calculate_bollinger,
        'breakout': calculate_breakout,
        'sar': calculate_sar,
        'williams': calculate_williams,
        'adx': calculate_adx,
        'all': calculate_all
    }

    handler = handlers.get(args.action)
    if handler:
        result = handler(args.market, args.symbol, df, params)
    else:
        result = {"success": False, "data": {}, "message": "未知的 action"}

    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
