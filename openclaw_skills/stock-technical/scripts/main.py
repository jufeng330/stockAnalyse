#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Technical Skill - 股票技术指标计算
"""
import sys
import json
import argparse
from datetime import datetime, timedelta

sys.path.insert(0, '/home/inspur/codes/stockAnalyse')

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
    try:
        indicator = stockAKIndicator()
        result_df = indicator.strategy_mac(df)

        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}

        # 获取最新信号
        latest = result_df.iloc[-1]
        signal = "buy" if latest.get('mac_signal') == 1 else "sell" if latest.get('mac_signal') == -1 else "neutral"

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "indicator": "ma",
                "ma_10": latest.get('MA_10'),
                "ma_30": latest.get('MA_30'),
                "signal": signal,
                "last_price": latest.get('收盘')
            },
            "message": f"MA信号: {signal}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


def calculate_macd(market: str, symbol: str, df, params: dict = None) -> dict:
    """计算 MACD"""
    try:
        indicator = stockAKIndicator()
        result_df = indicator.strategy_macd(df)

        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}

        latest = result_df.iloc[-1]
        signal = "buy" if latest.get('macd_signal_index') == 1 else "sell" if latest.get('macd_signal_index') == -1 else "neutral"

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "indicator": "macd",
                "macd_dif": latest.get('macd_dif'),
                "macd_signal": latest.get('macd_signal'),
                "hist": latest.get('hist'),
                "signal": signal,
                "last_price": latest.get('收盘')
            },
            "message": f"MACD信号: {signal}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


def calculate_rsi(market: str, symbol: str, df, params: dict = None) -> dict:
    """计算 RSI"""
    try:
        period = params.get('period', 14) if params else 14
        indicator = stockAKIndicator()
        result_df = indicator.strategy_rsi(df, period=period)

        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}

        latest = result_df.iloc[-1]
        rsi_value = latest.get('RSI')

        # RSI 解读
        if rsi_value > 70:
            signal = "overbought"
        elif rsi_value < 30:
            signal = "oversold"
        else:
            signal = "neutral"

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "indicator": "rsi",
                "rsi": rsi_value,
                "period": period,
                "signal": signal,
                "last_price": latest.get('收盘')
            },
            "message": f"RSI({period}): {rsi_value:.2f}, 信号: {signal}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


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
    try:
        indicator = stockAKIndicator()
        result_df = indicator.strategy_bollinger(df)

        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}

        latest = result_df.iloc[-1]
        signal = "buy" if latest.get('bb_signal') == 1 else "sell" if latest.get('bb_signal') == -1 else "neutral"

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "indicator": "bollinger",
                "upper": latest.get('Upper_Band'),
                "middle": latest.get('Middle_Band'),
                "lower": latest.get('Lower_Band'),
                "signal": signal,
                "last_price": latest.get('收盘')
            },
            "message": f"布林带信号: {signal}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


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
    try:
        results = {}

        # 计算各个指标
        ma_result = calculate_ma(market, symbol, df, params)
        macd_result = calculate_macd(market, symbol, df, params)
        rsi_result = calculate_rsi(market, symbol, df, params)
        kdj_result = calculate_kdj(market, symbol, df, params)
        bollinger_result = calculate_bollinger(market, symbol, df, params)

        if ma_result['success']:
            results['ma'] = ma_result['data']
        if macd_result['success']:
            results['macd'] = macd_result['data']
        if rsi_result['success']:
            results['rsi'] = rsi_result['data']
        if kdj_result['success']:
            results['kdj'] = kdj_result['data']
        if bollinger_result['success']:
            results['bollinger'] = bollinger_result['data']

        # 统计信号
        buy_signals = sum(1 for r in results.values() if r.get('signal') in ['buy', 'oversold'])
        sell_signals = sum(1 for r in results.values() if r.get('signal') in ['sell', 'overbought'])

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "indicators": results,
                "summary": {
                    "buy_signals": buy_signals,
                    "sell_signals": sell_signals,
                    "neutral_signals": len(results) - buy_signals - sell_signals
                }
            },
            "message": f"买入信号: {buy_signals}, 卖出信号: {sell_signals}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


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
