#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Strategy Skill - 股票策略评分
"""
import sys
import json
import argparse
from datetime import datetime, timedelta

sys.path.insert(0, '/home/inspur/codes/stockAnalyse')

from stocklib.stock_strategy import StockStrategy
from stocklib.stock_wave_analyser import StockWaveAnalyzer
from stocklib.stock_ak_indicator import stockAKIndicator
from stocklib.stock_company import stockCompanyInfo
from stocklib.stock_border import stockBorderInfo


def get_history_data(market: str, symbol: str):
    """获取历史数据"""
    stock = stockCompanyInfo(marker=market, symbol=symbol)
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
    return stock.get_stock_history_data(start_date_str=start_date, end_date_str=end_date)


def calculate_score(market: str, symbol: str) -> dict:
    """计算股票综合评分"""
    try:
        # 获取历史数据
        df = get_history_data(market, symbol)
        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无法获取历史数据"}

        # 计算技术指标
        indicator = stockAKIndicator()
        df = indicator.strategy_macd(df)
        df = indicator.strategy_rsi(df)
        df = indicator.strategy_kdj(df)
        df = indicator.strategy_bollinger(df)
        df = indicator.strategy_breakout(df)

        # 计算评分
        strategy = StockStrategy(market=market)
        score, signals = strategy.calculate_score_indicate(df)

        # 获取建议
        recommendation = strategy.get_recommendation(score)

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "score": score,
                "recommendation": recommendation,
                "signals": signals,
                "analysis_date": datetime.now().strftime("%Y-%m-%d")
            },
            "message": f"评分: {score}, 建议: {recommendation}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"计算失败: {str(e)}"}


def get_signals(market: str, symbol: str) -> dict:
    """获取买入信号"""
    try:
        df = get_history_data(market, symbol)
        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无法获取历史数据"}

        # 计算各项指标
        indicator = stockAKIndicator()
        signals = []

        # MACD
        df_macd = indicator.strategy_macd(df.copy())
        if df_macd is not None and not df_macd.empty:
            latest = df_macd.iloc[-1]
            if latest.get('macd_signal_index') == 1:
                signals.append({"indicator": "MACD", "signal": "buy", "value": latest.get('macd_dif')})

        # RSI
        df_rsi = indicator.strategy_rsi(df.copy())
        if df_rsi is not None and not df_rsi.empty:
            latest = df_rsi.iloc[-1]
            rsi = latest.get('RSI')
            if rsi < 30:
                signals.append({"indicator": "RSI", "signal": "oversold", "value": rsi})

        # KDJ
        df_kdj = indicator.strategy_kdj(df.copy())
        if df_kdj is not None and not df_kdj.empty:
            latest = df_kdj.iloc[-1]
            if latest.get('kdj_signal') == 1:
                signals.append({"indicator": "KDJ", "signal": "buy", "value": latest.get('K')})

        # 布林带
        df_bb = indicator.strategy_bollinger(df.copy())
        if df_bb is not None and not df_bb.empty:
            latest = df_bb.iloc[-1]
            if latest.get('bb_signal') == 1:
                signals.append({"indicator": "Bollinger", "signal": "buy", "value": latest.get('收盘')})

        # 突破
        df_break = indicator.strategy_breakout(df.copy())
        if df_break is not None and not df_break.empty:
            latest = df_break.iloc[-1]
            if latest.get('breakout_signal') == 1:
                signals.append({"indicator": "Breakout", "signal": "buy", "value": latest.get('收盘')})

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "signals": signals,
                "signal_count": len(signals),
                "has_buy_signal": len(signals) > 0
            },
            "message": f"发现 {len(signals)} 个买入信号"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def get_recommendation(market: str, symbol: str) -> dict:
    """获取投资建议"""
    try:
        # 获取评分
        score_result = calculate_score(market, symbol)
        if not score_result['success']:
            return score_result

        # 获取信号
        signals_result = get_signals(market, symbol)

        data = score_result['data']
        data['signals'] = signals_result.get('data', {}).get('signals', [])

        # 生成详细建议
        suggestions = []
        score = data['score']

        if score >= 50:
            suggestions.append("技术指标强劲，建议积极关注")
        elif score >= 30:
            suggestions.append("技术指标向好，可考虑分批建仓")
        elif score >= 10:
            suggestions.append("技术指标中性，建议观望")
        else:
            suggestions.append("技术指标偏弱，建议谨慎")

        # 根据信号添加建议
        signal_count = len(data['signals'])
        if signal_count >= 3:
            suggestions.append(f"多个指标发出买入信号({signal_count}个)，值得关注")
        elif signal_count >= 1:
            suggestions.append(f"部分指标显示买入机会({signal_count}个)")

        data['suggestions'] = suggestions

        return {
            "success": True,
            "data": data,
            "message": f"建议: {data['recommendation']}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"获取失败: {str(e)}"}


def batch_analyze(market: str, min_score: int = 30) -> dict:
    """批量分析市场"""
    try:
        border = stockBorderInfo(market=market)
        df_spot = border.get_stock_spot()

        if df_spot is None or df_spot.empty:
            return {"success": False, "data": {}, "message": "无法获取市场数据"}

        # 只分析前20只股票
        symbols = df_spot.head(20)['股票代码'].tolist() if '股票代码' in df_spot.columns else []

        results = []
        for symbol in symbols[:10]:  # 限制数量
            try:
                result = calculate_score(market, symbol)
                if result['success'] and result['data'].get('score', 0) >= min_score:
                    results.append(result['data'])
            except:
                continue

        # 按评分排序
        results.sort(key=lambda x: x.get('score', 0), reverse=True)

        return {
            "success": True,
            "data": {
                "market": market,
                "min_score": min_score,
                "analyzed": len(symbols[:10]),
                "qualified": len(results),
                "top_stocks": results[:5]
            },
            "message": f"分析 {len(symbols[:10])} 只股票，{len(results)} 只达标"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"分析失败: {str(e)}"}


def main():
    parser = argparse.ArgumentParser(description='Stock Strategy Skill')
    parser.add_argument('--action', type=str, required=True,
                        choices=['score', 'signals', 'profitable', 'recommend', 'batch'],
                        help='操作类型')
    parser.add_argument('--market', type=str, required=True,
                        choices=['SH', 'SZ', 'H', 'usa', 'zq'],
                        help='市场代码')
    parser.add_argument('--symbol', type=str, help='股票代码')
    parser.add_argument('--date', type=str, help='报告日期(YYYYMMDD)')
    parser.add_argument('--min_score', type=int, default=30, help='最低评分阈值')

    args = parser.parse_args()

    # 参数校验
    if args.action != 'batch' and not args.symbol:
        print(json.dumps({"success": False, "data": {}, "message": "缺少 symbol 参数"}, ensure_ascii=False))
        sys.exit(1)

    # 路由到对应处理函数
    if args.action == 'score':
        result = calculate_score(args.market, args.symbol)
    elif args.action == 'signals':
        result = get_signals(args.market, args.symbol)
    elif args.action == 'recommend':
        result = get_recommendation(args.market, args.symbol)
    elif args.action == 'batch':
        result = batch_analyze(args.market, args.min_score)
    else:
        result = {"success": False, "data": {}, "message": "功能开发中"}

    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
