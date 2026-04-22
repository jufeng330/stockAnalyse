#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Wave Skill - 股票波浪分析
"""
import sys
import json
import argparse

sys.path.insert(0, '/home/inspur/codes/stockAnalyse')

from stocklib.stock_wave_analyser import StockWaveAnalyzer


def analyze_waves(market: str, symbol: str, days: int = 200) -> dict:
    """波浪分析"""
    try:
        analyzer = StockWaveAnalyzer(market=market, symbol=symbol)

        # 获取波浪数据
        df_wave, total_trend, last_trend = analyzer.analysis_stock_trend()

        if df_wave is None or df_wave.empty:
            return {"success": False, "data": {}, "message": "无法分析波浪"}

        waves = df_wave.to_dict(orient='records')

        # 获取最新波浪
        latest_wave = df_wave.iloc[-1] if not df_wave.empty else {}

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "total_trend": total_trend,
                "last_trend": last_trend,
                "wave_count": len(waves),
                "latest_wave": {
                    "type": latest_wave.get('类型'),
                    "start_price": latest_wave.get('开始价格'),
                    "end_price": latest_wave.get('结束价格'),
                    "amplitude": latest_wave.get('波动幅度'),
                    "amplitude_pct": latest_wave.get('波动百分比'),
                    "days": latest_wave.get('周期天数')
                },
                "waves": waves[-10:]  # 只返回最近10个波浪
            },
            "message": f"整体趋势: {total_trend}, 当前状态: {last_trend}, 共 {len(waves)} 个波浪"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"分析失败: {str(e)}"}


def analyze_trend(market: str, symbol: str, days: int = 200) -> dict:
    """趋势分析"""
    try:
        analyzer = StockWaveAnalyzer(market=market, symbol=symbol)

        df_wave, total_trend, last_trend = analyzer.analysis_stock_trend()

        if df_wave is None:
            return {"success": False, "data": {}, "message": "无法分析趋势"}

        # 分析建议
        suggestions = []

        if total_trend == "上升":
            if last_trend == "翻转中":
                suggestions.append("上升趋势中可能出现回调，注意风险")
            else:
                suggestions.append("上升趋势保持良好，可关注")
        elif total_trend == "下降":
            if last_trend == "探底中":
                suggestions.append("下降趋势中，等待企稳信号")
            else:
                suggestions.append("下降趋势，谨慎操作")
        else:
            suggestions.append("趋势不明朗，建议观望")

        # 计算波动统计
        up_waves = len(df_wave[df_wave['类型'] == '上升'])
        down_waves = len(df_wave[df_wave['类型'] == '下降'])

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "total_trend": total_trend,
                "last_trend": last_trend,
                "statistics": {
                    "up_waves": up_waves,
                    "down_waves": down_waves,
                    "total_waves": len(df_wave)
                },
                "suggestions": suggestions
            },
            "message": f"趋势: {total_trend}, 状态: {last_trend}"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"分析失败: {str(e)}"}


def visualize(market: str, symbol: str, days: int = 200) -> dict:
    """可视化"""
    try:
        # 这里简化处理，实际应该生成图表文件
        analyzer = StockWaveAnalyzer(market=market, symbol=symbol)

        df_wave, total_trend, last_trend = analyzer.analysis_stock_trend()

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "message": "可视化功能需要图形界面支持",
                "wave_data": df_wave.to_dict(orient='records')[-5:] if df_wave is not None else []
            },
            "message": "波浪数据已准备好，可在支持图形界面的环境中显示"
        }
    except Exception as e:
        return {"success": False, "data": {}, "message": f"可视化失败: {str(e)}"}


def main():
    parser = argparse.ArgumentParser(description='Stock Wave Skill')
    parser.add_argument('--action', type=str, required=True,
                        choices=['analyze', 'trend', 'visualize'],
                        help='操作类型')
    parser.add_argument('--market', type=str, required=True,
                        choices=['SH', 'SZ', 'H', 'usa', 'zq'],
                        help='市场代码')
    parser.add_argument('--symbol', type=str, required=True, help='股票代码')
    parser.add_argument('--days', type=int, default=200, help='分析天数')

    args = parser.parse_args()

    # 路由到对应处理函数
    if args.action == 'analyze':
        result = analyze_waves(args.market, args.symbol, args.days)
    elif args.action == 'trend':
        result = analyze_trend(args.market, args.symbol, args.days)
    elif args.action == 'visualize':
        result = visualize(args.market, args.symbol, args.days)
    else:
        result = {"success": False, "data": {}, "message": "未知的 action"}

    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
