from __future__ import annotations

from stock_analyse.domain.services.stock_wave_analyzer import StockWaveAnalyzer



def execute(market: str, symbol: str, days: int = 200, gateway: StockWaveAnalyzer | None = None) -> dict:
    try:
        gateway = gateway or StockWaveAnalyzer(market=market, symbol=symbol)
        df_wave, total_trend, last_trend = gateway.analysis_stock_trend()
        if df_wave is None or df_wave.empty:
            return {"success": False, "data": {}, "message": "无法分析趋势"}

        suggestions = []
        if total_trend == '上升':
            if last_trend == '翻转中':
                suggestions.append('上升趋势中可能出现回调，注意风险')
            else:
                suggestions.append('上升趋势保持良好，可关注')
        elif total_trend == '下降':
            if last_trend == '探底中':
                suggestions.append('下降趋势中，等待企稳信号')
            else:
                suggestions.append('下降趋势，谨慎操作')
        else:
            suggestions.append('趋势不明朗，建议观望')

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
                    "total_waves": len(df_wave),
                },
                "suggestions": suggestions,
            },
            "message": f"趋势: {total_trend}, 状态: {last_trend}",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"分析失败: {exc}"}
