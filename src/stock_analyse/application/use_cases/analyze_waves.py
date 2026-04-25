from __future__ import annotations

from stock_analyse.domain.services.stock_wave_analyzer import StockWaveAnalyzer


RECENT_WAVES_LIMIT = 10


def execute(market: str, symbol: str, days: int = 200, gateway: StockWaveAnalyzer | None = None) -> dict:
    try:
        gateway = gateway or StockWaveAnalyzer(market=market, symbol=symbol)
        df_wave, total_trend, last_trend = gateway.analysis_stock_trend()
        if df_wave is None or df_wave.empty:
            return {"success": False, "data": {}, "message": "无法分析波浪"}

        waves = df_wave.to_dict(orient='records')
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
                    "days": latest_wave.get('周期天数'),
                },
                "waves": waves[-RECENT_WAVES_LIMIT:],
            },
            "message": f"整体趋势: {total_trend}, 当前状态: {last_trend}, 共 {len(waves)} 个波浪",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"分析失败: {exc}"}
