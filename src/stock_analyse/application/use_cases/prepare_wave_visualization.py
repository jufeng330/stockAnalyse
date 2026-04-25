from __future__ import annotations

from stock_analyse.domain.services.stock_wave_analyzer import StockWaveAnalyzer


VISUAL_WAVES_LIMIT = 5


def execute(market: str, symbol: str, days: int = 200, gateway: StockWaveAnalyzer | None = None) -> dict:
    try:
        gateway = gateway or StockWaveAnalyzer(market=market, symbol=symbol)
        df_wave, _, _ = gateway.analysis_stock_trend()
        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "message": "可视化功能需要图形界面支持",
                "wave_data": df_wave.to_dict(orient='records')[-VISUAL_WAVES_LIMIT:] if df_wave is not None else [],
            },
            "message": "波浪数据已准备好，可在支持图形界面的环境中显示",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"可视化失败: {exc}"}
