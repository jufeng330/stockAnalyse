from __future__ import annotations

from datetime import datetime, timedelta

from stock_analyse.domain.services.technical_indicator_service import TechnicalIndicatorService
from stock_analyse.infrastructure.data_sources.stocklib.technical_indicator_gateway import TechnicalIndicatorGateway

SUPPORTED_ACTIONS = {"ma", "macd", "rsi", "bollinger", "all"}


def _default_dates(start_date: str | None, end_date: str | None) -> tuple[str, str]:
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    if not start_date:
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=365)).strftime("%Y%m%d")
    return start_date, end_date


def _get_history(market: str, symbol: str, start_date: str | None, end_date: str | None, gateway: TechnicalIndicatorGateway):
    start_date, end_date = _default_dates(start_date, end_date)
    return gateway.get_history_data(market=market, symbol=symbol, start_date=start_date, end_date=end_date)


def _build_ma(symbol: str, latest, service: TechnicalIndicatorService) -> dict:
    signal = service.signal_from_value(latest.get("mac_signal"))
    return {
        "symbol": symbol,
        "indicator": "ma",
        "ma_10": latest.get("MA_10"),
        "ma_30": latest.get("MA_30"),
        "signal": signal,
        "last_price": latest.get("收盘"),
    }


def _build_macd(symbol: str, latest, service: TechnicalIndicatorService) -> dict:
    signal = service.signal_from_value(latest.get("macd_signal_index"))
    return {
        "symbol": symbol,
        "indicator": "macd",
        "macd_dif": latest.get("macd_dif"),
        "macd_signal": latest.get("macd_signal"),
        "hist": latest.get("hist"),
        "signal": signal,
        "last_price": latest.get("收盘"),
    }


def _build_rsi(symbol: str, latest, period: int, service: TechnicalIndicatorService) -> dict:
    rsi_value = latest.get("RSI")
    signal = service.rsi_signal(rsi_value)
    return {
        "symbol": symbol,
        "indicator": "rsi",
        "rsi": rsi_value,
        "period": period,
        "signal": signal,
        "last_price": latest.get("收盘"),
    }


def _build_bollinger(symbol: str, latest, service: TechnicalIndicatorService) -> dict:
    signal = service.signal_from_value(latest.get("bb_signal"))
    return {
        "symbol": symbol,
        "indicator": "bollinger",
        "upper": latest.get("Upper_Band"),
        "middle": latest.get("Middle_Band"),
        "lower": latest.get("Lower_Band"),
        "signal": signal,
        "last_price": latest.get("收盘"),
    }


def _execute_single(action: str, market: str, symbol: str, start_date: str | None, end_date: str | None, params: dict | None, gateway: TechnicalIndicatorGateway, service: TechnicalIndicatorService) -> dict:
    df = _get_history(market, symbol, start_date, end_date, gateway)
    if df is None or df.empty:
        return {"success": False, "data": {}, "message": "无法获取历史数据"}

    if action == "ma":
        result_df = gateway.calculate_ma(df)
        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}
        data = _build_ma(symbol, result_df.iloc[-1], service)
        return {"success": True, "data": data, "message": f"MA信号: {data['signal']}"}

    if action == "macd":
        result_df = gateway.calculate_macd(df)
        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}
        data = _build_macd(symbol, result_df.iloc[-1], service)
        return {"success": True, "data": data, "message": f"MACD信号: {data['signal']}"}

    if action == "rsi":
        period = params.get("period", 14) if params else 14
        result_df = gateway.calculate_rsi(df, period=period)
        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}
        data = _build_rsi(symbol, result_df.iloc[-1], period, service)
        rsi_value = data["rsi"]
        return {"success": True, "data": data, "message": f"RSI({period}): {rsi_value:.2f}, 信号: {data['signal']}"}

    if action == "bollinger":
        result_df = gateway.calculate_bollinger(df)
        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}
        data = _build_bollinger(symbol, result_df.iloc[-1], service)
        return {"success": True, "data": data, "message": f"布林带信号: {data['signal']}"}

    return {"success": False, "data": {}, "message": "未知的 action"}


def execute(action: str, market: str, symbol: str, start_date: str | None = None, end_date: str | None = None, params: dict | None = None, gateway: TechnicalIndicatorGateway | None = None, service: TechnicalIndicatorService | None = None) -> dict:
    try:
        gateway = gateway or TechnicalIndicatorGateway()
        service = service or TechnicalIndicatorService()

        if action not in SUPPORTED_ACTIONS:
            return {"success": False, "data": {}, "message": "未知的 action"}

        if action != "all":
            return _execute_single(action, market, symbol, start_date, end_date, params, gateway, service)

        results = {}
        for name in ("ma", "macd", "rsi", "bollinger"):
            result = _execute_single(name, market, symbol, start_date, end_date, params, gateway, service)
            if result["success"]:
                results[name] = result["data"]

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "indicators": results,
                "summary": service.summarize(results),
            },
            "message": f"买入信号: {service.summarize(results)['buy_signals']}, 卖出信号: {service.summarize(results)['sell_signals']}",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"计算失败: {exc}"}
