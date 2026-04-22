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
    return service.build_indicator_data(
        symbol=symbol,
        indicator="ma",
        signal=signal,
        last_price=latest.get("收盘"),
        ma_10=latest.get("MA_10"),
        ma_30=latest.get("MA_30"),
    )


def _build_macd(symbol: str, latest, service: TechnicalIndicatorService) -> dict:
    signal = service.signal_from_value(latest.get("macd_signal_index"))
    return service.build_indicator_data(
        symbol=symbol,
        indicator="macd",
        signal=signal,
        last_price=latest.get("收盘"),
        macd_dif=latest.get("macd_dif"),
        macd_signal=latest.get("macd_signal"),
        hist=latest.get("hist"),
    )


def _build_rsi(symbol: str, latest, period: int, service: TechnicalIndicatorService) -> dict:
    rsi_value = latest.get("RSI")
    signal = service.rsi_signal(rsi_value)
    return service.build_indicator_data(
        symbol=symbol,
        indicator="rsi",
        signal=signal,
        last_price=latest.get("收盘"),
        rsi=rsi_value,
        period=period,
    )


def _build_bollinger(symbol: str, latest, service: TechnicalIndicatorService) -> dict:
    signal = service.signal_from_value(latest.get("bb_signal"))
    return service.build_indicator_data(
        symbol=symbol,
        indicator="bollinger",
        signal=signal,
        last_price=latest.get("收盘"),
        upper=latest.get("Upper_Band"),
        middle=latest.get("Middle_Band"),
        lower=latest.get("Lower_Band"),
    )


def _build_all_data(symbol: str, results: dict, service: TechnicalIndicatorService) -> dict:
    summary = service.summarize(results)
    return {
        "symbol": symbol,
        "indicator_values": {name: result.get("indicator_values", {}) for name, result in results.items()},
        "signal": summary["signal"],
        "indicators": results,
        "summary": summary,
    }


def _all_message(summary: dict) -> str:
    return (
        f"买入信号: {summary['buy_signals']}, 卖出信号: {summary['sell_signals']}, "
        f"评分: {summary['score']}, 建议: {summary['recommendation']}"
    )


def _single_message(action: str, data: dict) -> str:
    if action == "ma":
        return f"MA信号: {data['signal']}"
    if action == "macd":
        return f"MACD信号: {data['signal']}"
    if action == "rsi":
        return f"RSI({data['period']}): {data['rsi']:.2f}, 信号: {data['signal']}"
    if action == "bollinger":
        return f"布林带信号: {data['signal']}"
    return ""


def _execute_calculation(action: str, df, params: dict | None, gateway: TechnicalIndicatorGateway):
    if action == "ma":
        return gateway.calculate_ma(df)
    if action == "macd":
        return gateway.calculate_macd(df)
    if action == "rsi":
        period = params.get("period", 14) if params else 14
        return gateway.calculate_rsi(df, period=period)
    if action == "bollinger":
        return gateway.calculate_bollinger(df)
    return None


def _build_result_data(action: str, symbol: str, latest, params: dict | None, service: TechnicalIndicatorService) -> dict:
    if action == "ma":
        return _build_ma(symbol, latest, service)
    if action == "macd":
        return _build_macd(symbol, latest, service)
    if action == "rsi":
        period = params.get("period", 14) if params else 14
        return _build_rsi(symbol, latest, period, service)
    if action == "bollinger":
        return _build_bollinger(symbol, latest, service)
    raise ValueError("未知的 action")


def _execute_single(action: str, market: str, symbol: str, start_date: str | None, end_date: str | None, params: dict | None, gateway: TechnicalIndicatorGateway, service: TechnicalIndicatorService) -> dict:
    df = _get_history(market, symbol, start_date, end_date, gateway)
    if df is None or df.empty:
        return {"success": False, "data": {}, "message": "无法获取历史数据"}

    result_df = _execute_calculation(action, df, params, gateway)
    if result_df is None or result_df.empty:
        return {"success": False, "data": {}, "message": "计算失败"}

    data = _build_result_data(action, symbol, result_df.iloc[-1], params, service)
    return {"success": True, "data": data, "message": _single_message(action, data)}


def _execute_all(market: str, symbol: str, start_date: str | None, end_date: str | None, params: dict | None, gateway: TechnicalIndicatorGateway, service: TechnicalIndicatorService) -> dict:
    results = {}
    for name in ("ma", "macd", "rsi", "bollinger"):
        result = _execute_single(name, market, symbol, start_date, end_date, params, gateway, service)
        if result["success"]:
            results[name] = result["data"]

    data = _build_all_data(symbol, results, service)
    return {
        "success": True,
        "data": data,
        "message": _all_message(data["summary"]),
    }



def execute(action: str, market: str, symbol: str, start_date: str | None = None, end_date: str | None = None, params: dict | None = None, gateway: TechnicalIndicatorGateway | None = None, service: TechnicalIndicatorService | None = None) -> dict:
    try:
        gateway = gateway or TechnicalIndicatorGateway()
        service = service or TechnicalIndicatorService()

        if action not in SUPPORTED_ACTIONS:
            return {"success": False, "data": {}, "message": "未知的 action"}

        if action != "all":
            return _execute_single(action, market, symbol, start_date, end_date, params, gateway, service)

        return _execute_all(market, symbol, start_date, end_date, params, gateway, service)
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"计算失败: {exc}"}
