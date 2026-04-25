from __future__ import annotations

from datetime import datetime, timedelta

from stock_analyse.domain.services.technical_indicator_service import TechnicalIndicatorService
from stock_analyse.infrastructure.analysis.technical_indicator_calculator import stockAKIndicator

SUPPORTED_ACTIONS = {"ma", "macd", "rsi", "kdj", "bollinger", "breakout", "sar", "williams", "adx", "all"}


def _default_dates(start_date: str | None, end_date: str | None) -> tuple[str, str]:
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    if not start_date:
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=365)).strftime("%Y%m%d")
    return start_date, end_date


def _get_history(market: str, symbol: str, start_date: str | None, end_date: str | None, gateway: stockAKIndicator):
    start_date, end_date = _default_dates(start_date, end_date)
    return gateway.stock_day_data_code(symbol, market, start_date, end_date)


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


def _build_kdj(symbol: str, latest, service: TechnicalIndicatorService) -> dict:
    signal = service.signal_from_value(latest.get("kdj_signal"))
    return service.build_indicator_data(
        symbol=symbol,
        indicator="kdj",
        signal=signal,
        last_price=latest.get("收盘"),
        k=latest.get("K"),
        d=latest.get("D"),
        j=latest.get("J"),
    )


def _build_breakout(symbol: str, latest, service: TechnicalIndicatorService) -> dict:
    signal = service.signal_from_value(latest.get("breakout_signal"))
    return service.build_indicator_data(
        symbol=symbol,
        indicator="breakout",
        signal=signal,
        last_price=latest.get("收盘"),
        resistance=latest.get("residence"),
        support=latest.get("support"),
    )


def _build_sar(symbol: str, latest, service: TechnicalIndicatorService) -> dict:
    signal = service.signal_from_value(latest.get("sar_signal"))
    return service.build_indicator_data(
        symbol=symbol,
        indicator="sar",
        signal=signal,
        last_price=latest.get("收盘"),
        sar=latest.get("SAR"),
    )


def _build_williams(symbol: str, latest, period: int, service: TechnicalIndicatorService) -> dict:
    williams_value = latest.get("Williams_R")
    signal = service.williams_signal(williams_value)
    return service.build_indicator_data(
        symbol=symbol,
        indicator="williams",
        signal=signal,
        last_price=latest.get("收盘"),
        williams_r=williams_value,
        period=period,
    )


def _build_adx(symbol: str, latest, period: int, threshold: int, service: TechnicalIndicatorService) -> dict:
    signal = service.signal_from_value(latest.get("adx_signal"))
    return service.build_indicator_data(
        symbol=symbol,
        indicator="adx",
        signal=signal,
        last_price=latest.get("收盘"),
        adx=latest.get("ADX"),
        di_plus=latest.get("+DI"),
        di_minus=latest.get("-DI"),
        period=period,
        threshold=threshold,
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
    if action == "kdj":
        return f"KDJ信号: {data['signal']}"
    if action == "bollinger":
        return f"布林带信号: {data['signal']}"
    if action == "breakout":
        return f"突破信号: {data['signal']}"
    if action == "sar":
        return f"SAR信号: {data['signal']}"
    if action == "williams":
        return f"威廉指标: {data['williams_r']:.2f}, 信号: {data['signal']}"
    if action == "adx":
        return f"ADX信号: {data['signal']}"
    return ""


def _execute_calculation(action: str, df, params: dict | None, gateway: stockAKIndicator):
    if action == "ma":
        return gateway.strategy_mac(df)
    if action == "macd":
        return gateway.strategy_macd(df)
    if action == "rsi":
        period = params.get("period", 14) if params else 14
        return gateway.strategy_rsi(df, period=period)
    if action == "kdj":
        fastk_period = params.get("fastk_period", 9) if params else 9
        slowk_period = params.get("slowk_period", 3) if params else 3
        slowd_period = params.get("slowd_period", 3) if params else 3
        return gateway.strategy_kdj(df, fastk_period=fastk_period, slowk_period=slowk_period, slowd_period=slowd_period)
    if action == "bollinger":
        return gateway.strategy_bollinger(df)
    if action == "breakout":
        window = params.get("window", 20) if params else 20
        return gateway.strategy_breakout(df, window=window)
    if action == "sar":
        return gateway.strategy_sar(df)
    if action == "williams":
        period = params.get("period", 14) if params else 14
        return gateway.strategy_williams_r(df, time_period=period)
    if action == "adx":
        period = params.get("period", 14) if params else 14
        threshold = params.get("adx_threshold", 25) if params else 25
        return gateway.strategy_adx(df, time_period=period, adx_threshold=threshold)
    return None


def _build_result_data(action: str, symbol: str, latest, params: dict | None, service: TechnicalIndicatorService) -> dict:
    if action == "ma":
        return _build_ma(symbol, latest, service)
    if action == "macd":
        return _build_macd(symbol, latest, service)
    if action == "rsi":
        period = params.get("period", 14) if params else 14
        return _build_rsi(symbol, latest, period, service)
    if action == "kdj":
        return _build_kdj(symbol, latest, service)
    if action == "bollinger":
        return _build_bollinger(symbol, latest, service)
    if action == "breakout":
        return _build_breakout(symbol, latest, service)
    if action == "sar":
        return _build_sar(symbol, latest, service)
    if action == "williams":
        period = params.get("period", 14) if params else 14
        return _build_williams(symbol, latest, period, service)
    if action == "adx":
        period = params.get("period", 14) if params else 14
        threshold = params.get("adx_threshold", 25) if params else 25
        return _build_adx(symbol, latest, period, threshold, service)
    raise ValueError("未知的 action")


ACTIONS_FOR_ALL = ("ma", "macd", "rsi", "kdj", "bollinger", "breakout", "sar", "williams", "adx")


def _execute_single(action: str, market: str, symbol: str, start_date: str | None, end_date: str | None, params: dict | None, gateway: stockAKIndicator, service: TechnicalIndicatorService) -> dict:
    df = _get_history(market, symbol, start_date, end_date, gateway)
    if df is None or df.empty:
        return {"success": False, "data": {}, "message": "无法获取历史数据"}

    result_df = _execute_calculation(action, df, params, gateway)
    if result_df is None or result_df.empty:
        return {"success": False, "data": {}, "message": "计算失败"}

    data = _build_result_data(action, symbol, result_df.iloc[-1], params, service)
    return {"success": True, "data": data, "message": _single_message(action, data)}


def _execute_all(market: str, symbol: str, start_date: str | None, end_date: str | None, params: dict | None, gateway: stockAKIndicator, service: TechnicalIndicatorService) -> dict:
    results = {}
    for name in ACTIONS_FOR_ALL:
        result = _execute_single(name, market, symbol, start_date, end_date, params, gateway, service)
        if result["success"]:
            results[name] = result["data"]

    data = _build_all_data(symbol, results, service)
    return {
        "success": True,
        "data": data,
        "message": _all_message(data["summary"]),
    }



def execute(action: str, market: str, symbol: str, start_date: str | None = None, end_date: str | None = None, params: dict | None = None, gateway: stockAKIndicator | None = None, service: TechnicalIndicatorService | None = None) -> dict:
    try:
        gateway = gateway or stockAKIndicator()
        service = service or TechnicalIndicatorService()

        if action not in SUPPORTED_ACTIONS:
            return {"success": False, "data": {}, "message": "未知的 action"}

        if action != "all":
            return _execute_single(action, market, symbol, start_date, end_date, params, gateway, service)

        return _execute_all(market, symbol, start_date, end_date, params, gateway, service)
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"计算失败: {exc}"}
