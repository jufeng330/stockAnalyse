from __future__ import annotations

from stock_analyse.application.use_cases import get_price_range
from stock_analyse.domain.services.valuation_service import ValuationService
from stock_analyse.infrastructure.data_sources.stocklib.valuation_gateway import ValuationGateway



def _get_current_price(market: str, symbol: str, gateway: ValuationGateway) -> float:
    df = gateway.get_stock_spot(market=market)
    if df is None or df.empty:
        return -1

    stock_row = df[df['股票代码'] == symbol]
    if stock_row.empty:
        return -1

    price = stock_row.iloc[0].get('最新价', -1)
    return float(price) if price else -1



def execute(market: str, symbol: str, gateway: ValuationGateway | None = None, service: ValuationService | None = None) -> dict:
    try:
        gateway = gateway or ValuationGateway()
        service = service or ValuationService()
        range_result = get_price_range.execute(market=market, symbol=symbol, gateway=gateway, service=service)
        if not range_result['success']:
            return range_result

        current_price = _get_current_price(market=market, symbol=symbol, gateway=gateway)
        price_range = range_result['data']['price_range']
        margin = service.margin_of_safety(current_price, price_range['normal'])
        status, suggestion = service.compare_status(current_price, price_range)

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "current_price": current_price if current_price > 0 else None,
                "price_range": price_range,
                "margin_of_safety": f"{round(margin, 2)}%",
                "status": status,
                "suggestion": suggestion,
            },
            "message": f"估值状态: {status}, 安全边际: {round(margin, 2)}%",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"分析失败: {exc}"}
