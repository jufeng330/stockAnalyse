from __future__ import annotations

from stock_analyse.domain.services.valuation_service import ValuationService
from stock_analyse.infrastructure.services.valuation_gateway import ValuationGateway



def execute(market: str, symbol: str, gateway: ValuationGateway | None = None, service: ValuationService | None = None) -> dict:
    try:
        gateway = gateway or ValuationGateway()
        service = service or ValuationService()
        zcfz, lrb, xjll = gateway.get_stock_report(market=market, symbol=symbol, years=5)
        if zcfz is None or lrb is None or xjll is None:
            return {"success": False, "data": {}, "message": "无法获取财务报表"}

        result_df = gateway.calculate_stock_price_range(market=market, symbol=symbol, zcfz=zcfz, lrb=lrb, xjll=xjll)
        if result_df is None or result_df.empty:
            return {"success": False, "data": {}, "message": "计算失败"}

        latest = result_df.iloc[0]
        price_range = {
            "conservative": round(latest.get('dcf_lower_stock_price', 0), 2),
            "normal": round(latest.get('dcf_normal_stock_price', 0), 2),
            "optimistic": round(latest.get('dcf_upper_stock_price', 0), 2),
        }

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "price_range": price_range,
                "midpoint": round(service.midpoint(price_range['conservative'], price_range['optimistic']), 2),
            },
            "message": f"股价区间: {price_range['conservative']} - {price_range['optimistic']} 元",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"计算失败: {exc}"}
