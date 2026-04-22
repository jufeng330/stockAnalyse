from __future__ import annotations

from stock_analyse.infrastructure.data_sources.stocklib.report_gateway import ReportGateway


def execute(market: str, symbol: str, years: int = 5, gateway: ReportGateway | None = None) -> dict:
    try:
        gateway = gateway or ReportGateway()
        zcfz, lrb, xjll = gateway.get_stock_report(market=market, symbol=symbol, years=years)
        result = {
            "symbol": symbol,
            "market": market,
            "balance_sheet": zcfz.to_dict(orient="records") if zcfz is not None else [],
            "income_statement": lrb.to_dict(orient="records") if lrb is not None else [],
            "cash_flow": xjll.to_dict(orient="records") if xjll is not None else [],
        }
        return {"success": True, "data": result, "message": "获取成功"}
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"获取失败: {exc}"}
