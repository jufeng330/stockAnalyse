from __future__ import annotations

from stock_analyse.infrastructure.data_sources.reports.annual_report_client import stockAnnualReport


def execute(market: str, symbol: str, years: int = 5, gateway: stockAnnualReport | None = None) -> dict:
    try:
        gateway = gateway or stockAnnualReport()
        zcfz, lrb, xjll = gateway.get_stock_report(stock_code=symbol, market=market, years=years)
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
