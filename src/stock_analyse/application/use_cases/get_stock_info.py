from __future__ import annotations

from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo


def execute(market: str, symbol: str, gateway: stockCompanyInfo | None = None) -> dict:
    try:
        gateway = gateway or stockCompanyInfo(marker=market, symbol=symbol)
        info_df = gateway.get_stock_individual_info()
        name = gateway.get_stock_name()
        _, list_date, industry = gateway.get_stock_individual_info_em()
        concept = gateway.get_stock_concept_by_code(symbol)
        sector = gateway.get_stock_industry_by_code(symbol)
        result = {
            "symbol": symbol,
            "name": name,
            "market": market,
            "industry": industry,
            "concept": concept,
            "sector": sector,
            "list_date": list_date,
            "detail": info_df.to_dict() if info_df is not None else {},
        }
        return {"success": True, "data": result, "message": "获取成功"}
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"获取失败: {exc}"}
