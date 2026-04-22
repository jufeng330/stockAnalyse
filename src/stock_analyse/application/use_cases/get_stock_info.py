from __future__ import annotations

from stock_analyse.infrastructure.data_sources.stocklib.company_info_gateway import CompanyInfoGateway


def execute(market: str, symbol: str, gateway: CompanyInfoGateway | None = None) -> dict:
    try:
        gateway = gateway or CompanyInfoGateway()
        info_df, name, list_date, industry, concept, sector = gateway.get_stock_info_parts(market, symbol)
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
