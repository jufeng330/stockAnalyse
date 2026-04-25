from __future__ import annotations

from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo


def execute(market: str, symbol: str, gateway: stockCompanyInfo | None = None) -> dict:
    try:
        gateway = gateway or stockCompanyInfo(marker=market, symbol=symbol)
        concepts = gateway.get_stock_concept_by_code(symbol)
        industry = gateway.get_stock_industry_by_code(symbol)
        concepts_list = concepts.split(',') if concepts else []
        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "concepts": concepts_list,
                "industry": industry,
            },
            "message": f"所属行业: {industry}, 概念数: {len(concepts_list)}",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"获取失败: {exc}"}
