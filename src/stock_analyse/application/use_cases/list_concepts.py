from __future__ import annotations

from stock_analyse.infrastructure.data_sources.concepts.ths_concept_client import stockConceptData


MAX_ITEMS = 50


def execute(market: str, gateway: stockConceptData | None = None) -> dict:
    try:
        gateway = gateway or stockConceptData()
        df = gateway.stock_board_concept_name_ths()
        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无数据"}

        records = df.to_dict(orient='records')
        return {
            "success": True,
            "data": {
                "market": market,
                "type": "concept",
                "count": len(records),
                "sectors": records[:MAX_ITEMS],
            },
            "message": f"共 {len(records)} 个概念板块",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"获取失败: {exc}"}
