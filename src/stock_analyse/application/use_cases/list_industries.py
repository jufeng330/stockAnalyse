from __future__ import annotations

from stock_analyse.infrastructure.services.concept_service import stockConcepService


MAX_ITEMS = 50


def execute(market: str, gateway: stockConcepService | None = None) -> dict:
    try:
        gateway = gateway or stockConcepService(market=market)
        _, df = gateway.get_all_sectors_and_stocks()
        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无数据"}

        records = df.to_dict(orient='records')
        return {
            "success": True,
            "data": {
                "market": market,
                "type": "industry",
                "count": len(records),
                "sectors": records[:MAX_ITEMS],
            },
            "message": f"共 {len(records)} 个行业板块",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"获取失败: {exc}"}
