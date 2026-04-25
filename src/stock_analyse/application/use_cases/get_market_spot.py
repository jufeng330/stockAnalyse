from __future__ import annotations

from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo


def execute(market: str, gateway: stockBorderInfo | None = None) -> dict:
    try:
        gateway = gateway or stockBorderInfo(market=market)
        df = gateway.get_stock_spot()
        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无市场数据"}
        records = df.head(100).to_dict(orient="records")
        return {
            "success": True,
            "data": {
                "market": market,
                "records": records,
                "total": len(df),
                "returned": len(records),
            },
            "message": f"共 {len(df)} 只股票，返回前 {len(records)} 只",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"获取失败: {exc}"}
