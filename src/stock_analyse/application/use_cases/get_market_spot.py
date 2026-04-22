from __future__ import annotations

from stock_analyse.infrastructure.data_sources.stocklib.spot_gateway import SpotGateway


def execute(market: str, gateway: SpotGateway | None = None) -> dict:
    try:
        gateway = gateway or SpotGateway()
        df = gateway.get_market_spot(market)
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
