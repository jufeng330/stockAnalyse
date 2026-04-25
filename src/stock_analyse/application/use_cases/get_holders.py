from __future__ import annotations

from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo



def execute(market: str, symbol: str, gateway: stockCompanyInfo | None = None) -> dict:
    try:
        gateway = gateway or stockCompanyInfo(marker=market, symbol=symbol)
        df = gateway.get_stock_gdzjc()
        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无股东数据"}

        records = df.to_dict(orient='records')
        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "records": records,
                "count": len(records),
            },
            "message": f"获取 {len(records)} 条记录",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"获取失败: {exc}"}
