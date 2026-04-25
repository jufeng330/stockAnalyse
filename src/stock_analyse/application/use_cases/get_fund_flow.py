from __future__ import annotations

from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo


MAX_ITEMS = 100


def execute(market: str, symbol: str | None = None, gateway: stockCompanyInfo | stockBorderInfo | None = None) -> dict:
    try:
        if symbol:
            gateway = gateway or stockCompanyInfo(marker=market, symbol=symbol)
            target, df = symbol, gateway.get_stock_individual_fund_flow()
        else:
            gateway = gateway or stockBorderInfo(market=market)
            target, df = market, gateway.get_stock_all_info()
        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无资金流数据"}

        records = df.head(MAX_ITEMS).to_dict(orient='records')
        return {
            "success": True,
            "data": {
                "target": target,
                "market": market,
                "records": records,
                "count": len(records),
            },
            "message": f"获取 {len(records)} 条记录",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"获取失败: {exc}"}
