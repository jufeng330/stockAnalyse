from __future__ import annotations

from datetime import datetime, timedelta

from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo


def execute(
    market: str,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
    gateway: stockCompanyInfo | None = None,
) -> dict:
    try:
        gateway = gateway or stockCompanyInfo(marker=market, symbol=symbol)
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        if not start_date:
            start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=120)).strftime("%Y%m%d")
        df = gateway.get_stock_history_data(start_date_str=start_date, end_date_str=end_date)
        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无历史数据"}
        records = df.to_dict(orient="records")
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
