from __future__ import annotations

from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
from stock_analyse.shared.report_date_utils import ReportDateUtils


MAX_ITEMS = 100


def execute(market: str, date: str | None = None, gateway: stockBorderInfo | None = None) -> dict:
    try:
        gateway = gateway or stockBorderInfo(market=market)
        report_date = date or ReportDateUtils().get_current_report_year_st(market=market)
        df = gateway.get_stock_fhps_info(date=report_date)
        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无分红数据"}

        records = df.head(MAX_ITEMS).to_dict(orient='records')
        return {
            "success": True,
            "data": {
                "market": market,
                "date": report_date,
                "records": records,
                "count": len(records),
            },
            "message": f"获取 {len(records)} 条记录",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"获取失败: {exc}"}
