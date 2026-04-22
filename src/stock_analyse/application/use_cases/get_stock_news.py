from __future__ import annotations

from stock_analyse.infrastructure.data_sources.stocklib.news_gateway import NewsGateway


DEFAULT_PAGE_SIZE = 20


def execute(market: str, symbol: str, days: int = 15, page_size: int = DEFAULT_PAGE_SIZE, gateway: NewsGateway | None = None) -> dict:
    try:
        gateway = gateway or NewsGateway()
        df = gateway.get_stock_news(symbol=symbol, page_size=page_size)
        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无新闻数据"}

        records = df.to_dict(orient="records")
        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "days": days,
                "count": len(records),
                "news": records,
            },
            "message": f"获取 {len(records)} 条新闻",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"获取失败: {exc}"}
