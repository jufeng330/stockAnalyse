from __future__ import annotations

from stock_analyse.infrastructure.data_sources.news.eastmoney_news_client import stockNewsData
from stock_analyse.infrastructure.data_sources.searxng_client import SearxngClient


DEFAULT_PAGE_SIZE = 20


def execute(market: str, symbol: str, days: int = 15, page_size: int = DEFAULT_PAGE_SIZE, gateway: stockNewsData | None = None) -> dict:
    try:
        gateway = gateway or stockNewsData()
        df = gateway.stock_news_em(symbol=symbol, pageSize=page_size)
        if df is None or df.empty:
            results = SearxngClient().search(query=f'{symbol} stock latest news', limit=page_size, category='news', time_range='month')
            if not results:
                return {"success": False, "data": {}, "message": "无新闻数据"}
            records = [
                {
                    '关键词': symbol,
                    '新闻标题': item.get('title', ''),
                    '新闻内容': item.get('content', ''),
                    '发布时间': item.get('publishedDate', '') or item.get('published_date', '') or '',
                    '文章来源': ', '.join(item.get('engines', []) or []),
                    '新闻链接': item.get('url', ''),
                    'source': 'searxng',
                }
                for item in results
            ]
        else:
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

