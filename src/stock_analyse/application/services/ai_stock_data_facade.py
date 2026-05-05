from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging
from typing import Any

from stock_analyse.infrastructure.data_sources.news.eastmoney_news_client import stockNewsData
from stock_analyse.infrastructure.data_sources.searxng_client import SearxngClient
from stock_analyse.infrastructure.persistence.file_cache import FileCacheUtils

logger = logging.getLogger(__name__)


class AIStockDataFacade:
    """AI 分析前置数据快照门面。

    用于股票分析、进场决策等 AI 场景，在 application 层统一聚合公司资料、技术面、情绪面、市场快照与财报摘要。
    """

    def __init__(
        self,
        *,
        technical_analysis_workflow: Any | None = None,
        company_service_factory: Any | None = None,
        market_service_factory: Any | None = None,
        report_service: Any | None = None,
    ) -> None:
        if technical_analysis_workflow is None:
            from stock_analyse.application.workflows.technical_analysis_workflow import TechnicalAnalysisWorkflow

            technical_analysis_workflow = TechnicalAnalysisWorkflow()
        if company_service_factory is None:
            from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo

            company_service_factory = lambda market, stock_code: stockCompanyInfo(marker=market, symbol=stock_code)
        if market_service_factory is None:
            from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo

            market_service_factory = lambda market: stockBorderInfo(market=market)
        if report_service is None:
            from stock_analyse.infrastructure.data_sources.reports.annual_report_client import stockAnnualReport

            report_service = stockAnnualReport()

        self._technical_analysis_workflow = technical_analysis_workflow
        self._company_service_factory = company_service_factory
        self._market_service_factory = market_service_factory
        self._report_service = report_service
        self._cache_service = FileCacheUtils(market='none', cache_dir='ai_stock_snapshot')
        self._searxng = SearxngClient()

    def build_snapshot(
        self,
        *,
        stock_code: str,
        market: str,
        trade_date: str | None = None,
        start_date_str: str | None = None,
        end_date_str: str | None = None,
        include_technical: bool = True,
        include_sentiment: bool = True,
    ) -> dict[str, Any]:
        company_service = self._company_service_factory(market, stock_code)
        market_service = self._market_service_factory(market)
        start_date_str, end_date_str = self._resolve_dates(trade_date, start_date_str, end_date_str)

        snapshot: dict[str, Any] = {
            'stock_code': stock_code,
            'market': market,
            'trade_date': trade_date or end_date_str,
            'date_range': {'start_date': start_date_str, 'end_date': end_date_str},
            'company_profile': self._safe_call(company_service.get_stock_individual_info),
            'company_name': self._safe_call(company_service.get_stock_name, default=stock_code),
            'business_intro': self._safe_call(company_service.get_stock_zyjs),
            'industry': self._safe_call(lambda: company_service.get_stock_industry_by_code(stock_code), default=''),
            'concepts': self._safe_call(lambda: company_service.get_stock_concept_by_code(stock_code), default=''),
            'market_context': self._build_market_context(market_service, stock_code),
            'news': self._build_news(stock_code),
            'financial_indicators': self._safe_call(company_service.get_stock_financial_analysis_indicator),
            'fund_flow': self._safe_call(company_service.get_stock_individual_fund_flow),
            'reports': self._build_reports(stock_code, market),
        }

        if include_technical:
            technical_score, technical_summary = self._safe_call(
                lambda: self._technical_analysis_workflow.run(stock_code=stock_code, market=market),
                default=(0, {}),
            )
            snapshot['technical'] = {
                'score': technical_score,
                'summary': technical_summary,
            }
        else:
            snapshot['technical'] = {'score': 0, 'summary': {}}

        if include_sentiment:
            from stock_analyse.application.use_cases import analyze_sentiment

            sentiment_result = analyze_sentiment.execute(market=market, symbol=stock_code, days=15)
            snapshot['sentiment'] = sentiment_result.get('data', {}) if sentiment_result.get('success') else {}
        else:
            snapshot['sentiment'] = {}

        self._log_snapshot_summary(snapshot)
        return snapshot

    def _log_snapshot_summary(self, snapshot: dict[str, Any]) -> None:
        field_summaries = {key: self._summarize_snapshot_value(value) for key, value in snapshot.items()}
        logger.info(
            'AI stock snapshot summary | stock_code=%s | market=%s | trade_date=%s | fields=%s',
            snapshot.get('stock_code'),
            snapshot.get('market'),
            snapshot.get('trade_date'),
            json.dumps(field_summaries, ensure_ascii=False),
        )

    def _summarize_snapshot_value(self, value: Any) -> dict[str, Any]:
        summary: dict[str, Any] = {
            'is_empty': self._is_empty_value(value),
            'type': type(value).__name__,
            'size': self._value_size(value),
            'preview': self._truncate_preview(value),
        }
        if hasattr(value, 'shape'):
            try:
                summary['shape'] = tuple(value.shape)
            except Exception:
                pass
        return summary

    def _is_empty_value(self, value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ''
        if isinstance(value, (list, tuple, set, dict)):
            return len(value) == 0
        if hasattr(value, 'empty'):
            try:
                return bool(value.empty)
            except Exception:
                return False
        return False

    def _value_size(self, value: Any) -> int | tuple[int, ...] | None:
        if value is None:
            return 0
        if isinstance(value, str):
            return len(value.strip())
        if isinstance(value, (list, tuple, set, dict)):
            return len(value)
        if hasattr(value, 'shape'):
            try:
                return tuple(value.shape)
            except Exception:
                return None
        return None

    def _truncate_preview(self, value: Any, limit: int = 200) -> str:
        if value is None:
            return 'None'
        try:
            if isinstance(value, str):
                preview = value.strip()
            else:
                preview = json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            preview = str(value)
        preview = preview.replace('\n', ' ').replace('\r', ' ')
        return preview[:limit] + ('...' if len(preview) > limit else '')

    def _build_market_context(self, market_service: Any, stock_code: str) -> dict[str, Any]:
        spot_df = self._safe_call(market_service.get_stock_spot)
        if spot_df is not None and not getattr(spot_df, 'empty', True):
            row = spot_df[spot_df['股票代码'] == stock_code] if '股票代码' in spot_df.columns else spot_df
            if not getattr(row, 'empty', True):
                try:
                    return {'spot': row.iloc[0].to_dict()}
                except Exception:
                    pass
        fallback_spot = self._search_spot_via_searxng(stock_code)
        return {'spot': fallback_spot}

    def _search_spot_via_searxng(self, stock_code: str) -> dict[str, Any] | None:
        results = self._searxng.search(query=f'{stock_code} stock price market cap site:finance.yahoo.com OR site:marketwatch.com', limit=5, category='general')
        if not results:
            return None
        top = results[0]
        return {
            '股票代码': stock_code,
            '名称': top.get('title', ''),
            '新闻链接': top.get('url', ''),
            '摘要': top.get('content', ''),
            'source': 'searxng',
        }

    def _search_news_via_searxng(self, stock_code: str) -> list[dict[str, Any]]:
        results = self._searxng.search(query=f'{stock_code} stock latest news', limit=10, category='news', time_range='month')
        news_items: list[dict[str, Any]] = []
        for item in results:
            news_items.append({
                '关键词': stock_code,
                '新闻标题': item.get('title', ''),
                '新闻内容': item.get('content', ''),
                '发布时间': item.get('publishedDate', '') or item.get('published_date', '') or '',
                '文章来源': ', '.join(item.get('engines', []) or []),
                '新闻链接': item.get('url', ''),
                'source': 'searxng',
            })
        return news_items

    def _is_empty_records(self, value: Any) -> bool:
        return value is None or (isinstance(value, list) and len(value) == 0)

    def _logger_warning(self, message: str, *args: Any) -> None:
        logger.warning(message, *args)

    @property
    def _logger(self):
        return logger

    def _build_news(self, stock_code: str):
        current_date = datetime.now().strftime('%Y-%m-%d')
        report_type = f'{stock_code}_news_top10'
        cached_data = self._cache_service.read_from_serialized(current_date, report_type)
        if cached_data is not None:
            return cached_data
        try:
            news_df = stockNewsData.stock_news_em(symbol=stock_code, pageSize=10)
            if news_df is None or getattr(news_df, 'empty', True):
                result = self._search_news_via_searxng(stock_code)
            else:
                result = news_df.head(10).to_dict('records')
            self._cache_service.write_to_cache_serialized(current_date, report_type, result)
            return result
        except Exception as exc:
            self._logger.warning('build news failed | stock_code=%s | error=%s', stock_code, exc)
            fallback = self._search_news_via_searxng(stock_code)
            self._cache_service.write_to_cache_serialized(current_date, report_type, fallback)
            return fallback

    def _build_reports(self, stock_code: str, market: str) -> dict[str, Any]:
        zcfz, lrb, xjll = self._report_service.get_stock_report(stock_code=stock_code, market=market)
        return {
            'balance_sheet': self._to_records(zcfz, limit=3),
            'income_statement': self._to_records(lrb, limit=3),
            'cash_flow': self._to_records(xjll, limit=3),
        }

    def _resolve_dates(self, trade_date: str | None, start_date_str: str | None, end_date_str: str | None) -> tuple[str, str]:
        if end_date_str and start_date_str:
            return start_date_str, end_date_str
        end = datetime.strptime(trade_date, '%Y-%m-%d') if trade_date else datetime.now()
        start = end - timedelta(days=100)
        return (start_date_str or start.strftime('%Y-%m-%d'), end_date_str or end.strftime('%Y-%m-%d'))

    def _safe_call(self, fn, default=None):
        try:
            value = fn()
            return default if value is None else value
        except Exception:
            return default

    def _to_records(self, df, *, limit: int) -> list[dict[str, Any]]:
        if df is None or getattr(df, 'empty', True):
            return []
        try:
            return df.head(limit).to_dict('records')
        except Exception:
            return []

    def _build_reports(self, stock_code: str, market: str) -> dict[str, Any]:
        zcfz, lrb, xjll = self._report_service.get_stock_report(stock_code=stock_code, market=market)
        return {
            'balance_sheet': self._to_records(zcfz, limit=3),
            'income_statement': self._to_records(lrb, limit=3),
            'cash_flow': self._to_records(xjll, limit=3),
        }

    def _resolve_dates(self, trade_date: str | None, start_date_str: str | None, end_date_str: str | None) -> tuple[str, str]:
        if end_date_str and start_date_str:
            return start_date_str, end_date_str
        end = datetime.strptime(trade_date, '%Y-%m-%d') if trade_date else datetime.now()
        start = end - timedelta(days=100)
        return (start_date_str or start.strftime('%Y-%m-%d'), end_date_str or end.strftime('%Y-%m-%d'))

    def _safe_call(self, fn, default=None):
        try:
            value = fn()
            return default if value is None else value
        except Exception:
            return default

    def _to_records(self, df, *, limit: int) -> list[dict[str, Any]]:
        if df is None or getattr(df, 'empty', True):
            return []
        try:
            return df.head(limit).to_dict('records')
        except Exception:
            return []
