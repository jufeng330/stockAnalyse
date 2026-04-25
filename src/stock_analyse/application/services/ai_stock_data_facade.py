from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any


class AIStockDataFacade:
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

        return snapshot

    def _build_market_context(self, market_service: Any, stock_code: str) -> dict[str, Any]:
        spot_df = self._safe_call(market_service.get_stock_spot)
        if spot_df is None or getattr(spot_df, 'empty', True):
            return {'spot': None}
        row = spot_df[spot_df['股票代码'] == stock_code] if '股票代码' in spot_df.columns else spot_df
        if getattr(row, 'empty', True):
            return {'spot': None}
        try:
            return {'spot': row.iloc[0].to_dict()}
        except Exception:
            return {'spot': None}

    def _build_news(self, stock_code: str):
        try:
            news_df = stockNewsData.stock_news_em(symbol=stock_code, pageSize=10)
            if news_df is None:
                return []
            return news_df.head(10).to_dict('records')
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
