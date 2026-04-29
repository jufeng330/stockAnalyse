from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    'stockAKIndicator': '.stock_ak_indicator',
    'stockAnnualReport': '.stock_annual_report',
    'stockIndicatorQuantitative': '.stock_indicator_quantitative',
    'stockConceptData': '.stock_concept_data',
    'stockCompanyInfo': '.stock_company',
    'stockNewsData': '.stock_news_data',
    'stockBorderInfo': '.stock_border',
    'stockDCFSimpleModel': '.dcf_model',
    'ReportDateUtils': '.utils_report_date',
    'FileCacheUtils': '.utils_file_cache',
    'StockUtils': '.utils_stock',
    'StockStrategy': '.stock_strategy',
    'MySQLCache': '.mysql_cache',
    'stockConcepService': '.stock_concept_service',
    'StockWaveAnalyzer': '.stock_wave_analyser',
    'StockSentimentAnalysis': '.stock_sentiment_analysis',
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
    module = import_module(_EXPORTS[name], __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + __all__)
