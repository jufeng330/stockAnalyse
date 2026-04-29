from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    'StockAnalyzer': '.stock_analyzer',
    'StockFenHengAnalyser': '.stock_fh_analyser',
    'TopStockScanner': '.top_stock_scanner',
    'StockFileUtils': '.stock_result_utils',
    'StockSelectStrategy': '.stock_select_strategy',
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
