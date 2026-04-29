from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path

_SRC_PATH = Path(__file__).resolve().parent / 'src'
if _SRC_PATH.exists():
    src_path = str(_SRC_PATH)
    if src_path not in sys.path:
        sys.path.append(src_path)

_EXPORTS = {
    'stocklib': '.stocklib',
    'stockAI': '.stockAI',
    'scanner': '.scanner',
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
    module = import_module(_EXPORTS[name], __name__)
    globals()[name] = module
    return module


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + __all__)
