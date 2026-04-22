from __future__ import annotations

import logging


def get_logger(name: str | None = None) -> logging.Logger:
    return logging.getLogger(name or "stock_analyse")
