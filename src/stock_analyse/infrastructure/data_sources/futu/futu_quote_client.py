from __future__ import annotations

import socket
from typing import Any

import pandas as pd

from stock_analyse.infrastructure.config.settings import get_settings


class FutuQuoteClient:
    def __init__(self, host: str | None = None, port: int | None = None, *, batch_size: int = 200) -> None:
        settings = get_settings().market_data
        self.host = host or settings.futu_host
        self.port = int(port or settings.futu_port)
        self.batch_size = batch_size

    def get_market_snapshot(self, codes: list[str]) -> pd.DataFrame:
        quote_ctx, ret_ok = self._open_quote_context()
        frames: list[pd.DataFrame] = []
        try:
            for start in range(0, len(codes), self.batch_size):
                batch = codes[start:start + self.batch_size]
                ret, data = quote_ctx.get_market_snapshot(batch)
                if ret != ret_ok:
                    raise RuntimeError(str(data))
                if data is not None and not data.empty:
                    frames.append(data.copy())
        finally:
            quote_ctx.close()
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def request_history_kline(self, **kwargs: Any) -> pd.DataFrame:
        quote_ctx, ret_ok = self._open_quote_context()
        frames: list[pd.DataFrame] = []
        page_req_key = None
        try:
            while True:
                ret, data, page_req_key = quote_ctx.request_history_kline(page_req_key=page_req_key, **kwargs)
                if ret != ret_ok:
                    raise RuntimeError(str(data))
                if data is not None and not data.empty:
                    frames.append(data.copy())
                if page_req_key is None:
                    break
        finally:
            quote_ctx.close()
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _open_quote_context(self):
        try:
            with socket.create_connection((self.host, self.port), timeout=1):
                pass
        except OSError as exc:
            raise RuntimeError(f'Futu OpenD unavailable at {self.host}:{self.port}') from exc
        try:
            from futu import OpenQuoteContext, RET_OK
        except ImportError as exc:
            raise RuntimeError('futu-api is not installed') from exc
        return OpenQuoteContext(host=self.host, port=self.port), RET_OK
