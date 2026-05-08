from __future__ import annotations

import logging
import socket
import time
from collections.abc import Sequence
from typing import Any

import pandas as pd

from stock_analyse.infrastructure.config.settings import get_settings

logger = logging.getLogger(__name__)


class FutuQuoteClient:
    SNAPSHOT_RATE_LIMIT_WINDOW_SECONDS = 30.0
    SNAPSHOT_RATE_LIMIT_MAX_CALLS = 60
    SNAPSHOT_SAFE_CALLS_PER_WINDOW = 45

    def __init__(self, host: str | None = None, port: int | None = None, *, batch_size: int = 200) -> None:
        settings = get_settings().market_data
        self.host = host or settings.futu_host
        self.port = int(port or settings.futu_port)
        self.batch_size = batch_size
        self.snapshot_min_interval_seconds = self.SNAPSHOT_RATE_LIMIT_WINDOW_SECONDS / self.SNAPSHOT_SAFE_CALLS_PER_WINDOW
        self._last_snapshot_call_at = 0.0

    def get_market_snapshot(self, codes: list[str], *, skip_unsupported: bool = False) -> pd.DataFrame:
        quote_ctx, ret_ok = self._open_quote_context()
        frames: list[pd.DataFrame] = []
        try:
            for start in range(0, len(codes), self.batch_size):
                batch = codes[start:start + self.batch_size]
                ret, data = self._request_market_snapshot(quote_ctx, batch)
                if ret != ret_ok:
                    error_message = str(data)
                    if skip_unsupported and len(batch) > 1:
                        frames.extend(self._get_market_snapshot_resilient(quote_ctx, ret_ok, batch, batch_error=error_message))
                        continue
                    if skip_unsupported and self._is_unsupported_snapshot_error(error_message):
                        logger.warning('Skip unsupported Futu snapshot code | code=%s | error=%s', ','.join(batch), error_message)
                        continue
                    raise RuntimeError(error_message)
                if data is not None and not data.empty:
                    frames.append(data.copy())
        finally:
            quote_ctx.close()
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _get_market_snapshot_resilient(self, quote_ctx, ret_ok, codes: list[str], *, batch_error: str | None = None) -> list[pd.DataFrame]:
        frames: list[pd.DataFrame] = []
        if batch_error and self._is_rate_limit_snapshot_error(batch_error):
            raise RuntimeError(batch_error)
        for code in codes:
            ret, data = self._request_market_snapshot(quote_ctx, [code])
            if ret != ret_ok:
                error_message = str(data)
                if self._is_unsupported_snapshot_error(error_message):
                    logger.warning('Skip unsupported Futu snapshot code | code=%s | error=%s', code, error_message)
                    continue
                if self._is_rate_limit_snapshot_error(error_message):
                    raise RuntimeError(f'Futu market snapshot rate limit exceeded while fetching {code}: {error_message}')
                raise RuntimeError(f'Futu market snapshot failed for {code}: {error_message}')
            if data is not None and not data.empty:
                frames.append(data.copy())
        return frames

    @staticmethod
    def _is_unsupported_snapshot_error(error_message: str) -> bool:
        message = str(error_message or '')
        return '暂不提供美股 OTC 市场行情' in message or 'not support' in message.lower()

    @staticmethod
    def _is_rate_limit_snapshot_error(error_message: str) -> bool:
        message = str(error_message or '')
        return '获取市场快照频率太高' in message or '每30秒最多60次' in message or 'rate limit' in message.lower()

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

    def get_capital_flow(
        self,
        stock_code: str,
        *,
        period_type: str = 'INTRADAY',
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        quote_ctx, ret_ok = self._open_quote_context()
        try:
            ret, data = quote_ctx.get_capital_flow(stock_code, period_type=period_type, start=start, end=end)
            if ret != ret_ok:
                raise RuntimeError(str(data))
            if data is None or data.empty:
                return pd.DataFrame()
            return data.copy()
        finally:
            quote_ctx.close()

    def get_owner_plate(self, codes: str | Sequence[str]) -> pd.DataFrame:
        quote_ctx, ret_ok = self._open_quote_context()
        code_list = self._normalize_codes_input(codes)
        try:
            ret, data = quote_ctx.get_owner_plate(code_list)
            if ret != ret_ok:
                raise RuntimeError(str(data))
            if data is None or data.empty:
                return pd.DataFrame()
            return data.copy()
        finally:
            quote_ctx.close()

    def get_plate_list(self, market, plate_class) -> pd.DataFrame:
        quote_ctx, ret_ok = self._open_quote_context()
        try:
            ret, data = quote_ctx.get_plate_list(market, plate_class)
            if ret != ret_ok:
                raise RuntimeError(str(data))
            if data is None or data.empty:
                return pd.DataFrame()
            return data.copy()
        finally:
            quote_ctx.close()

    def get_stock_filter(self, market, filter_list=None, plate_code: str | None = None, *, begin: int = 0, num: int | None = None) -> pd.DataFrame:
        quote_ctx, ret_ok = self._open_quote_context()
        page_size = int(num or self.batch_size or 200)
        rows: list[dict[str, Any]] = []
        cursor = int(begin or 0)
        try:
            while True:
                ret, data = quote_ctx.get_stock_filter(
                    market=market,
                    filter_list=filter_list or [],
                    plate_code=plate_code,
                    begin=cursor,
                    num=page_size,
                )
                if ret != ret_ok:
                    raise RuntimeError(str(data))
                last_page, _all_count, items = data
                for item in items or []:
                    row = {
                        key: value
                        for key, value in vars(item).items()
                        if not isinstance(key, tuple)
                    }
                    rows.append(row)
                if last_page:
                    break
                cursor += page_size
        finally:
            quote_ctx.close()
        if not rows:
            return pd.DataFrame()
        frame = pd.DataFrame(rows)
        if 'stock_code' in frame.columns:
            frame = frame.drop_duplicates(subset=['stock_code']).reset_index(drop=True)
        else:
            frame = frame.drop_duplicates().reset_index(drop=True)
        return frame

    @staticmethod
    def _normalize_codes_input(codes: str | Sequence[str]) -> list[str]:
        if isinstance(codes, str):
            return [code.strip() for code in codes.split(',') if code.strip()]
        return [str(code).strip() for code in codes if str(code).strip()]

    def _request_market_snapshot(self, quote_ctx, codes: list[str]):
        self._sleep_for_snapshot_rate_limit()
        return quote_ctx.get_market_snapshot(codes)

    def _sleep_for_snapshot_rate_limit(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_snapshot_call_at
        remaining = self.snapshot_min_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)
            now = time.monotonic()
        self._last_snapshot_call_at = now

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
