from __future__ import annotations

import logging
import socket
import threading
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

    FILTER_RATE_LIMIT_WINDOW_SECONDS = 30.0
    FILTER_RATE_LIMIT_MAX_CALLS = 10
    FILTER_SAFE_CALLS_PER_WINDOW = 8

    FINANCIAL_RATE_LIMIT_WINDOW_SECONDS = 30.0
    FINANCIAL_RATE_LIMIT_MAX_CALLS = 30
    FINANCIAL_SAFE_CALLS_PER_WINDOW = 25

    _shared_quote_ctx = None
    _shared_ret_ok = None
    _quote_ctx_lock = threading.RLock()

    def __init__(self, host: str | None = None, port: int | None = None, *, batch_size: int = 200) -> None:
        settings = get_settings().market_data
        self.host = host or settings.futu_host
        self.port = int(port or settings.futu_port)
        self.batch_size = batch_size
        self.snapshot_min_interval_seconds = self.SNAPSHOT_RATE_LIMIT_WINDOW_SECONDS / self.SNAPSHOT_SAFE_CALLS_PER_WINDOW
        self._last_snapshot_call_at = 0.0
        self.filter_min_interval_seconds = self.FILTER_RATE_LIMIT_WINDOW_SECONDS / self.FILTER_SAFE_CALLS_PER_WINDOW
        self._last_filter_call_at = 0.0
        self.financial_min_interval_seconds = self.FINANCIAL_RATE_LIMIT_WINDOW_SECONDS / self.FINANCIAL_SAFE_CALLS_PER_WINDOW
        self._last_financial_call_at = 0.0

    def get_market_snapshot(self, codes: list[str], *, skip_unsupported: bool = False) -> pd.DataFrame:
        def _run(quote_ctx, ret_ok):
            frames: list[pd.DataFrame] = []
            for start in range(0, len(codes), self.batch_size):
                batch = codes[start:start + self.batch_size]
                if skip_unsupported:
                    original_len = len(batch)
                    batch = [c for c in batch if not self._is_likely_unsupported(c)]
                    if len(batch) < original_len:
                        logger.debug("Pre-filtered %d likely unsupported codes from batch", original_len - len(batch))

                if not batch:
                    continue

                retry_count = 0
                max_retries = 10
                while True:
                    ret, data = self._request_market_snapshot(quote_ctx, batch)
                    if ret == ret_ok:
                        if data is not None and not data.empty:
                            frames.append(data.copy())
                        break

                    error_message = str(data)
                    if skip_unsupported and self._is_unsupported_snapshot_error(error_message):
                        failed_code = self._extract_code_from_error(error_message)
                        if failed_code and failed_code in batch:
                            logger.warning('Removing unsupported code from batch and retrying: %s', failed_code)
                            batch.remove(failed_code)
                            retry_count += 1
                            if batch and retry_count < max_retries:
                                continue
                        if len(batch) > 1:
                            frames.extend(self._get_market_snapshot_resilient(quote_ctx, ret_ok, batch, batch_error=error_message))
                        break
                    raise RuntimeError(error_message)
            return frames

        frames = self._execute_with_quote_context(_run)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _get_market_snapshot_resilient(self, quote_ctx, ret_ok, codes: list[str], *, batch_error: str | None = None) -> list[pd.DataFrame]:
        frames: list[pd.DataFrame] = []
        if batch_error and self._is_rate_limit_snapshot_error(batch_error):
            raise RuntimeError(batch_error)
        
        # 即使在退避逻辑中，也先拆成更小的组（例如 20 个一组）而不是 1 对 1，提高效率
        sub_batch_size = 20
        for i in range(0, len(codes), sub_batch_size):
            sub_batch = codes[i:i + sub_batch_size]
            if len(sub_batch) == 1:
                ret, data = self._request_market_snapshot(quote_ctx, sub_batch)
                if ret != ret_ok:
                    error_message = str(data)
                    if self._is_unsupported_snapshot_error(error_message):
                        logger.warning('Skip unsupported Futu snapshot code | code=%s | error=%s', sub_batch[0], error_message)
                        continue
                    raise RuntimeError(f'Futu market snapshot failed for {sub_batch[0]}: {error_message}')
                if data is not None and not data.empty:
                    frames.append(data.copy())
            else:
                # 递归尝试更小的批次
                try:
                    for code in sub_batch:
                        ret, data = self._request_market_snapshot(quote_ctx, [code])
                        if ret == ret_ok:
                            if data is not None and not data.empty:
                                frames.append(data.copy())
                        else:
                            error_message = str(data)
                            if self._is_unsupported_snapshot_error(error_message):
                                logger.warning('Skip unsupported Futu snapshot code | code=%s | error=%s', code, error_message)
                                continue
                            raise RuntimeError(f'Futu market snapshot failed for {code}: {error_message}')
                except Exception as e:
                    if self._is_rate_limit_snapshot_error(str(e)):
                        raise
                    logger.error("Resilient sub-batch failed: %s", e)
        return frames

    @staticmethod
    def _is_unsupported_snapshot_error(error_message: str) -> bool:
        message = str(error_message or '')
        return '暂不提供美股 OTC' in message or 'not support' in message.lower() or '暂不提供' in message

    @staticmethod
    def _is_likely_unsupported(code: str) -> bool:
        """启发式判断是否为可能不支持的代码（如美股 OTC）"""
        if not code.startswith('US.'):
            return False
        symbol = code[3:]
        # 5位及以上代码通常是 OTC (如 LMED, CAMVF)
        # 典型的 OTC 后缀: F (Foreign), Y (ADR)
        if len(symbol) >= 5:
            return True
        return False

    @staticmethod
    def _extract_code_from_error(error_message: str) -> str | None:
        """从错误信息中提取代码，例如 '... OTC 市场行情 LMED' -> 'US.LMED'"""
        import re
        # 匹配报错信息末尾的股票代码
        match = re.search(r'市场行情\s+([A-Z0-9.]+)', error_message)
        if match:
            code = match.group(1)
            if not code.startswith('US.') and 'OTC' in error_message:
                return f'US.{code}'
            return code
        return None

    @staticmethod
    def _is_rate_limit_snapshot_error(error_message: str) -> bool:
        message = str(error_message or '')
        return '获取市场快照频率太高' in message or '每30秒最多60次' in message or 'rate limit' in message.lower()

    def request_history_kline(self, **kwargs: Any) -> pd.DataFrame:
        def _run(quote_ctx, ret_ok):
            frames: list[pd.DataFrame] = []
            page_req_key = None
            while True:
                ret, data, page_req_key = quote_ctx.request_history_kline(page_req_key=page_req_key, **kwargs)
                if ret != ret_ok:
                    raise RuntimeError(str(data))
                if data is not None and not data.empty:
                    frames.append(data.copy())
                if page_req_key is None:
                    break
            return frames

        frames = self._execute_with_quote_context(_run)
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
        def _run(quote_ctx, ret_ok):
            ret, data = quote_ctx.get_capital_flow(stock_code, period_type=period_type, start=start, end=end)
            if ret != ret_ok:
                raise RuntimeError(str(data))
            if data is None or data.empty:
                return pd.DataFrame()
            return data.copy()
        return self._execute_with_quote_context(_run)

    def get_financials_statements(self, stock_code: str, **kwargs: Any) -> pd.DataFrame:
        # 定义 field_id 到名称的映射，基于 Futu API 文档
        FIELD_ID_MAP = {
            8001: '营业收入',
            8002: '营业总收入',
            8003: '营业成本',
            8004: '毛利润',
            8005: '净利润',
            8007: '经营活动现金流',
            8010: '销售及管理费用',
            8017: '营业利润',
            8018: '所得税',
            8019: '利息费用',
            8020: '利息收入',
            8022: '净收益',
            8033: '持续经营净收益',
            8034: 'EBITDA',
            8035: '折旧与摊销',
            8037: 'EBIT',
            8038: '息税前利润',
            8042: '少数股东损益',
            8043: '归属于母公司股东净利润',
            8046: '归属于普通股股东净利润',
            8047: '每股基本收益',
            8048: '每股稀释收益'
        }

        def _run(quote_ctx, ret_ok):
            frames: list[pd.DataFrame] = []
            next_key = None
            while True:
                self._sleep_for_financial_rate_limit()
                ret, data = quote_ctx.get_financials_statements(
                    code=stock_code, next_key=next_key, **kwargs
                )
                if ret != ret_ok:
                    if self._is_rate_limit_financial_error(str(data)):
                        time.sleep(5)
                        continue
                    raise RuntimeError(str(data))
                if data and 'report_list' in data:
                    report_list = data.get("report_list", [])
                    if report_list:
                        for report in report_list:
                            item_dict = {
                                item.get("display_name"): item.get("data")
                                for item in report.get("item_list", [])
                            }
                            # Add top-level report info
                            item_dict["date_time_str"] = report.get("date_time_str")
                            item_dict["fiscal_year"] = report.get("fiscal_year")
                            item_dict["period_text"] = report.get("period_text")
                            frames.append(pd.DataFrame([item_dict]))

                    next_key = data.get("next_key")

                if not next_key or next_key == "-1":
                    break
            return frames

        frames = self._execute_with_quote_context(_run)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def get_owner_plate(self, codes: str | Sequence[str]) -> pd.DataFrame:
        code_list = self._normalize_codes_input(codes)
        def _run(quote_ctx, ret_ok):
            ret, data = quote_ctx.get_owner_plate(code_list)
            if ret != ret_ok:
                raise RuntimeError(str(data))
            if data is None or data.empty:
                return pd.DataFrame()
            return data.copy()
        return self._execute_with_quote_context(_run)

    def get_plate_list(self, market, plate_class) -> pd.DataFrame:
        def _run(quote_ctx, ret_ok):
            ret, data = quote_ctx.get_plate_list(market, plate_class)
            if ret != ret_ok:
                raise RuntimeError(str(data))
            if data is None or data.empty:
                return pd.DataFrame()
            return data.copy()
        return self._execute_with_quote_context(_run)

    def get_stock_filter(self, market, filter_list=None, plate_code: str | None = None, *, begin: int = 0, num: int | None = None) -> pd.DataFrame:
        page_size = int(num or self.batch_size or 200)
        def _run(quote_ctx, ret_ok):
            rows: list[dict[str, Any]] = []
            cursor = int(begin or 0)
            while True:
                self._sleep_for_filter_rate_limit()
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
            return rows

        rows = self._execute_with_quote_context(_run)
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

    def _sleep_for_filter_rate_limit(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_filter_call_at
        remaining = self.filter_min_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)
            now = time.monotonic()
        self._last_filter_call_at = now

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

    @classmethod
    def _close_shared_quote_context(cls) -> None:
        quote_ctx = cls._shared_quote_ctx
        cls._shared_quote_ctx = None
        cls._shared_ret_ok = None
        if quote_ctx is None:
            return
        try:
            quote_ctx.close()
        except Exception as exc:
            logger.warning('Failed to close shared Futu quote context: %s', exc)

    @classmethod
    def close_shared_quote_context(cls) -> None:
        with cls._quote_ctx_lock:
            cls._close_shared_quote_context()

    def _get_or_create_shared_quote_context(self):
        if self.__class__._shared_quote_ctx is None:
            quote_ctx, ret_ok = self._open_quote_context()
            self.__class__._shared_quote_ctx = quote_ctx
            self.__class__._shared_ret_ok = ret_ok
        return self.__class__._shared_quote_ctx, self.__class__._shared_ret_ok

    def _execute_with_quote_context(self, operation, *, retry_once: bool = True):
        with self.__class__._quote_ctx_lock:
            try:
                quote_ctx, ret_ok = self._get_or_create_shared_quote_context()
                return operation(quote_ctx, ret_ok)
            except Exception as exc:
                message = str(exc)
                if retry_once and self._should_rebuild_quote_context(message):
                    logger.warning('Rebuilding shared Futu quote context after error: %s', message)
                    self.__class__._close_shared_quote_context()
                    quote_ctx, ret_ok = self._get_or_create_shared_quote_context()
                    return operation(quote_ctx, ret_ok)
                raise

    @staticmethod
    def _should_rebuild_quote_context(error_message: str) -> bool:
        message = str(error_message or '').lower()
        return (
            'timeout' in message
            or 'disconnected' in message
            or 'connection aborted' in message
            or 'network' in message
            or 'opend unavailable' in message
            or 'connect fail' in message
        )
