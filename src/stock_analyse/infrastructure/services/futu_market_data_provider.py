from __future__ import annotations

import logging
from typing import Any

import akshare as ak
import pandas as pd

from stock_analyse.infrastructure.config.settings import get_settings
from stock_analyse.infrastructure.data_sources.futu.futu_quote_client import FutuQuoteClient
from stock_analyse.infrastructure.persistence.file_cache import FileCacheUtils
from stock_analyse.infrastructure.services.market_spot_provider import MarketSpotProvider
from stock_analyse.shared.report_date_utils import ReportDateUtils

logger = logging.getLogger(__name__)


class FutuMarketDataProvider(MarketSpotProvider):
    HISTORY_NUMERIC_COLUMNS = ['开盘', '收盘', '最高', '最低', '成交量', '成交额', '换手率', '涨跌幅', '昨收']
    SPOT_NUMERIC_COLUMNS = ['最新价', '涨跌幅', '涨跌额', '今开', '最高', '最低', '昨收', '成交量', '成交额', '换手率', '市盈率-动态', '总市值', '市净率']

    SUPPORTED_MARKETS = {'H', 'HK', 'usa'}

    def __init__(self, market: str) -> None:
        self.market = self._normalize_market(market)
        self.report_utils = ReportDateUtils()
        self.cache_service = FileCacheUtils(market=self.market)
        self.client = FutuQuoteClient()

    @classmethod
    def is_enabled(cls, market: str) -> bool:
        settings = get_settings()
        normalized = cls._normalize_market(market)
        return settings.market_data.futu_enabled and settings.market_data.uses_provider(normalized, 'futu')

    def supports_market(self, market: str) -> bool:
        return self._normalize_market(market) in self.SUPPORTED_MARKETS

    def get_stock_spot(self, market: str) -> pd.DataFrame:
        normalized_market = self._normalize_market(market)
        if not self.supports_market(normalized_market):
            raise ValueError(f'Unsupported market for Futu provider: {market}')
        current_date = self.report_utils.get_current__history_date_str()
        report_type = f'futu_{normalized_market}_spot_snapshot_df'
        cached_df = self.cache_service.read_from_serialized(current_date, report_type)
        if cached_df is not None and not getattr(cached_df, 'empty', True):
            return self._normalize_output_schema(cached_df.copy(), normalized_market)

        seed_df = self._get_seed_spot_df(normalized_market)
        if seed_df.empty:
            raise RuntimeError(f'No seed stock codes available for market={normalized_market}')

        futu_codes = [
            self.to_futu_code(code, normalized_market)
            for code in seed_df['股票代码'].astype(str).tolist()
            if str(code).strip()
        ]
        snapshot_df = self.client.get_market_snapshot(futu_codes)
        if snapshot_df.empty:
            raise RuntimeError(f'Empty Futu market snapshot for market={normalized_market}')

        mapped_df = self._map_snapshot_frame(snapshot_df, normalized_market)
        if mapped_df.empty:
            raise RuntimeError(f'Futu snapshot mapping produced empty result for market={normalized_market}')

        seed_name_map = seed_df[['股票代码', '名称']].drop_duplicates(subset=['股票代码']).set_index('股票代码')['名称']
        if '名称' in mapped_df.columns:
            mapped_df['名称'] = mapped_df['名称'].fillna(mapped_df['股票代码'].map(seed_name_map))
        else:
            mapped_df['名称'] = mapped_df['股票代码'].map(seed_name_map)

        self.cache_service.write_to_cache_serialized(current_date, report_type, mapped_df)
        logger.info('Loaded Futu market snapshot | market=%s | provider=futu | rows=%s', normalized_market, len(mapped_df))
        return mapped_df

    @classmethod
    def _normalize_market(cls, market: str) -> str:
        market_text = str(market or '').strip()
        return 'H' if market_text.upper() == 'HK' else market_text

    def _get_seed_spot_df(self, market: str) -> pd.DataFrame:
        if market == 'H':
            seed_df = ak.stock_hk_main_board_spot_em()
            if seed_df is None or seed_df.empty:
                return pd.DataFrame(columns=['股票代码', '名称'])
            seed_df = seed_df.copy()
            seed_df['股票代码'] = seed_df['代码'].astype(str).str.strip().str.zfill(5)
            return seed_df[['股票代码', '名称']]
        if market == 'usa':
            seed_df = ak.stock_us_spot_em()
            if seed_df is None or seed_df.empty:
                return pd.DataFrame(columns=['股票代码', '名称'])
            seed_df = seed_df.copy()
            seed_df['股票代码'] = seed_df['代码'].apply(lambda value: self.report_utils.get_stock_code(market='usa', symbol=str(value))).astype(str).str.strip().str.upper()
            return seed_df[['股票代码', '名称']]
        return pd.DataFrame(columns=['股票代码', '名称'])

    def get_history_kline(self, stock_code: str, market: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
        normalized_market = self._normalize_market(market)
        if not self.supports_market(normalized_market):
            raise ValueError(f'Unsupported market for Futu provider: {market}')
        start_date = self._normalize_history_date(start_date_str)
        end_date = self._normalize_history_date(end_date_str)
        report_type = f'futu_history_{normalized_market}_{stock_code}_{start_date}_{end_date}'
        cached_df = self.cache_service.read_from_serialized(end_date, report_type)
        if cached_df is not None and not getattr(cached_df, 'empty', True):
            return self._normalize_history_schema(cached_df.copy(), normalized_market, stock_code)

        history_df = self.client.request_history_kline(
            code=self.to_futu_code(stock_code, normalized_market),
            start=start_date,
            end=end_date,
            ktype='K_DAY',
            autype='QFQ',
        )
        if history_df.empty:
            raise RuntimeError(f'Empty Futu history kline for market={normalized_market} stock_code={stock_code}')

        mapped_df = self._map_history_frame(history_df, normalized_market, stock_code)
        if mapped_df.empty:
            raise RuntimeError(f'Futu history mapping produced empty result for market={normalized_market} stock_code={stock_code}')
        self.cache_service.write_to_cache_serialized(end_date, report_type, mapped_df)
        logger.info(
            'Loaded Futu history kline | market=%s | stock_code=%s | provider=futu | rows=%s',
            normalized_market,
            stock_code,
            len(mapped_df),
        )
        return mapped_df

    def _map_snapshot_frame(self, df: pd.DataFrame, market: str) -> pd.DataFrame:
        frame = df.copy()
        frame['股票代码'] = frame.get('code', pd.Series(index=frame.index, dtype='object')).apply(
            lambda value: self.from_futu_code(value, market)
        )
        frame['代码'] = frame['股票代码']

        mapped = pd.DataFrame({
            '代码': frame['代码'],
            '股票代码': frame['股票代码'],
            '名称': self._first_series(frame, ['name', 'stock_name']),
            '最新价': self._first_series(frame, ['last_price', 'cur_price']),
            '涨跌幅': self._first_series(frame, ['change_rate']),
            '涨跌额': self._first_series(frame, ['change_val', 'change_value']),
            '今开': self._first_series(frame, ['open_price']),
            '最高': self._first_series(frame, ['high_price']),
            '最低': self._first_series(frame, ['low_price']),
            '昨收': self._first_series(frame, ['prev_close_price', 'last_close']),
            '成交量': self._first_series(frame, ['volume']),
            '成交额': self._first_series(frame, ['turnover']),
            '换手率': self._first_series(frame, ['turnover_rate']),
            '市盈率-动态': self._first_series(frame, ['pe_ttm_ratio', 'pe_ratio']),
            '总市值': self._first_series(frame, ['total_market_val', 'market_val']),
            '市净率': self._first_series(frame, ['pb_ratio']),
        })
        return self._normalize_output_schema(mapped, market)

    def _normalize_output_schema(self, df: pd.DataFrame, market: str) -> pd.DataFrame:
        frame = df.copy()
        if '股票代码' in frame.columns:
            if market == 'H':
                frame['股票代码'] = frame['股票代码'].astype(str).str.strip().str.zfill(5)
            elif market == 'usa':
                frame['股票代码'] = frame['股票代码'].astype(str).str.strip().str.upper()
        if '代码' in frame.columns:
            if market == 'H':
                frame['代码'] = frame['代码'].astype(str).str.strip().str.zfill(5)
            elif market == 'usa':
                frame['代码'] = frame['代码'].astype(str).str.strip().str.upper()
        for column in self.SPOT_NUMERIC_COLUMNS:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors='coerce')
        return frame.reset_index(drop=True)

    @staticmethod
    def to_futu_code(stock_code: str, market: str) -> str:
        code = str(stock_code or '').strip()
        if market == 'H':
            return f'HK.{code.zfill(5)}'
        return f'US.{code.upper()}'

    @staticmethod
    def from_futu_code(futu_code: Any, market: str) -> str:
        code = str(futu_code or '').strip()
        _, _, normalized = code.partition('.')
        normalized = normalized or code
        if market == 'H':
            return normalized.zfill(5)
        return normalized.upper()

    def _map_history_frame(self, df: pd.DataFrame, market: str, stock_code: str) -> pd.DataFrame:
        frame = df.copy()
        frame['股票代码'] = self.from_futu_code(frame.get('code', pd.Series(index=frame.index, dtype='object')), market) if isinstance(frame.get('code'), str) else frame.get('code', pd.Series(index=frame.index, dtype='object')).apply(lambda value: self.from_futu_code(value, market))
        frame['股票代码'] = frame['股票代码'].fillna(str(stock_code))
        mapped = pd.DataFrame({
            '日期': self._first_series(frame, ['time_key', 'date']),
            '开盘': self._first_series(frame, ['open']),
            '收盘': self._first_series(frame, ['close']),
            '最高': self._first_series(frame, ['high']),
            '最低': self._first_series(frame, ['low']),
            '成交量': self._first_series(frame, ['volume']),
            '成交额': self._first_series(frame, ['turnover']),
            '换手率': self._first_series(frame, ['turnover_rate']),
            '涨跌幅': self._first_series(frame, ['change_rate']),
            '昨收': self._first_series(frame, ['last_close', 'prev_close']),
            '名称': self._first_series(frame, ['name', 'stock_name']),
            '股票代码': frame['股票代码'],
        })
        return self._normalize_history_schema(mapped, market, stock_code)

    def _normalize_history_schema(self, df: pd.DataFrame, market: str, stock_code: str) -> pd.DataFrame:
        frame = df.copy()
        frame['股票代码'] = str(stock_code).strip().upper() if market == 'usa' else str(stock_code).strip().zfill(5)
        if '日期' in frame.columns:
            frame['日期'] = frame['日期'].astype(str).str.slice(0, 10)
        for column in self.HISTORY_NUMERIC_COLUMNS:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors='coerce')
        if '日期' in frame.columns:
            frame = frame.sort_values('日期').reset_index(drop=True)
        else:
            frame = frame.reset_index(drop=True)
        return frame

    @staticmethod
    def _normalize_history_date(date_text: str) -> str:
        return str(date_text or '').replace('-', '')

    @staticmethod
    def _first_series(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
        for column in candidates:
            if column in df.columns:
                return df[column]
        return pd.Series(index=df.index, dtype='object')
