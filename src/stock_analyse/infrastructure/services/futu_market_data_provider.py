from __future__ import annotations

import logging
from typing import Any

import akshare as ak
import pandas as pd

from stock_analyse.infrastructure.config.settings import get_settings
from stock_analyse.infrastructure.data_sources.futu.futu_quote_client import FutuQuoteClient
from stock_analyse.infrastructure.persistence.file_cache import FileCacheUtils
from stock_analyse.infrastructure.services.futu_stock_filter_mapper import FutuStockFilterMapper
from stock_analyse.infrastructure.services.market_spot_provider import MarketSpotProvider
from stock_analyse.shared.report_date_utils import ReportDateUtils

try:
    from futu import Market, Plate
except ImportError:
    Market = None
    Plate = None

logger = logging.getLogger(__name__)


class FutuMarketDataProvider(MarketSpotProvider):
    HISTORY_NUMERIC_COLUMNS = ['开盘', '收盘', '最高', '最低', '成交量', '成交额', '换手率', '涨跌幅', '昨收']
    SPOT_NUMERIC_COLUMNS = ['最新价', '涨跌幅', '涨跌额', '今开', '最高', '最低', '昨收', '成交量', '成交额', '换手率', '市盈率-动态', '总市值', '市净率']
    DETAIL_NUMERIC_COLUMNS = [
        '最新价', '今开', '最高', '最低', '昨收', '成交量', '成交额', '换手率', '每手股数', '振幅', '均价', '量比',
        '52周最高', '52周最低', '历史最高', '历史最低', '已发行股份', '流通股本', '总市值', '流通市值', '净资产',
        '净利润', '每股收益', '每股净资产', '收益率', '市盈率', '市净率', '市盈率-TTM', '股息-TTM', '股息率-TTM',
        '上一财年股息', '上一财年股息率', '盘前价格', '盘前成交量', '盘前成交额', '盘前涨跌额', '盘前涨跌幅',
        '盘后价格', '盘后成交量', '盘后成交额', '盘后涨跌额', '盘后涨跌幅', '夜盘价格', '夜盘成交量', '夜盘成交额',
        '夜盘涨跌额', '夜盘涨跌幅',
    ]
    CAPITAL_FLOW_NUMERIC_COLUMNS = ['资金净流入', '超大单净流入', '大单净流入', '中单净流入', '小单净流入']
    OWNER_PLATE_TYPE_MAP = {'INDUSTRY': '行业', 'CONCEPT': '概念', 'OTHER': '其他', 'REGION': '地域', 'ALL': '全部'}
    PLATE_CLASS_MAP = {'industry': 'INDUSTRY', 'concept': 'CONCEPT', 'other': 'OTHER', 'region': 'REGION'}

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
            seed_df['股票代码'] = seed_df['代码'].apply(
                lambda value: self.report_utils.get_stock_code(market='usa', symbol=str(value))
            ).astype(str).str.strip().str.upper()
            return seed_df[['股票代码', '名称']]
        return pd.DataFrame(columns=['股票代码', '名称'])

    def get_history_kline(self, stock_code: str, market: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
        normalized_market = self._normalize_market(market)
        if not self.supports_market(normalized_market):
            raise ValueError(f'Unsupported market for Futu provider: {market}')
        start_date = self._normalize_history_date(start_date_str)
        end_date = self._normalize_history_date(end_date_str)
        futu_start_date = self._format_futu_history_date(start_date)
        futu_end_date = self._format_futu_history_date(end_date)
        report_type = f'futu_history_{normalized_market}_{stock_code}_{start_date}_{end_date}'
        cached_df = self.cache_service.read_from_serialized(end_date, report_type)
        if cached_df is not None and not getattr(cached_df, 'empty', True):
            return self._normalize_history_schema(cached_df.copy(), normalized_market, stock_code)

        history_df = self.client.request_history_kline(
            code=self.to_futu_code(stock_code, normalized_market),
            start=futu_start_date,
            end=futu_end_date,
            ktype='K_DAY',
            autype='qfq',
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

    def get_filtered_stock_spot(self, market: str, strategy_type: int | str, strategy_config: dict | None) -> pd.DataFrame:
        normalized_market = self._normalize_market(market)
        if not self.supports_market(normalized_market):
            raise ValueError(f'Unsupported market for Futu provider: {market}')
        futu_market = FutuStockFilterMapper.to_futu_market(normalized_market)
        filter_list = FutuStockFilterMapper.build_filters(strategy_type=strategy_type, market=normalized_market, config=strategy_config)
        if not filter_list:
            logger.info('Skip Futu stock filter because no pushdown filters are available | market=%s | strategy=%s', normalized_market, strategy_type)
            return self.get_stock_spot(normalized_market)

        filtered_df = self.client.get_stock_filter(market=futu_market, filter_list=filter_list)
        if filtered_df.empty:
            logger.info('Futu stock filter returned empty candidates | market=%s | strategy=%s', normalized_market, strategy_type)
            return pd.DataFrame()

        filtered_df = filtered_df.copy()
        filtered_df['股票代码'] = filtered_df['stock_code'].apply(lambda value: self.from_futu_code(value, normalized_market))
        futu_codes = filtered_df['stock_code'].dropna().astype(str).tolist()
        snapshot_df = self.client.get_market_snapshot(futu_codes, skip_unsupported=True)
        if snapshot_df.empty:
            raise RuntimeError(f'Empty Futu market snapshot for filtered candidates: market={normalized_market}')

        result_df = self._map_snapshot_frame(snapshot_df, normalized_market)
        if result_df.empty:
            raise RuntimeError(f'Futu filtered snapshot mapping produced empty result for market={normalized_market}')

        filtered_name_map = filtered_df[['股票代码', 'stock_name']].drop_duplicates(subset=['股票代码']).set_index('股票代码')['stock_name']
        if '名称' in result_df.columns:
            result_df['名称'] = result_df['名称'].fillna(result_df['股票代码'].map(filtered_name_map))
        else:
            result_df['名称'] = result_df['股票代码'].map(filtered_name_map)

        candidate_codes = set(filtered_df['股票代码'].astype(str).tolist())
        result_df = result_df[result_df['股票代码'].astype(str).isin(candidate_codes)].copy()
        returned_codes = set(result_df['股票代码'].astype(str).tolist())
        missing_codes = sorted(candidate_codes - returned_codes)
        if missing_codes:
            logger.warning(
                'Futu filtered snapshot dropped some candidates | market=%s | strategy=%s | missing=%s | sample=%s',
                normalized_market,
                strategy_type,
                len(missing_codes),
                ','.join(missing_codes[:10]),
            )
        logger.info(
            'Loaded Futu filtered spot | market=%s | strategy=%s | filters=%s | candidates=%s | rows=%s',
            normalized_market,
            strategy_type,
            len(filter_list),
            len(candidate_codes),
            len(result_df),
        )
        return result_df.reset_index(drop=True)

    def should_use_as_prefilter(self, market: str) -> bool:
        normalized_market = self._normalize_market(market)
        settings = get_settings()
        return self.is_enabled(normalized_market) and settings.market_data.provider_for_market(normalized_market) == 'futu'

    def get_stock_snapshot_detail(self, stock_code: str, market: str) -> pd.DataFrame:
        normalized_market = self._normalize_market(market)
        snapshot_df = self.client.get_market_snapshot([self.to_futu_code(stock_code, normalized_market)])
        if snapshot_df.empty:
            return pd.DataFrame()
        return self._map_snapshot_detail_frame(snapshot_df, normalized_market, stock_code)

    def get_stock_capital_flow(self, stock_code: str, market: str) -> pd.DataFrame:
        normalized_market = self._normalize_market(market)
        flow_df = self.client.get_capital_flow(self.to_futu_code(stock_code, normalized_market))
        if flow_df.empty:
            return pd.DataFrame()
        return self._map_capital_flow_frame(flow_df, normalized_market, stock_code)

    def get_stock_owner_plate(self, stock_code: str, market: str) -> pd.DataFrame:
        normalized_market = self._normalize_market(market)
        plate_df = self.client.get_owner_plate([self.to_futu_code(stock_code, normalized_market)])
        if plate_df.empty:
            return pd.DataFrame()
        return self._map_owner_plate_frame(plate_df, normalized_market, stock_code)

    def get_plate_list(self, market: str, plate_type: str) -> pd.DataFrame:
        normalized_market = self._normalize_market(market)
        futu_market = self._to_futu_market_enum(normalized_market)
        futu_plate_class = self._to_futu_plate_enum(plate_type)
        plate_df = self.client.get_plate_list(futu_market, futu_plate_class)
        if plate_df.empty:
            return pd.DataFrame()
        return self._map_plate_list_frame(plate_df, normalized_market, plate_type)

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

    def _map_snapshot_detail_frame(self, df: pd.DataFrame, market: str, stock_code: str) -> pd.DataFrame:
        frame = df.copy()
        frame['股票代码'] = stock_code
        mapped = pd.DataFrame({
            '代码': frame['股票代码'],
            '股票代码': frame['股票代码'],
            '名称': self._first_series(frame, ['name', 'stock_name']),
            '更新时间': self._first_series(frame, ['update_time']),
            '上市日期': self._first_series(frame, ['listing_date']),
            '最新价': self._first_series(frame, ['last_price']),
            '今开': self._first_series(frame, ['open_price']),
            '最高': self._first_series(frame, ['high_price']),
            '最低': self._first_series(frame, ['low_price']),
            '昨收': self._first_series(frame, ['prev_close_price']),
            '成交量': self._first_series(frame, ['volume']),
            '成交额': self._first_series(frame, ['turnover']),
            '换手率': self._first_series(frame, ['turnover_rate']),
            '每手股数': self._first_series(frame, ['lot_size']),
            '价格最小变动': self._first_series(frame, ['price_spread']),
            '振幅': self._first_series(frame, ['amplitude']),
            '均价': self._first_series(frame, ['avg_price']),
            '量比': self._first_series(frame, ['volume_ratio']),
            '52周最高': self._first_series(frame, ['highest52weeks_price']),
            '52周最低': self._first_series(frame, ['lowest52weeks_price']),
            '历史最高': self._first_series(frame, ['highest_history_price']),
            '历史最低': self._first_series(frame, ['lowest_history_price']),
            '已发行股份': self._first_series(frame, ['issued_shares']),
            '流通股本': self._first_series(frame, ['outstanding_shares']),
            '总市值': self._first_series(frame, ['total_market_val']),
            '流通市值': self._first_series(frame, ['circular_market_val']),
            '净资产': self._first_series(frame, ['net_asset']),
            '净利润': self._first_series(frame, ['net_profit']),
            '每股收益': self._first_series(frame, ['earning_per_share']),
            '每股净资产': self._first_series(frame, ['net_asset_per_share']),
            '收益率': self._first_series(frame, ['ey_ratio']),
            '市盈率': self._first_series(frame, ['pe_ratio']),
            '市净率': self._first_series(frame, ['pb_ratio']),
            '市盈率-TTM': self._first_series(frame, ['pe_ttm_ratio']),
            '股息-TTM': self._first_series(frame, ['dividend_ttm']),
            '股息率-TTM': self._first_series(frame, ['dividend_ratio_ttm']),
            '上一财年股息': self._first_series(frame, ['dividend_lfy']),
            '上一财年股息率': self._first_series(frame, ['dividend_lfy_ratio']),
            '盘前价格': self._first_series(frame, ['pre_price']),
            '盘前成交量': self._first_series(frame, ['pre_volume']),
            '盘前成交额': self._first_series(frame, ['pre_turnover']),
            '盘前涨跌额': self._first_series(frame, ['pre_change_val']),
            '盘前涨跌幅': self._first_series(frame, ['pre_change_rate']),
            '盘后价格': self._first_series(frame, ['after_price']),
            '盘后成交量': self._first_series(frame, ['after_volume']),
            '盘后成交额': self._first_series(frame, ['after_turnover']),
            '盘后涨跌额': self._first_series(frame, ['after_change_val']),
            '盘后涨跌幅': self._first_series(frame, ['after_change_rate']),
            '夜盘价格': self._first_series(frame, ['overnight_price']),
            '夜盘成交量': self._first_series(frame, ['overnight_volume']),
            '夜盘成交额': self._first_series(frame, ['overnight_turnover']),
            '夜盘涨跌额': self._first_series(frame, ['overnight_change_val']),
            '夜盘涨跌幅': self._first_series(frame, ['overnight_change_rate']),
        })
        return self._normalize_detail_schema(mapped, market, stock_code)

    def _normalize_detail_schema(self, df: pd.DataFrame, market: str, stock_code: str) -> pd.DataFrame:
        frame = df.copy()
        normalized_code = str(stock_code).strip().upper() if market == 'usa' else str(stock_code).strip().zfill(5)
        frame['股票代码'] = normalized_code
        frame['代码'] = normalized_code
        for column in self.DETAIL_NUMERIC_COLUMNS:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors='coerce')
        return frame.reset_index(drop=True)

    def _map_capital_flow_frame(self, df: pd.DataFrame, market: str, stock_code: str) -> pd.DataFrame:
        normalized_code = str(stock_code).strip().upper() if market == 'usa' else str(stock_code).strip().zfill(5)
        mapped = pd.DataFrame({
            '代码': normalized_code,
            '股票代码': normalized_code,
            '更新时间': self._first_series(df, ['last_valid_time']),
            '日期': self._first_series(df, ['capital_flow_item_time']),
            '资金净流入': self._first_series(df, ['in_flow']),
            '超大单净流入': self._first_series(df, ['super_in_flow']),
            '大单净流入': self._first_series(df, ['big_in_flow']),
            '中单净流入': self._first_series(df, ['mid_in_flow']),
            '小单净流入': self._first_series(df, ['sml_in_flow']),
            '主力净流入': self._first_series(df, ['main_in_flow']),
        })
        for column in self.CAPITAL_FLOW_NUMERIC_COLUMNS:
            if column in mapped.columns:
                mapped[column] = pd.to_numeric(mapped[column], errors='coerce')
        if '日期' in mapped.columns:
            mapped['日期'] = mapped['日期'].astype(str).str.slice(0, 19)
        return mapped.sort_values('日期').reset_index(drop=True)

    def _map_owner_plate_frame(self, df: pd.DataFrame, market: str, stock_code: str) -> pd.DataFrame:
        frame = df.copy()
        normalized_code = str(stock_code).strip().upper() if market == 'usa' else str(stock_code).strip().zfill(5)
        mapped = pd.DataFrame({
            '代码': normalized_code,
            '股票代码': normalized_code,
            '名称': self._first_series(frame, ['name', 'stock_name']),
            '板块代码': self._first_series(frame, ['plate_code']),
            '所属板块': self._first_series(frame, ['plate_name']),
            '板块类型': self._first_series(frame, ['plate_type']),
        })
        if '板块类型' in mapped.columns:
            mapped['板块类型'] = mapped['板块类型'].astype(str).map(lambda value: self.OWNER_PLATE_TYPE_MAP.get(value, value))
        return mapped.drop_duplicates().reset_index(drop=True)

    def _map_plate_list_frame(self, df: pd.DataFrame, market: str, plate_type: str) -> pd.DataFrame:
        frame = df.copy()
        normalized_type = self.OWNER_PLATE_TYPE_MAP.get(self.PLATE_CLASS_MAP.get(str(plate_type).strip().lower(), ''), plate_type)
        mapped = pd.DataFrame({
            '代码': self._first_series(frame, ['code']),
            '板块代码': self._first_series(frame, ['code']),
            '板块名称': self._first_series(frame, ['plate_name']),
            '板块ID': self._first_series(frame, ['plate_id']),
            '板块类型': normalized_type,
            'market': market,
        })
        return mapped.drop_duplicates().reset_index(drop=True)

    @staticmethod
    def _to_futu_market_enum(market: str):
        if Market is None:
            raise RuntimeError('futu-api is not installed')
        if market == 'H':
            return Market.HK
        if market == 'usa':
            return Market.US
        raise ValueError(f'Unsupported market for Futu provider: {market}')

    @classmethod
    def _to_futu_plate_enum(cls, plate_type: str):
        if Plate is None:
            raise RuntimeError('futu-api is not installed')
        normalized = str(plate_type or '').strip().lower()
        plate_name = cls.PLATE_CLASS_MAP.get(normalized)
        if not plate_name:
            raise ValueError(f'Unsupported plate type: {plate_type}')
        return getattr(Plate, plate_name)

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
        code_series = frame.get('code', pd.Series(index=frame.index, dtype='object'))
        if isinstance(code_series, str):
            frame['股票代码'] = self.from_futu_code(code_series, market)
        else:
            frame['股票代码'] = code_series.apply(lambda value: self.from_futu_code(value, market))
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
    def _format_futu_history_date(date_text: str) -> str:
        normalized = str(date_text or '').replace('-', '').strip()
        if len(normalized) != 8 or not normalized.isdigit():
            raise ValueError(f'Invalid history date: {date_text}')
        return f'{normalized[:4]}-{normalized[4:6]}-{normalized[6:8]}'

    @staticmethod
    def _first_series(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
        for column in candidates:
            if column in df.columns:
                return df[column]
        return pd.Series(index=df.index, dtype='object')
