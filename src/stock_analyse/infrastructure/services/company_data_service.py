import akshare as ak
import datetime
import pandas as pd
from stock_analyse.infrastructure.config.settings import get_settings
from stock_analyse.infrastructure.data_sources.concepts.ths_concept_client import stockConceptData
from stock_analyse.infrastructure.data_sources.news.eastmoney_news_client import stockNewsData
from stock_analyse.infrastructure.data_sources.searxng_client import SearxngClient
from stock_analyse.infrastructure.analysis.technical_indicator_calculator import stockAKIndicator
from stock_analyse.shared.report_date_utils import ReportDateUtils
from stock_analyse.infrastructure.persistence.file_cache import FileCacheUtils
from stock_analyse.infrastructure.services.concept_service import stockConcepService
from stock_analyse.infrastructure.services.futu_market_data_provider import FutuMarketDataProvider
import traceback
from stock_analyse.infrastructure.persistence.mysql_cache import MySQLCache
import time
from tqdm import tqdm

import logging

try:
    from futu import OpenQuoteContext
except ImportError:
    OpenQuoteContext = None

# 个股相关信息查询
"""
  需要缓存的数据：
  获取所有股票数据  stock_fund_flow_individual_df = ak.stock_fund_flow_individual(symbol="即时")
  获取所有板块信息: stock_board = stock_concept_service.stock_board_concept_name_ths()
"""
class stockCompanyInfo:
    _MYSQL_CACHE: MySQLCache | None = None

    def __init__(self, marker='sz', symbol="002624"):
        # 定义 current_date 并格式化
        self.market = marker
        self.symbol = symbol
        self.xq_a_token = 'a9afe36f1d53c5f7180395537db631d013033091'
        # 新增变量 usa 和 ETF
        self.usa = 'usa'
        self.ETF = 'zq'
        # 新增变量 HongKong
        self.HongKong = 'H'
        self.logger = logging.getLogger(__name__)
        # self.border = stockBorderInfo(self.market)
        self.report_util = ReportDateUtils()
        self.reportUtils = self.report_util
        self.cache_service = FileCacheUtils(market=self.market)
        self.searxng = SearxngClient()
        if stockCompanyInfo._MYSQL_CACHE is None:
            stockCompanyInfo._MYSQL_CACHE = MySQLCache()
        self.mysql = stockCompanyInfo._MYSQL_CACHE

    def _normalized_market(self):
        return get_settings().market_data.normalize_market(self.market)

    def _should_use_futu_provider(self):
        normalized_market = self._normalized_market()
        if normalized_market == self.usa:
            return FutuMarketDataProvider.is_enabled(normalized_market)
        if normalized_market == self.HongKong:
            return FutuMarketDataProvider.is_enabled(normalized_market) or FutuMarketDataProvider.is_enabled('HK')
        return False

    def _provider_market(self):
        normalized_market = self._normalized_market()
        if normalized_market == self.HongKong:
            return 'HK' if FutuMarketDataProvider.is_enabled('HK') and not FutuMarketDataProvider.is_enabled(normalized_market) else normalized_market
        return normalized_market

    def _get_futu_provider(self):
        provider_market = self._provider_market()
        return FutuMarketDataProvider(provider_market)

    def _normalize_symbol_for_provider(self, symbol=None):
        target_symbol = self.symbol if symbol is None else symbol
        if self._normalized_market() == self.usa:
            parts = str(target_symbol).split('.', 1)
            return parts[1] if len(parts) > 1 else str(target_symbol)
        return str(target_symbol)

    @staticmethod
    def _get_first_non_empty(frame, columns):
        if frame is None or frame.empty:
            return ''
        for column in columns:
            if column in frame.columns:
                series = frame[column].dropna()
                if not series.empty:
                    value = series.iloc[0]
                    return '' if pd.isna(value) else value
        return ''

    def _build_futu_financial_snapshot(self, detail_df):
        if detail_df is None or detail_df.empty:
            return pd.DataFrame()
        snapshot = pd.DataFrame([{
            '股票代码': self._normalize_symbol_for_provider(),
            '报告期': '',
            '报告日期': '',
            '总市值': self._get_first_non_empty(detail_df, ['总市值']),
            '流通市值': self._get_first_non_empty(detail_df, ['流通市值']),
            '市盈率': self._get_first_non_empty(detail_df, ['市盈率']),
            '市盈率-TTM': self._get_first_non_empty(detail_df, ['市盈率-TTM']),
            '市净率': self._get_first_non_empty(detail_df, ['市净率']),
            '每股收益': self._get_first_non_empty(detail_df, ['每股收益']),
            '每股净资产': self._get_first_non_empty(detail_df, ['每股净资产']),
            '净利润': self._get_first_non_empty(detail_df, ['净利润']),
            '净资产': self._get_first_non_empty(detail_df, ['净资产']),
            '股息-TTM': self._get_first_non_empty(detail_df, ['股息-TTM']),
            '股息率-TTM': self._get_first_non_empty(detail_df, ['股息率-TTM']),
            '收益率': self._get_first_non_empty(detail_df, ['收益率']),
            '上市日期': self._get_first_non_empty(detail_df, ['上市日期']),
            '名称': self._get_first_non_empty(detail_df, ['名称']),
        }])
        return snapshot

    def _get_futu_stock_name(self):
        if not self._should_use_futu_provider() or OpenQuoteContext is None:
            return ''
        provider = self._get_futu_provider()
        symbol = self._normalize_symbol_for_provider()
        snapshot_df = provider.get_stock_snapshot_detail(symbol, self._normalized_market())
        return str(self._get_first_non_empty(snapshot_df, ['名称'])).strip()

    #  获取概念板块名称
    def get_stock_board_all_concept_name(self):
        """
        获取概念板块名称
        :return:
        """
        if self._should_use_futu_provider():
            return self._get_futu_provider().get_plate_list(self._normalized_market(), 'concept')
        if self.market == self.HongKong or self.market == self.usa:
            return self.get_default_df()
        concept_sectors = self.mysql.read_from_cache(date='20250331', report_type='stock_concept_data')
        # concept_sectors = self.cache_service.read_from_serialized(date='20250331', report_type='stock_concept_data');
        if concept_sectors is None or concept_sectors.empty:
            stock_concept_service = stockConcepService()
            concept_sectors, industry_sectors = stock_concept_service.get_all_sectors_and_stocks()
        return concept_sectors

    def get_stock_board_all_industry_name(self):
        """
        获取行业信息
        :return:
        """
        if self._should_use_futu_provider():
            return self._get_futu_provider().get_plate_list(self._normalized_market(), 'industry')
        if self.market == self.HongKong or self.market == self.usa:
            return self.get_default_df()
        industry_sectors = self.mysql.read_from_cache(date='20250331', report_type='stock_industry_data')
        # industry_sectors = self.cache_service.read_from_serialized(date='20250331', report_type='stock_industry_data');
        if industry_sectors is None or industry_sectors.empty:
            stock_concept_service = stockConcepService()
            concept_sectors, industry_sectors = stock_concept_service.get_all_sectors_and_stocks()
        return industry_sectors

    def get_stock_board_concept_name(self):
        """
        获取概念板块名称
        :return:
        """
        if self.market == self.HongKong or self.market == self.usa:
            return self.get_default_df()

        df_concept = self.mysql.read_from_cache(date='20250331', report_type='stock_concept_data')
        if df_concept is not None and not df_concept.empty:
            return df_concept

        stock_concept_service = stockConcepService()
        df_concept, df_industry = stock_concept_service.get_all_sectors_and_stocks()

        return df_concept
    def get_stock_concept_by_name(self,concept_name,industry_sectors):
        """
        获取概念板块名称
        :return:
        """
        if self.market  == self.HongKong or self.market == self.usa:
            return self.get_default_df()
        report_type = 'stock_concept_data'
        date = '20250331'
        df_data = self.mysql.read_from_cache(date=date, report_type=report_type,
                                             conditions={"所属板块": concept_name})
        if df_data is  None or  df_data.empty:
            # 获取成分股
            df_data = ak.stock_board_industry_cons_em(symbol=concept_name)
            df_data["所属板块"] = concept_name
            df_data["板块类型"] = "行业"
            self.mysql.write_to_cache(date=date, report_type=report_type,
                                      data=df_data)
        return df_data

    def get_stock_industry_by_name(self, concept_name, industry_sectors):
        """
        获取概念板块名称
        :return:
        """
        if self.market == self.HongKong or self.market == self.usa:
            return self.get_default_df()
        report_type = 'stock_industry_data'
        date = '20250331'
        df_data = self.mysql.read_from_cache(date=date, report_type=report_type,
                                             conditions={"所属板块": concept_name})
        if df_data is None or df_data.empty:
            # 获取成分股
            df_data = ak.stock_board_industry_cons_em(symbol=concept_name)
            df_data["所属板块"] = concept_name
            df_data["板块类型"] = "行业"
            self.mysql.write_to_cache(date=date, report_type=report_type,
                                      data=df_data)
        return df_data

    def get_stock_industry_by_code(self, code, date='2025-08-06'):
        """
        获取行业板块名称
        :return:
        """
        border_name = ''
        normalized_market = self._normalized_market()
        
        # 1. 尝试从 Futu 供应者获取
        if self._should_use_futu_provider():
            plate_df = self._get_futu_provider().get_stock_owner_plate(self._normalize_symbol_for_provider(code), normalized_market)
            if plate_df is not None and not plate_df.empty:
                industry_df = plate_df[plate_df['板块类型'].astype(str) == '行业'] if '板块类型' in plate_df.columns else pd.DataFrame()
                if not industry_df.empty and '所属板块' in industry_df.columns:
                    return ','.join(industry_df['所属板块'].astype(str).drop_duplicates())
        
        # 2. 如果 Futu 未获取到，或者未启用 Futu，则尝试本地 MySQL
        # 确定表类型和列名适配
        if normalized_market == 'SH':
            report_type = 'stock_industry_data'
            code_col = '代码'
            plate_col = '所属板块'
        else:
            report_type = 'stock_industry_data' # MySQLCache 会映射到 stock_industry_usa/H
            code_col = 'code'
            plate_col = 'plate_name'

        date_cache = '20250331'
        if normalized_market == 'SH': date_cache = '20260517' # 对应我们刚才 Seed 的数据
        
        # 处理代码逻辑：兼容前缀
        search_codes = [code]
        clean_code = code
        if '.' in code:
            clean_code = code.split('.')[-1]
            search_codes.append(clean_code)
        
        # 补全前缀逻辑
        if normalized_market == 'usa':
            if not code.startswith('US.'): search_codes.append(f'US.{clean_code}')
        elif normalized_market == 'H':
            if not code.startswith('HK.'): search_codes.append(f'HK.{clean_code}')
        
        try:
            unique_search_codes = list(dict.fromkeys(search_codes))
            for c in unique_search_codes:
                df_data = self.mysql.read_from_cache(date=date_cache, report_type=report_type,
                                                     market=normalized_market,
                                                     conditions={code_col: c})
                if df_data is not None and not df_data.empty:
                    # 针对 A 股原生 data 表的额外过滤
                    if normalized_market == 'SH' and '板块类型' in df_data.columns:
                        df_data = df_data[df_data['板块类型'].astype(str) == '行业']
                    
                    # 针对港美股的过滤
                    if normalized_market in ('H', 'usa') and 'plate_type' in df_data.columns:
                        df_data = df_data[df_data['plate_type'].astype(str) == '行业']
                    
                    if df_data.empty: continue

                    for col in [plate_col, '所属板块', 'plate_name', '板块名称']:
                        if col in df_data.columns:
                            border_name = ','.join(df_data[col].astype(str).drop_duplicates())
                            if border_name:
                                return border_name
        except Exception as e:
            self.logger.warning(f"获取 {code} 行业信息失败 (MySQL): {e}")
            
        return "行业" if not border_name else border_name

    def get_stock_concept_by_code(self, code, date='2025-08-06'):
        """
        获取概念板块名称
        :return:
        """
        concept_name = ''
        normalized_market = self._normalized_market()
        
        # 1. 尝试从 Futu 供应者获取
        if self._should_use_futu_provider():
            plate_df = self._get_futu_provider().get_stock_owner_plate(self._normalize_symbol_for_provider(code), normalized_market)
            if plate_df is not None and not plate_df.empty:
                concept_df = plate_df[plate_df['板块类型'].astype(str) == '概念'] if '板块类型' in plate_df.columns else pd.DataFrame()
                if not concept_df.empty and '所属板块' in concept_df.columns:
                    return ','.join(concept_df['所属板块'].astype(str).drop_duplicates())
        
        # 2. 尝试本地 MySQL
        if normalized_market == 'SH':
            report_type = 'stock_concept_data'
            code_col = '代码'
            plate_col = '所属板块'
        else:
            report_type = 'stock_industry_data'
            code_col = 'code'
            plate_col = 'plate_name'

        date_cache = '20250331'
        if normalized_market == 'SH': date_cache = '20260517'

        search_codes = [code]
        clean_code = code
        if '.' in code:
            clean_code = code.split('.')[-1]
            search_codes.append(clean_code)
            
        if normalized_market == 'usa':
            if not code.startswith('US.'): search_codes.append(f'US.{clean_code}')
        elif normalized_market == 'H':
            if not code.startswith('HK.'): search_codes.append(f'HK.{clean_code}')

        try:
            unique_search_codes = list(dict.fromkeys(search_codes))
            for c in unique_search_codes:
                df_data = self.mysql.read_from_cache(date=date_cache, report_type=report_type,
                                                     market=normalized_market,
                                                     conditions={code_col: c})
                if df_data is not None and not df_data.empty:
                    # 针对 A 股
                    if normalized_market == 'SH' and '板块类型' in df_data.columns:
                        df_data = df_data[df_data['板块类型'].astype(str) == '概念']
                    # 针对港美股
                    if normalized_market in ('H', 'usa') and 'plate_type' in df_data.columns:
                        df_data = df_data[df_data['plate_type'].astype(str) == '概念']
                    
                    if df_data.empty: continue

                    for col in [plate_col, '所属板块', 'plate_name', '板块名称']:
                        if col in df_data.columns:
                            concept_name = ','.join(df_data[col].astype(str).drop_duplicates())
                            if concept_name:
                                return concept_name
        except Exception as e:
            self.logger.warning(f"获取 {code} 概念信息失败 (MySQL): {e}")
            
        return "" if not concept_name else concept_name
    def save_stock_board_all_concept_name(self):
        """
        获取概念板块名称
        :return:
        """
        if self.market  == self.HongKong or self.market == self.usa:
            return self.get_default_df()
        stock_concept_service = stockConceptData()
        industry_sectors = stock_concept_service.stock_board_concept_name_ths()
        # self.mysql.write_to_cache(date='20250331',report_type='stock_concept_thx',data = industry_sectors)
        # 获取行业板块成分股
        print("获取行业板块成分股...")
        industry_stocks_list = []
        for _, row in tqdm(industry_sectors.iterrows(), total=len(industry_sectors), desc="行业板块进度"):
            sector_name = row["概念名称"]
            try:
                # 获取成分股
                stocks = stock_concept_service.stock_board_concept_cons_ths(symbol=sector_name,
                                                                     stock_board_ths_map_df=industry_sectors)
                stocks["概念名称"] = sector_name
                # stocks["板块类型"] = "行业"
                self.mysql.write_to_cache(date='20250331', report_type='stock_concept_thx_data',
                                          data=stocks)

                # 避免请求过于频繁
                time.sleep(1)
            except Exception as e:
                print(f"获取{sector_name}成分股失败: {e}")
                continue
        return industry_sectors


    # 主营业务介绍 根据主营业务网络搜索相关事件报道
    def get_stock_zyjs(self):
        # 修改：将日期改为固定值 'total'，实现跨日期持久化缓存
        current_date = 'total'
        report_type = f"{self.market}_{self.symbol}_stock_zyjs"
        cached_data = self.cache_service.read_from_serialized(current_date, report_type)
        if cached_data is not None:
            return cached_data
        if self.market  == self.HongKong or self.market == self.usa:
            result = self.get_default_df()
        else:
            result = ak.stock_zyjs_ths(symbol=self.symbol)
        if result is not None:
            self.cache_service.write_to_cache_serialized(current_date, report_type, result)
        return result

    def get_stock_name(self):
        """ 获取股票名称"""
        # 修改：将日期改为固定值 'total'，实现跨日期持久化缓存
        current_date = 'total'
        report_type = f"{self.market}_{self.symbol}_stock_name"
        cached_data = self.cache_service.read_from_serialized(current_date, report_type)
        if cached_data is not None:
            return cached_data
        try:
            if self.market == self.ETF:
                stock_name = self.symbol
            elif self._should_use_futu_provider():
                stock_name = self._get_futu_stock_name() or self.symbol
            elif self.market == self.HongKong:
                stock_individual = ak.stock_individual_basic_info_hk_xq(symbol=self.symbol, token=self.xq_a_token)
                stock_name = stock_individual[stock_individual['item'] == 'comcnname']['value'].values[0]
            elif self.market == self.usa:
                symbol = self.get_usa_code()
                stock_individual_info_em_df = ak.stock_individual_basic_info_us_xq(symbol=symbol, token=self.xq_a_token)
                stock_name = stock_individual_info_em_df[stock_individual_info_em_df['item'] == 'org_name_cn']['value'].values[0]
            else:
                stock_individual_info_em_df = ak.stock_individual_info_em(symbol=self.symbol)
                stock_name = stock_individual_info_em_df[stock_individual_info_em_df['item'] == '股票简称']['value'].values[0]
            self.cache_service.write_to_cache_serialized(current_date, report_type, stock_name)
            return stock_name
        except Exception as e:
            self.logger.error(f"调用方法时发生属性错误，请检查对象是否正确初始化: {e}")
            traceback.print_exc()
            return self.symbol

    # 个股信息查询
    def get_stock_individual_info(self):
        """
        个股信息查询
        # [comunic 公司代码，comcnname 公司中文名称，comenname 公司英文名称，incdate 成立日期，rgiofc 注册办公地址，hofclctmbu 总部办公地址，chairman 董事长，
        mbu 主营业务，comintr 公司简介，refccomty 参考社区，numtissh 发行股数，ispr 发行价格，nrfd 净资产，
        nation_name 国家名称，tel 联系电话，fax 传真号码，email 电子邮箱，web_site 官方网站，lsdateipo 上市日期，mainholder 主要股东]

        :return:
        """
        # 修改：将日期改为固定值 'total'，实现跨日期持久化缓存
        current_date = 'total'
        report_type = f"{self.market}_{self.symbol}_stock_individual_info"
        cached_data = self.cache_service.read_from_serialized(current_date, report_type)
        if cached_data is not None:
            return cached_data
        try:
            if self.market == self.ETF:
                result = self.get_default_df()
            elif self._should_use_futu_provider():
                result = self._get_futu_provider().get_stock_snapshot_detail(self._normalize_symbol_for_provider(), self._normalized_market())
            elif self.market == self.HongKong:
                result = ak.stock_individual_basic_info_hk_xq(symbol=self.symbol, token=self.xq_a_token)
            elif self.market == self.usa:
                symbol = self.get_usa_code()
                result = ak.stock_individual_basic_info_us_xq(symbol=symbol, token=self.xq_a_token)
            else:
                result = ak.stock_individual_info_em(symbol=self.symbol)
            if result is not None:
                self.cache_service.write_to_cache_serialized(current_date, report_type, result)
            return result
        except  Exception as e:
            self.logger.error(f"get_stock_individual_info error {self.symbol} 错误: {e}")
            traceback.print_exc()
            return self.get_default_df()


    # 个股信息查询
    def get_stock_individual_info_em(self):

        # 个股信息查询,功能同上
        default_df = self.get_default_df()
        default_list_date = "2000-01-01"
        default_industry = "None"
        try:
            if self.market == self.ETF:
                default_industry = "EFT"
                return default_df, default_list_date, default_industry
            elif self._should_use_futu_provider():
                stock_individual_info_em_df = self._get_futu_provider().get_stock_snapshot_detail(self._normalize_symbol_for_provider(), self._normalized_market())
                list_date = str(self._get_first_non_empty(stock_individual_info_em_df, ['上市日期']) or default_list_date)
                industry = self.get_stock_industry_by_code(self._normalize_symbol_for_provider()) or default_industry
                return stock_individual_info_em_df, list_date, industry
            elif self.market == self.HongKong:
                stock_individual_info_em_df = ak.stock_individual_basic_info_hk_xq(symbol=self.symbol,
                                                                                   token=self.xq_a_token)
                list_date = '2010-01-01'
                industry = self.get_stock_industry_by_code(self.symbol) or ''
                return stock_individual_info_em_df, list_date, industry
            elif self.market == self.usa:
                symbol = self.get_usa_code()
                stock_individual_info_em_df = ak.stock_individual_basic_info_us_xq(symbol=symbol, token=self.xq_a_token)
                list_date = '2010-01-01'
                industry = self.get_stock_industry_by_code(symbol) or ''
                return stock_individual_info_em_df, list_date, industry
            else:
                stock_individual_info_em_df = ak.stock_individual_info_em(symbol=self.symbol)
                list_date = stock_individual_info_em_df[stock_individual_info_em_df['item'] == '上市时间']['value'].values[0]
                industry = stock_individual_info_em_df[stock_individual_info_em_df['item'] == '行业']['value'].values[0]
                return stock_individual_info_em_df, list_date, industry
        except Exception as e:
            self.logger.error(f"get_stock_individual_info_em stock:{self.symbol} Error occurred: {e}")
            traceback.print_exc()
            return  default_df, default_list_date, default_industry



    # 个股新闻查询
    def get_stock_news(self):
        stock_news_em_df = stockNewsData.stock_news_em(symbol=self.symbol, pageSize=10)
        if stock_news_em_df is None or stock_news_em_df.empty:
            fallback_records = self.searxng.search(
                query=f'{self.symbol} stock latest news',
                limit=10,
                category='news',
                time_range='month',
            )
            if not fallback_records:
                return pd.DataFrame()
            stock_news_em_df = pd.DataFrame([
                {
                    '关键词': self.symbol,
                    '新闻标题': item.get('title', ''),
                    '新闻内容': item.get('content', ''),
                    '发布时间': item.get('publishedDate', '') or item.get('published_date', '') or '',
                    '文章来源': ', '.join(item.get('engines', []) or []),
                    '新闻链接': item.get('url', ''),
                    'source': 'searxng',
                }
                for item in fallback_records
            ])
        if '文章来源' in stock_news_em_df.columns and '新闻链接' in stock_news_em_df.columns:
            stock_news_em_df = stock_news_em_df.drop(["文章来源", "新闻链接"], axis=1)
        return stock_news_em_df

    # # 获取当前个股所在行业板块情况
    def get_stock_fund_flow(self):
        # 获取当前个股所在行业板块情况
        # [序号，名称，今日涨跌幅，主力净流入 - 净额，主力净流入 - 净占比，超大单净流入 - 净额，超大单净流入 - 净占比，大单净流入 - 净额，大单净流入 - 净占比，中单净流入 - 净额，中单净流入 - 净占比，小单净流入 - 净额，小单净流入 - 净占比，主力净流入最大股]
        try:
            stock_sector_fund_flow_rank_df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
            return stock_sector_fund_flow_rank_df
        except Exception as e:
            self.logger.error(f"get_stock_fund_flow Error occurred: {e}")
            return pd.DataFrame()


    # 历史的个股资金流
    def get_stock_individual_fund_flow(self):
        # 历史的个股资金流
        current_date = self.reportUtils.get_current__history_date_str()
        report_type = f"{self.market}_{self.symbol}_individual_fund_flow"
        cached_data = self.cache_service.read_from_serialized(current_date, report_type)
        if cached_data is not None and not getattr(cached_data, 'empty', False):
            return cached_data
        try:
            if not isinstance(self.market, str):
                raise ValueError(f'market 必须是字符串，当前类型: {type(self.market).__name__}')
            normalized_market = self.market.strip()
            market_name = normalized_market.lower()
            if self._should_use_futu_provider():
                result = self._get_futu_provider().get_stock_capital_flow(self._normalize_symbol_for_provider(), self._normalized_market())
            elif(normalized_market == self.HongKong or normalized_market == self.usa):
                result = self.get_default_df()
            else:
                if(normalized_market == 'zq'):
                    market_name = 'sh'
                stock_individual_fund_flow_df = ak.stock_individual_fund_flow(stock=self.symbol, market=market_name)
                stock_individual_fund_flow_df['日期'] = pd.to_datetime(stock_individual_fund_flow_df['日期'])
                sorted_data = stock_individual_fund_flow_df.sort_values(by='日期', ascending=False)
                num_records = min(20, len(sorted_data))
                result = sorted_data.head(num_records)
            if result is not None and not getattr(result, 'empty', False):
                self.cache_service.write_to_cache_serialized(current_date, report_type, result)
            return result
        except Exception as e:
            self.logger.error(f"Error occurred: {e}")
            return pd.DataFrame()


     # 财务指标
    def get_stock_financial_analysis_indicator(self,start_year="2024"):

        cache = True
        report_type = self.market +"_" +self.symbol+'_financial_indicator_window'
        current_date = f"{start_year}_5y"
        stock_financial_analysis_indicator_df = self.cache_service.read_from_serialized(current_date, report_type)
        if cache and stock_financial_analysis_indicator_df is not None:
            return stock_financial_analysis_indicator_df

        if self._should_use_futu_provider():
            stock_financial_analysis_indicator_df = self._get_futu_provider().get_stock_financials_statements(
                self._normalize_symbol_for_provider(), self._normalized_market()
            )
            stock_financial_analysis_indicator_df = self._filter_financial_indicator_window(
                df=stock_financial_analysis_indicator_df,
                start_year=start_year,
            )
        elif self.market  == self.HongKong:
            #SECUCODE, SECURITY_CODE, SECURITY_NAME_ABBR, ORG_CODE, REPORT_DATE, DATE_TYPE_CODE, PER_NETCASH_OPERATE, PER_OI, BPS, BASIC_EPS, DILUTED_EPS, OPERATE_INCOME, OPERATE_INCOME_YOY, GROSS_PROFIT, GROSS_PROFIT_YOY, HOLDER_PROFIT, HOLDER_PROFIT_YOY, GROSS_PROFIT_RATIO, EPS_TTM, OPERATE_INCOME_QOQ, NET_PROFIT_RATIO, ROE_AVG, GROSS_PROFIT_QOQ, ROA, HOLDER_PROFIT_QOQ, ROE_YEARLY, ROIC_YEARLY, TAX_EBT, OCF_SALES, DEBT_ASSET_RATIO, CURRENT_RATIO, CURRENTDEBT_DEBT, START_DATE, FISCAL_YEAR, CURRENCY, IS_CNY_CODE
            # 证券代码, 股票代码, 股票简称, 机构代码, 报告日期, 数据类型代码, 经营活动每股净现金流量, 每股经营活动现金流量, 每股净资产, 基本每股收益, 稀释每股收益, 营业收入, 营业收入同比增长率, 毛利润, 毛利润同比增长率, 归属于母公司股东的净利润, 归属于母公司股东的净利润同比增长率, 毛利率, 滚动市盈率每股收益, 营业收入环比增长率, 净利率, 平均净资产收益率, 毛利润环比增长率, 总资产收益率, 归属于母公司股东的净利润环比增长率, 年度净资产收益率, 年度投入资本回报率, 息税前利润税负, 销售商品、提供劳务收到的现金占营业收入比重, 资产负债率, 流动比率, 流动负债占总负债比重, 起始日期, 会计年度, 货币类型, 是否人民币代码
            stock_financial_analysis_indicator_df = ak.stock_financial_hk_analysis_indicator_em(symbol=self.symbol)
            stock_financial_analysis_indicator_df = self.report_util.financial_indicator_map_hk_fields(df = stock_financial_analysis_indicator_df)
            # return stock_financial_analysis_indicator_df
        elif self.market == self.usa:
            symbol = self.get_usa_code()
            # SECUCODE, SECURITY_CODE, SECURITY_NAME_ABBR, ORG_CODE, SECURITY_INNER_CODE, ACCOUNTING_STANDARDS, NOTICE_DATE, START_DATE, REPORT_DATE, FINANCIAL_DATE, STD_REPORT_DATE, CURRENCY, DATE_TYPE, DATE_TYPE_CODE, REPORT_TYPE, REPORT_DATA_TYPE, ORGTYPE, OPERATE_INCOME, OPERATE_INCOME_YOY, GROSS_PROFIT, GROSS_PROFIT_YOY, PARENT_HOLDER_NETPROFIT, PARENT_HOLDER_NETPROFIT_YOY, BASIC_EPS, DILUTED_EPS, GROSS_PROFIT_RATIO, NET_PROFIT_RATIO, ACCOUNTS_RECE_TR, INVENTORY_TR, TOTAL_ASSETS_TR, ACCOUNTS_RECE_TDAYS, INVENTORY_TDAYS, TOTAL_ASSETS_TDAYS, ROE_AVG, ROA, CURRENT_RATIO, SPEED_RATIO, OCF_LIQDEBT, DEBT_ASSET_RATIO, EQUITY_RATIO, BASIC_EPS_YOY, GROSS_PROFIT_RATIO_YOY, NET_PROFIT_RATIO_YOY, ROE_AVG_YOY, ROA_YOY, DEBT_ASSET_RATIO_YOY, CURRENT_RATIO_YOY, SPEED_RATIO_YOY
            # 证券代码, 股票代码, 股票简称, 机构代码, 证券内部代码, 会计准则, 公告日期, 起始日期, 报告日期, 财务日期, 标准报告日期, 货币类型, 日期类型, 数据类型代码, 报告类型, 报告数据类型, 机构类型, 营业收入, 营业收入同比增长率, 毛利润, 毛利润同比增长率, 归属于母公司股东净利润, 归属于母公司股东净利润同比增长率, 基本每股收益, 稀释每股收益, 毛利率, 净利率, 应收账款周转率, 存货周转率, 总资产周转率, 应收账款周转天数, 存货周转天数, 总资产周转天数, 平均净资产收益率, 总资产收益率, 流动比率, 速动比率, 经营活动现金流净额与流动负债比率, 资产负债率, 股东权益比率, 基本每股收益同比增长率, 毛利率同比增长率, 净利率同比增长率, 平均净资产收益率同比增长率, 总资产收益率同比增长率, 资产负债率同比增长率, 流动比率同比增长率, 速动比率同比增长率
            stock_financial_analysis_indicator_df = ak.stock_financial_us_analysis_indicator_em(symbol=symbol)
            stock_financial_analysis_indicator_df = self.report_util.financial_indicator_map_usa_fields(df = stock_financial_analysis_indicator_df)
            # return stock_financial_analysis_indicator_df
        else:
            # 日期, 摊薄每股收益(元), 加权每股收益(元), 每股收益_调整后(元), 扣除非经常性损益后的每股收益(元), 每股净资产_调整前(元), 每股净资产_调整后(元), 每股经营性现金流(元), 每股资本公积金(元), 每股未分配利润(元), 调整后的每股净资产(元), 总资产利润率(%), 主营业务利润率(%), 总资产净利润率(%), 成本费用利润率(%), 营业利润率(%), 主营业务成本率(%), 销售净利率(%), 股本报酬率(%), 净资产报酬率(%), 资产报酬率(%), 销售毛利率(%), 三项费用比重, 非主营比重, 主营利润比重, 股息发放率(%), 投资收益率(%), 主营业务利润(元), 净资产收益率(%), 加权净资产收益率(%), 扣除非经常性损益后的净利润(元), 主营业务收入增长率(%), 净利润增长率(%), 净资产增长率(%), 总资产增长率(%), 应收账款周转率(次), 应收账款周转天数(天), 存货周转天数(天), 存货周转率(次), 固定资产周转率(次), 总资产周转率(次), 总资产周转天数(天), 流动资产周转率(次), 流动资产周转天数(天), 股东权益周转率(次), 流动比率, 速动比率, 现金比率(%), 利息支付倍数, 长期债务与营运资金比率(%), 股东权益比率(%), 长期负债比率(%), 股东权益与固定资产比率(%), 负债与所有者权益比率(%), 长期资产与长期资金比率(%), 资本化比率(%), 固定资产净值率(%), 资本固定化比率(%), 产权比率(%), 清算价值比率(%), 固定资产比重(%), 资产负债率(%), 总资产(元), 经营现金净流量对销售收入比率(%), 资产的经营现金流量回报率(%), 经营现金净流量与净利润的比率(%), 经营现金净流量对负债比率(%), 现金流量比率(%), 短期股票投资(元), 短期债券投资(元), 短期其它经营性投资(元), 长期股票投资(元), 长期债券投资(元), 长期其它经营性投资(元), 1年以内应收帐款(元), 1-2年以内应收帐款(元), 2-3年以内应收帐款(元), 3年以内应收帐款(元), 1年以内预付货款(元), 1-2年以内预付货款(元), 2-3年以内预付货款(元), 3年以内预付货款(元), 1年以内其它应收款(元), 1-2年以内其它应收款(元), 2-3年以内其它应收款(元), 3年以内其它应收款(元)
            # date, diluted_eps(yuan), weighted_eps(yuan), adjusted_eps(yuan), eps_excluding_non_recurring(yuan), adjusted_net_assets_per_share_before(yuan), adjusted_net_assets_per_share_after(yuan), operating_cash_flow_per_share(yuan), capital_surplus_per_share(yuan), undistributed_profit_per_share(yuan), adjusted_net_assets_per_share(yuan), total_assets_profit_rate(%), main_business_profit_rate(%), total_assets_net_profit_rate(%), cost_expense_profit_rate(%), operating_profit_rate(%), main_business_cost_rate(%), sales_net_profit_rate(%), share_capital_remuneration_rate(%), net_assets_remuneration_rate(%), assets_remuneration_rate(%), gross_profit_rate(%), three_expenses_ratio, non_main_business_ratio, main_profit_ratio, dividend_payout_rate(%), investment_return_rate(%), main_business_profit(yuan), return_on_equity(%), weighted_return_on_equity(%), net_profit_excluding_non_recurring(yuan), main_business_income_growth_rate(%), net_profit_growth_rate(%), net_assets_growth_rate(%), total_assets_growth_rate(%), accounts_receivable_turnover(times), accounts_receivable_days(days), inventory_days(days), inventory_turnover(times), fixed_assets_turnover(times), total_assets_turnover(times), total_assets_days(days), current_assets_turnover(times), current_assets_days(days), shareholder_equity_turnover(times), current_ratio, quick_ratio, cash_ratio(%), interest_coverage_ratio, long_term_debt_to_working_capital_ratio(%), shareholder_equity_ratio(%), long_term_debt_ratio(%), shareholder_equity_to_fixed_assets_ratio(%), debt_to_equity_ratio(%), long_term_assets_to_long_term_funds_ratio(%), capitalization_ratio(%), fixed_assets_net_value_ratio(%), capital_immobilization_ratio(%), equity_ratio(%), liquidation_value_ratio(%), fixed_assets_ratio(%), asset_liability_ratio(%), total_assets(yuan), operating_cash_flow_to_sales_ratio(%), operating_cash_flow_return_on_assets(%), operating_cash_flow_to_net_profit_ratio(%), operating_cash_flow_to_debt_ratio(%), cash_flow_ratio(%), short_term_stock_investment(yuan), short_term_bond_investment(yuan), short_term_other_operating_investment(yuan), long_term_stock_investment(yuan), long_term_bond_investment(yuan), long_term_other_operating_investment(yuan), accounts_receivable_within_1_year(yuan), accounts_receivable_1-2_years(yuan), accounts_receivable_2-3_years(yuan), accounts_receivable_over_3_years(yuan), prepaid_purchases_within_1_year(yuan), prepaid_purchases_1-2_years(yuan), prepaid_purchases_2-3_years(yuan), prepaid_purchases_over_3_years(yuan), other_receivables_within_1_year(yuan), other_receivables_1-2_years(yuan), other_receivables_2-3_years(yuan), other_receivables_over_3_years(yuan)
            # stock_financial_analysis_indicator_df = ak.stock_financial_analysis_indicator(symbol=self.symbol, start_year=start_year)
            stock_financial_analysis_indicator_df = ak.stock_financial_abstract_ths(symbol=self.symbol, indicator="按报告期")
            stock_financial_analysis_indicator_df = self.report_util.financial_indicator_map_sh_fields(df = stock_financial_analysis_indicator_df)
            stock_financial_analysis_indicator_df['股票代码'] = self.symbol
            stock_financial_analysis_indicator_df = self._filter_financial_indicator_window(
                df=stock_financial_analysis_indicator_df,
                start_year=start_year,
            )

        if cache and stock_financial_analysis_indicator_df is not None and not getattr(stock_financial_analysis_indicator_df, 'empty', False):
            self.cache_service.write_to_cache_serialized(current_date, report_type,stock_financial_analysis_indicator_df)
        return stock_financial_analysis_indicator_df

    def _filter_financial_indicator_window(self, df, start_year):
        if df is None or df.empty:
            return df
        start_year_int = int(start_year)
        filtered_df = df.copy()
        report_date_series = None
        if '报告期' in filtered_df.columns:
            report_date_series = pd.to_datetime(filtered_df['报告期'], errors='coerce')
        elif '报告日期' in filtered_df.columns:
            report_date_series = pd.to_datetime(filtered_df['报告日期'], errors='coerce')
        if report_date_series is None:
            return filtered_df
        filtered_df['年份'] = report_date_series.dt.year
        filtered_df = filtered_df[filtered_df['年份'].fillna(0).astype(int) >= start_year_int].copy()
        if '报告期' in filtered_df.columns:
            filtered_df['报告期'] = pd.to_datetime(filtered_df['报告期'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
        elif '报告日期' in filtered_df.columns:
            filtered_df['报告期'] = pd.to_datetime(filtered_df['报告日期'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
        if '报告日期' in filtered_df.columns:
            filtered_df['报告日期'] = pd.to_datetime(filtered_df['报告日期'], errors='coerce')
        return filtered_df

    # 即时的个股资金流
    def get_stock_fund_flow_individual(self,stock_name = ''):
        # 即时的个股资金流
        if self._should_use_futu_provider():
            flow_df = self._get_futu_provider().get_stock_capital_flow(self._normalize_symbol_for_provider(), self._normalized_market())
            if flow_df is None or flow_df.empty:
                return self.get_default_df()
            return flow_df.tail(1).to_string(index=False)
        if self.market  == self.usa:
            default_df = self.get_default_df()
            return default_df
        stock_fund_flow_individual_df = ak.stock_fund_flow_individual(symbol="即时")
        specific_stock_data = stock_fund_flow_individual_df[
             stock_fund_flow_individual_df['股票简称'] == stock_name].to_string(
             index=False)
        return specific_stock_data

    # 个股公司股本变动
    def get_stock_share_change_cninfo(self, list_date="2024",end_date="2025"):
        if self.market == self.ETF or self.market == self.usa:
            default_df = self.get_default_df()
            return default_df
        try:
            stock_share_change_cninfo_df = ak.stock_share_change_cninfo(symbol=self.symbol,
                                                                         start_date=str(list_date),
                                                                         end_date=end_date).to_markdown(index=False)
            return stock_share_change_cninfo_df
        except Exception as e:
            self.logger.error(f'get_stock_share_change_cninfo error stock_code:{self.symbol} {e}')
            default_df = self.get_default_df()
            import traceback
            traceback.print_exc()
            return default_df

    # 分红配送详情
    def get_stock_fhps_detail_ths(self):
        if self._should_use_futu_provider():
            stock_financial_analysis_indicator_df = self._get_futu_provider().get_stock_financials_statements(
                self._normalize_symbol_for_provider(), self._normalized_market()
            )
            # Map standard columns to match downstream expectations if needed
            stock_financial_analysis_indicator_df = self._filter_financial_indicator_window(
                df=stock_financial_analysis_indicator_df,
                start_year=start_year,
            )
        elif self.market  == self.HongKong:
            symbol = self.symbol[1:]
            stock_fhps_detail_ths_df = ak.stock_hk_fhpx_detail_ths(symbol=symbol).to_markdown(index=False)
        elif self.market == self.usa:
            stock_fhps_detail_ths_df = self.get_default_df()
            return stock_fhps_detail_ths_df
        else:
            stock_fhps_detail_ths_df = ak.stock_fhps_detail_ths(symbol=self.symbol).to_markdown(index=False)
        return stock_fhps_detail_ths_df

    # 获取高管持股数据
    def get_stock_ggcg_em(self,stock_name=''):
        if self.market  == self.HongKong or self.market == self.usa:
            return self.get_default_df()
        stock_ggcg_em_df = ak.stock_ggcg_em(symbol="全部")
        tock_ggcg_em_df = stock_ggcg_em_df[stock_ggcg_em_df['名称'] == stock_name].to_markdown(index=False)
        return tock_ggcg_em_df
    #获取历史数据，并赋值技术指标
    def get_stock_history_data(self,start_date_str=None, end_date_str=None):

        """针对单只股票执行完整的技术分析，下面是买卖点的分析信息
            ma_signal  ma_signal_position
            bb_signal  bb_signal_position
            macd_signal_index  macd_signal_position
            breakout_signal  breakout_position
            'sar_signal', 'sar_position'
            'mean_signal', 'mean_signal_position'
            rsi_signal,rsi_signal_position
            kdj_signal  kdj_signal_position
            williams_signal,williams_signal_position
            adx_signal adx_signal_position
            volume_signal volume_signal_position
        """
        current_date = datetime.datetime.now()
        if start_date_str is None :
            previous_date = current_date - datetime.timedelta(days=100)
            start_date_str =  previous_date.strftime("%Y%m%d")
        if end_date_str is None:
            end_date_str = current_date.strftime("%Y%m%d")
        stock_code = self.symbol
        start_date_str = start_date_str.replace('-', '')
        end_date_str = end_date_str.replace('-', '')

        report_type = 'history_' + stock_code
        date = end_date_str
        df_history_data = None
        cache_switch = True
        if cache_switch:
            df_history_data = self.cache_service.read_from_serialized(date, report_type=report_type)
        if df_history_data is not None and not df_history_data.empty:
            return df_history_data


        stock = stockAKIndicator()

        stock_us_hist_df = stock.stock_day_data_code(stock_code = stock_code, market = self.market, start_date_str= start_date_str, end_date_str = end_date_str)
        # 均线策略
        stock.strategy_mac(stock_us_hist_df)
        # 布林带策略
        stock.strategy_bollinger(stock_us_hist_df)
        # 动量策略
        stock.strategy_macd(stock_us_hist_df)
        # 突破策略
        stock.strategy_breakout(stock_us_hist_df)
        # SAR策略
        stock.strategy_sar(stock_us_hist_df)
        # 均值回归策略
        stock.mean_reversion_strategy(stock_us_hist_df)
        # rsi策略
        stock.strategy_rsi(stock_us_hist_df)
        # kdj策略
        stock.strategy_kdj(stock_us_hist_df)
        # williams_r策略
        stock.strategy_williams_r(stock_us_hist_df)
        # ADX策略
        stock.strategy_adx(stock_us_hist_df)
        # 成交量策略
        stock.strategy_volume(stock_us_hist_df)
        # print(stock_us_hist_df.to_markdown())
        if cache_switch:
            self.cache_service.write_to_cache_serialized(date, report_type=report_type,data = stock_us_hist_df)
        return stock_us_hist_df

    def get_stock_indicator_data(self):
        """
         # 获取估值数据
        # 交易日，市盈率，市盈率 TTM, 市净率，市销率，市销率 TTM, 股息率，股息率 TTM, 总市值
        # trade_date,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_mv
        :return:
        """

        cache = True
        report_type = self.market + "_" + self.symbol + '_stock_indicator'
        current_date = self.report_util.get_current_report_year_st(format='%Y',market=self.market)
        df_indicator = self.cache_service.read_from_serialized(current_date, report_type)
        if cache and df_indicator is not None:
            return df_indicator
        try:
            if self.market == 'SH' or self.market == 'SZ':
                df_indicator = ak.stock_a_indicator_lg(symbol=self.symbol)
                df_indicator['股票代码'] = self.symbol
                # return df_indicator
            elif self.market == 'HK':
                symbol = "hk" + self.symbol
                # 港股", "市盈率", "市净率", "股息率", "ROE", "市值
                df_indicator_ = ak.stock_hk_indicator_eniu(symbol=symbol, indicator="港股")
                df_indicator_PE = ak.stock_hk_indicator_eniu(symbol=symbol, indicator="市盈率")
                df_indicator_PS = ak.stock_hk_indicator_eniu(symbol=symbol, indicator="市净率")
                df_indicator_GX = ak.stock_hk_indicator_eniu(symbol=symbol, indicator="股息率")
                df_indicator_ROE = ak.stock_hk_indicator_eniu(symbol=symbol, indicator="ROE")
                df_indicator_Total = ak.stock_hk_indicator_eniu(symbol=symbol, indicator="市值")
                df_indicator = df_indicator_PE
                df_indicator['pe'] = df_indicator_PS['pe']
                df_indicator['pb'] = df_indicator_PS['ps']
                df_indicator['ps_ttm'] = df_indicator_GX['ps_ttm']  # 股息率
                df_indicator['roe'] = df_indicator_ROE['roe']
                df_indicator['total_mv'] = df_indicator_Total['total_mv']
                df_indicator['股票代码'] = self.symbol
                # return df_indicator
            elif self.market == 'usa':
                # 美股：从财报指标中提取估值序列
                df_fin = self.get_stock_financial_analysis_indicator()
                if df_fin is not None and not df_fin.empty:
                    # 映射美股字段到统一指标名
                    df_indicator = df_fin.copy()
                    # 确保 PE 和 ROE 存在
                    if 'PE' not in df_indicator.columns and '市盈率' in df_indicator.columns:
                        df_indicator['pe'] = df_indicator['市盈率']
                    if 'roe' not in df_indicator.columns:
                        for roe_col in ['平均净资产收益率', '净资产收益率', 'ROE']:
                            if roe_col in df_indicator.columns:
                                df_indicator['roe'] = df_indicator[roe_col]
                                break
                    df_indicator['股票代码'] = self.symbol
                else:
                    df_indicator = pd.DataFrame()
            else:
                df_indicator = pd.DataFrame()
            if cache and df_indicator.empty == False:
                self.cache_service.write_to_cache_serialized(current_date, report_type, df_indicator)
            return df_indicator

        except Exception as e:
            self.logger.error(f"get_stock_indicator_data失败{self.symbol}: {e}")
            return pd.DataFrame()


    def get_default_df(self ):
        default_data = {
            'item': ['Item'],
            'stock': [self.symbol],
            'market': [self.market]
        }
        default_df = pd.DataFrame()
        return default_df

    def get_usa_code(self):
        if self.market == self.usa:
            symbol = self.symbol.split('.',1)
            if len(symbol) > 1:
                return symbol[1]
            return self.symbol
        else:
            return self.symbol
