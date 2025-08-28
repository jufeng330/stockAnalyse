import akshare as ak
import pandas as pd
import time
from tqdm import tqdm
import os
import logging
import sys

from .utils_file_cache import FileCacheUtils
from .mysql_cache import MySQLCache

class stockConcepService:

    def __init__(self, max_workers: int = 20, min_score: float = 30, market = 'SH'):
        """
        初始化扫描器

        Args:
            max_workers: 并发线程数量（已增至20以加速分析）
            min_score: 高分最低阈值
        """
        self.max_workers = max_workers
        self.min_score = min_score
        self.logger = logging.getLogger(__name__)
        self.market = market
        self.cache_service = FileCacheUtils(market=self.market, cache_dir='history_' + market)
        self.mysql = MySQLCache()

    def get_all_sectors_and_stocks(self):

        # 获取东方财富概念板块列表
        print("获取概念板块列表...")
        concept_sectors =  self.cache_service.read_from_serialized(date='20250331', report_type = 'stock_concept');
        industry_sectors = self.cache_service.read_from_serialized(date='20250331', report_type = 'stock_industry');
        # industry_sectors = ak.stock_board_industry_name_em()
        if concept_sectors is None or concept_sectors.empty:
            concept_sectors = ak.stock_board_concept_name_em()
            self.cache_service.write_to_cache_serialized( date='20250331', report_type = 'stock_concept', data = concept_sectors)
            print(f"获取到{len(concept_sectors)}个概念板块")

            # 获取东方财富行业板块列表
            print("获取行业板块列表...")
            industry_sectors = ak.stock_board_industry_name_em()
            self.cache_service.write_to_cache_serialized(date='20250331', report_type='stock_industry',
                                                         data=industry_sectors)
            print(f"获取到{len(industry_sectors)}个行业板块")

        # 获取行业板块成分股
        print("获取行业板块成分股...")
        for _, row in tqdm(industry_sectors.iterrows(), total=len(industry_sectors), desc="行业板块进度"):
            sector_name = row["板块名称"]
            try:
                df_data = self.mysql.read_from_cache(date='20250331', report_type='stock_industry_data',
                                                     conditions={"所属板块": sector_name})
                if df_data is not None and not df_data.empty and len(df_data) > 0:
                    continue
                # 获取成分股
                stocks = ak.stock_board_industry_cons_em(symbol=sector_name, df=industry_sectors)
                stocks["所属板块"] = sector_name
                stocks["板块类型"] = "行业"
                self.mysql.write_to_cache(date='20250331', report_type='stock_industry_data',
                                          data=stocks)

                # 避免请求过于频繁
                time.sleep(1)
            except Exception as e:
                print(f"获取{sector_name}成分股失败: {e}")
                continue
        # 获取概念板块成分股
        print("获取概念板块成分股...")
        for _, row in tqdm(concept_sectors.iterrows(), total=len(concept_sectors), desc="概念板块进度"):
            sector_name = row["板块名称"]
            try:
                # 获取成分股
                df_data = self.mysql.read_from_cache(date='20250331', report_type='stock_concept_data',conditions={"所属板块": sector_name})
                if df_data is not None and not df_data.empty and len(df_data) > 0 :
                    continue
                stocks = ak.stock_board_concept_cons_em(symbol=sector_name,df = concept_sectors)
                stocks["所属板块"] = sector_name
                stocks["板块类型"] = "概念"
                self.mysql.write_to_cache(date='20250331', report_type='stock_concept_data',
                                                             data=stocks)
                # 避免请求过于频繁
                time.sleep(1)
            except Exception as e:
                print(f"获取{sector_name}成分股失败: {e}")
                continue

        return concept_sectors,industry_sectors





        # 合并所有股票数据

        print("数据获取完成！")
        print("数据已保存到'板块股票数据'目录下")
