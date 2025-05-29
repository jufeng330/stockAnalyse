
import os
import time
import random
import logging
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Union
import pandas as pd
import akshare as ak
from akshare import stock_individual_basic_info_hk_xq
from tqdm import tqdm
from .stock_analyzer import  StockAnalyzer
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from stocklib.stock_border import stockBorderInfo
from .stock_result_utils import  StockFileUtils

# -------------------------------
# **全盘股票扫描器**
# -------------------------------
class TopStockScanner:
    """全盘筛选高打分股票的扫描器"""

    def __init__(self, max_workers: int = 20, min_score: float = 30,market = 'SH'):
        """
        初始化扫描器

        Args:
            max_workers: 并发线程数量（已增至20以加速分析）
            min_score: 高分最低阈值
        """
        self.analyzer = StockAnalyzer()
        self.max_workers = max_workers
        self.min_score = min_score
        self.logger = logging.getLogger(__name__)
        self.market = market
        self.file_utils = StockFileUtils(market = self.market)


    #
    def get_all_stocks(self) -> pd.DataFrame:
        """
        获取所有上市 A 股股票代码（全盘版）。
        使用 ak.stock_info_sh_name_code(symbol="主板A股") 与 ak.stock_info_sz_name_code(symbol="A股列表")，
        候选字段列表为：['A股代码', '证券代码', '股票代码', 'code'] 。
        """
        try:
            stock = stockBorderInfo(market=self.market)
            df_stock = stock.get_stock_border_info()
            # sh_df = ak.stock_info_sh_name_code(symbol="主板A股")
            # sz_df = ak.stock_info_sz_name_code(symbol="A股列表")

            self.logger.info(f"完整股票列表获取到 {len(df_stock)} 支股票信息")
            self.logger.info("\n开始分析 {len(df_stock)} 支股票...")
            return df_stock

        except Exception as e:
            self.logger.error(f"获取股票列表失败：{str(e)}")
            traceback.print_exc()
            raise

    def analyze_stock_safe(self, stock, max_retries: int = 3) -> Optional[Dict]:
        """
        安全分析单只股票（加入重试机制），数据异常则跳过。
        """
        stock_code = stock['代码']
        market = stock['market']
        for attempt in range(max_retries):
            try:
                result =  self.analyzer.analyze_stock(stock,market)
                self.logger.debug(f"股票 {stock_code} 分析完成，结果：{result}")
                return result
            except ValueError as e:
                self.logger.warning(f"跳过股票 {stock_code}: {str(e)}")
                return None
            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"股票 {stock_code} 分析尝试 {max_retries} 次后失败：{str(e)}")
                    return None
                self.logger.warning(f"股票 {stock_code} 第 {attempt+1} 次分析失败：{str(e)}")
                traceback.print_exc()
                time.sleep(random.uniform(2, 5))

    def process_batch(self, stock_codes) -> List[Dict]:
        """利用多线程并行处理一批股票的分析任务"""
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # futures = {executor.submit(self.analyze_stock_safe, code): code for code in stock_codes}
            futures = {executor.submit(self.analyze_stock_safe, row): index for index, row in stock_codes.iterrows()}

            for future in tqdm(futures, desc="分析进度", ncols=80):
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as e:
                    stock = futures[future]
                    self.logger.error(f"处理股票 {stock} 时出错：{str(e)}")
        return results



    def scan_high_score_stocks(self, batch_size: int = 20) -> List[Dict]:
        """扫描全盘股票，返回高打分结果列表"""
        try:
            df_stocks_data = self.get_all_stocks()

            # all_stocks = df_stocks_data['代码'].astype(str).str.startswith("600").loc[lambda x: x].index[:200].tolist()
            # all_stocks = df_stocks_data['代码'].astype(str).str.tolist()

            all_stocks = df_stocks_data
            all_stocks['market'] = self.market
            # all_stocks = all_stocks.head(100)
            total_stocks = len(all_stocks)
            self.logger.info(f"\n开始扫描 {total_stocks} 支股票……")
            results = []
            total_batches = (total_stocks + batch_size - 1) // batch_size

            with tqdm(total=total_batches, desc="批次处理进度", ncols=80) as pbar:
                for i in range(0, total_stocks, batch_size):
                    batch_number = i // batch_size + 1
                    self.logger.info(f"\r当前进度: 批次 {batch_number}/{total_batches}")
                    batch = all_stocks.iloc[i:i + batch_size]
                    batch_results = self.process_batch(batch)
                    results.extend(batch_results)
                    if i + batch_size < total_stocks:
                        time.sleep(random.uniform(3, 5))
                    if results and ((len(results) % 100 == 0) or (i + batch_size >= total_stocks)):
                        self.file_utils.save_intermediate_results(results)
                        # 更新进度条
                    pbar.update(1)
                    pbar.set_description(f"批次处理进度 (当前批次: {batch_number}/{total_batches})")

            self.logger.info("\n扫描结束！")

            if results:
                df_results = pd.DataFrame(results)
                formatted_results = self.file_utils.save_high_score_stocks(df_results)
                return formatted_results
            return []

        except Exception as e:
            self.logger.error(f"全盘扫描失败：{str(e)}")
            traceback.print_exc()
            raise
