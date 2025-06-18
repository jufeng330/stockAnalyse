import os
import sys
import pandas as pd
import pickle
import logging

# 个股相关信息查询
"""
  需要缓存的数据：
  获取所有股票数据  stock_fund_flow_individual_df = ak.stock_fund_flow_individual(symbol="即时")
  获取所有板块信息: stock_board = stock_concept_service.stock_board_concept_name_ths()
"""
class FileCacheUtils:
    def __init__(self, market='SZ', cache_dir=None):
        # 定义 current_date 并格式化
        self.market = market
        if(cache_dir is None):
            self.cache_dir = os.path.join(os.path.dirname(__file__), 'cache', 'financial_reports') # 缓存目录可自定义
        else:
            self.cache_dir = os.path.join(os.path.dirname(__file__), 'cache', cache_dir)
         # 缓存目录可自定义
        os.makedirs(self.cache_dir, exist_ok=True)  # 创建缓存目录（如果不存在）

        self.logger = logging.getLogger(__name__)

    def _get_cache_filepath(self, date, report_type ,file_type='csv'):
        """生成缓存文件路径"""
        if file_type == 'csv':
            return os.path.join(self.cache_dir, f"{report_type}_{date}_{self.market}.csv")
        else:
            return os.path.join(self.cache_dir, f"{report_type}_{date}_{self.market}.pkl")


    def read_from_csv(self, date, report_type):
        """从 CSV 缓存读取数据"""
        filepath = self._get_cache_filepath(date, report_type)
        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath, index_col=0)  # index_col=0 避免额外索引列
                df = df.reset_index()  # 将索引还原为普通列
                return df
            except Exception as e:
                print(f"[CACHE] 读取缓存失败: {filepath}, 错误: {e}")
                return None
        return None

    def write_to_csv(self, date, report_type, data ,force=False):
        """将 DataFrame 写入 CSV 缓存"""
        if data is None or data.empty:
            print(f"[CACHE] 数据为空，不写入缓存: {report_type}")
            return
        filepath = self._get_cache_filepath(date, report_type)
        try:
            if not force and  os.path.exists(filepath):
                return
            data.to_csv(filepath, index=False)  # index=False 避免保存无意义的索引
            print(f"[CACHE] 已缓存 {report_type} 数据到: {filepath}")
        except Exception as e:
            print(f"[CACHE] 写入缓存失败: {filepath}, 错误: {e}")


    def write_to_csv_force(self, stock_zcfz_em_df ,stock_lrb_em_df ,stock_xjll_em_df ,date):
        self._write_to_csv(date, "zcfz", stock_zcfz_em_df ,force=True)
        self._write_to_csv(date, "lrb", stock_lrb_em_df ,force=True)
        self._write_to_csv(date, "xjll", stock_xjll_em_df ,force=True)

    def write_to_cache_serialized(self, date, report_type, data ,force=False):
        """将 DataFrame 使用 pickle 写入二进制缓存"""
        filepath = self._get_cache_filepath(date, report_type ,file_type='pkl')
        try:
            if not force and os.path.exists(filepath) :
                return
            # 创建目录（如果不存在）
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            # 使用 pickle 写入二进制文件
            with open(filepath, 'wb') as f:
                pickle.dump(data, f)

            print(f"[CACHE] 已缓存 {report_type} 数据到: {filepath}")
        except Exception as e:
            print(f"[CACHE] 写入缓存失败: {filepath}, 错误: {e}")

    def read_from_serialized(self, date, report_type):
        """从 pickle 缓存中读取数据"""
        filepath = self._get_cache_filepath(date, report_type ,file_type='pkl')
        try:
            if os.path.exists(filepath):
                # 从 pickle 文件中读取数据
                with open(filepath, 'rb') as f:
                    return pickle.load(f)
            return None  # 文件不存在返回 None
        except Exception as e:
            print(f"[CACHE] 读取缓存失败: {filepath}, 错误: {e}")
            return None
