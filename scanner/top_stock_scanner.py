
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
# from litellm import maritalk_key
# from akshare import stock_individual_basic_info_hk_xq
from tqdm import tqdm
from .stock_analyzer import  StockAnalyzer
from .stock_select_strategy import StockSelectStrategy

import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from stocklib.stock_border import stockBorderInfo
from stocklib.utils_file_cache import FileCacheUtils
from stocklib.utils_report_date import ReportDateUtils
from stocklib.utils_report_date import ReportDateUtils
from stocklib.stock_strategy import StockStrategy
from stocklib.utils_stock import StockUtils
from stocklib.stock_company import stockCompanyInfo

from .stock_result_utils import  StockFileUtils

# -------------------------------
# **全盘股票扫描器**
# -------------------------------
class TopStockScanner:
    """全盘筛选高打分股票的扫描器"""

    def __init__(self, max_workers: int = 20, min_score: float = 30,market = 'SH',strategy_type ='1'):
        """
        初始化扫描器

        Args:
            max_workers: 并发线程数量（已增至20以加速分析）
            min_score: 高分最低阈值
        """
        self.analyzer = StockAnalyzer(market=market)
        self.max_workers = max_workers
        self.min_score = min_score
        self.logger = logging.getLogger(__name__)
        self.market = market
        self.stockSelector = StockSelectStrategy(market=self.market,strategy_type = strategy_type)
        strategy_name = self.stockSelector.get_strategy_name(strategy_type)
        self.file_utils = StockFileUtils(market = self.market,name = strategy_name)
        self.cache_service = FileCacheUtils(market=self.market, cache_dir='history_' + market)
        self.reportUtils = ReportDateUtils()
        self.stock_strategy = StockStrategy()
        self.stock_utils = StockUtils()


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
            self.logger.info(f"\n开始分析 {len(df_stock)} 支股票...")
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



    def scan_high_score_stocks(self, batch_size: int = 20,type = 1,strategy_filter = 'avg') -> List[Dict]:
        """扫描全盘股票，返回高打分结果列表"""
        try:
            df_stocks_data = self.get_all_stocks()


            df_normal = self.stockSelector.select_stock(df_stocks_data,strategy_type = type,strategy_filter = strategy_filter)

            set_normal  = set(df_normal['代码'])
            df_stocks_data = df_stocks_data[df_stocks_data['代码'].astype(str).isin(set_normal)]

            # all_stocks = df_stocks_data['代码'].astype(str).str.startswith("600").loc[lambda x: x].index[:200].tolist()
            # all_stocks = df_stocks_data['代码'].astype(str).str.tolist()

            results = self.scan_stock(batch_size, df_stocks_data)

            if results:
                df_results = pd.DataFrame(results)
                formatted_results = self.file_utils.save_high_score_stocks(df_results)
                return formatted_results
            return []

        except Exception as e:
            self.logger.error(f"全盘扫描失败：{str(e)}")
            traceback.print_exc()
            raise

    def scan_stock(self, batch_size, df_stocks_data):
        all_stocks = df_stocks_data
        all_stocks['market'] = self.market
        # all_stocks = all_stocks.head(100)
        total_stocks = len(all_stocks)
        self.logger.info(f"\n开始扫描 {total_stocks} 支股票……")
        results = []
        total_batches = (total_stocks + batch_size - 1) // batch_size
        GREEN = '\033[92m'
        RESET = '\033[0m'
        bar_format = f"{GREEN}{{l_bar}}{{bar}}{{r_bar}}{RESET}"
        with tqdm(total=total_batches, desc="批次处理进度", ncols=80, bar_format=bar_format) as pbar:
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
        return results

    def backtest_stocks(self, list_high_score_stocks, analysis_date='2025-06-06'):
        """
        回测一批股票在分析日期之后的涨跌情况

        参数:
        df_high_score_stocks: 高得分股票数据框
        analysis_date: 分析日期

        返回:
        回测结果数据框和统计信息
        """
        # 转换分析日期为日期格式
        analysis_date = pd.to_datetime(analysis_date)
        # 结果存储列表
        results = []
        # 遍历每只股票
        df_high_score_stocks = pd.DataFrame(list_high_score_stocks)
        for i in range(1, 4):
            df_high_score_stocks[f'day_{i}_return'] = 0.0
            df_high_score_stocks[f'day_{i}_is_up'] = None
        for idx, row in df_high_score_stocks.iterrows():
            stock_code = row['股票代码']
            stock_name = row['stock_name']
            # 获取该股票的历史数据，假设结束日期为分析日期后的7天
            # 实际应用中可能需要调整时间范围

            current_date = datetime.now().strftime('%Y%m%d')
            # 计算60天前的日期作为开始日
            start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
            end_date = current_date

            stock_company = stockCompanyInfo(marker=self.market,symbol=stock_code)

            # 获取历史数据
            try:
                stock_data = stock_company.get_stock_history_data(
                    start_date_str=start_date,
                    end_date_str=end_date
                )

                # 确保数据不为空
                if stock_data is None or stock_data.empty:
                    continue

                # 找到分析日期或之后的第一个交易日
                stock_data['日期'] = pd.to_datetime(stock_data['日期']).dt.date
                df_data = len(stock_data[stock_data['日期'].apply(lambda x: x >= analysis_date.date())])
                if len(df_data) == 0:
                    continue
                first_trade_date =df_data['日期'].iloc[0]
                # 获取分析日期的价格
                col_price = '收盘'
                future_data = stock_data[stock_data['日期'].apply(lambda x: x >= first_trade_date)]
                analysis_price = float(future_data[col_price].iloc[0])


                for i in range(1, 4):
                    target_date = first_trade_date + pd.Timedelta(days=1)
                    # 找到目标日期或之后的第一个交易日
                    future_data = stock_data[stock_data['日期'].apply(lambda x: x >= target_date)]

                    if not future_data.empty:
                        future_price = float(future_data.iloc[0][col_price] ) # 假设列名为'close'
                        price_change_pct = (future_price - analysis_price) / analysis_price * 100
                        is_up = price_change_pct > 0

                        df_high_score_stocks.at[idx, f'day_{i}_return'] = price_change_pct
                        df_high_score_stocks.at[idx, f'day_{i}_is_up'] = is_up
                        first_trade_date = future_data.iloc[0]['日期']
                    else:
                        df_high_score_stocks.at[idx, f'day_{i}_return'] = None
                        df_high_score_stocks.at[idx, f'day_{i}_is_up'] = None


            except Exception as e:
                print(f"Error processing stock {stock_code}: {e}")
                continue

        # 统计信息
        stats = self.generate_statistics_report(df_high_score_stocks)
        stats_s1 = self.generate_statistics_report(df_high_score_stocks,type='强烈推荐买入')
        stats_s2 = self.generate_statistics_report(df_high_score_stocks, type='建议买入')

        stats_result = f'整体统计信息:\n {stats} 强烈推荐买入统计信息:{stats_s1}\n 建议买入统计信息{stats_s2}'

        return df_high_score_stocks, stats_result

    def generate_statistics_report(self, df_result, type='all'):
        """
        生成统计报告，输出每只股票在第1、2、3天后的涨跌幅平均值和涨跌数量

        参数:
        results_df: 回测结果数据框

        返回:
        统计报告字符串
        """
        if df_result.empty:
            return "没有可用的回测数据"
        if type == 'all':
            df = df_result
        else:
            df = df_result[df_result['投资建议'] == type]

        report = "===== 股票回测统计报告 =====\n\n"

        for i in range(1, 4):
            day_return_col = f'day_{i}_return'
            day_is_up_col = f'day_{i}_is_up'

            if day_return_col  not in df.columns or day_is_up_col not in df.columns:
                continue
            avg_return = df[day_return_col].mean()
            up_count = len(df[df[day_is_up_col] == True])
            down_count =len(df[df[day_is_up_col] == False])
            total_count = len(df)
            if total_count>0:
                report += f"第{i}天统计:\n"
                report += f"  平均涨跌幅: {avg_return:.2f}%\n"
                report += f"  上涨数量: {up_count} ({up_count / total_count * 100:.2f}%)\n"
                report += f"  下跌数量: {down_count} ({down_count / total_count * 100:.2f}%)\n\n"
            else:
                report += f"第{i}天统计:\n"
                report += f"  总数量: 0\n"


        return report

    """
     下面是废弃的代码，暂时保留
    """











    def get_stock_normal_info(self, df_stock = None, strategy_filter ='avg') -> pd.DataFrame:
        """
        获取公司本身是合格公司的数据  主要条件如下：
         1、 市值百亿以上
         2、 最近3年盈利为正，营业额是正增长的
         3、 公司估值在合理区间内  PE<15或者ROE>10 或者股息率>3%
        :return:
        """
        date = self.reportUtils.get_current_report_year_st()
        #实时数据行情
        # 字段包括[序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收', '量比', '换手率', '市盈率-动态', '市净率', '总市值', '流通市值', '涨速', '5分钟涨跌', '60日涨跌幅', '年初至今涨跌幅']
        stock = stockBorderInfo(market=self.market)
        if df_stock is None:
            df_stock_spot = stock.get_stock_spot()
        else:
            df_stock_spot = df_stock.copy()
        df_stock_spot['代码'] = df_stock_spot['代码'].astype(str)
        print(f"df_stock 股票数量：{len(df_stock_spot)}")

        df_stock_spot = df_stock_spot[df_stock_spot['总市值'] > 100 * 10000 * 10000]  if '总市值' in df_stock_spot.columns else df_stock_spot  #市值100以以上
        df_stock_spot = df_stock_spot[df_stock_spot['市盈率-动态'] < 50] if '市盈率-动态' in df_stock_spot.columns else df_stock_spot
        # df_stock = df_stock[df_stock['市净率'] < 20] if '市净率' in df_stock.columns else df_stock
        # df_stock = df_stock[df_stock['股息率']>0.01 or df_stock['股息率'] is None] if '股息率' in df_stock.columns else df_stock
        # df_stock = df_stock[df_stock['ROE'] > 0.0001] if 'ROE' in df_stock.columns else df_stock


        df_stock_spot.loc[:, '资产负债率_%'] = df_stock_spot['资产负债率'] if '资产负债率' in df_stock_spot.columns else None
        if '资产负债率_%' in df_stock_spot.columns:
            df_stock_spot =  self.stock_utils.pd_convert_to_float(df_stock_spot, '资产负债率_%')
            df_stock_spot.loc[:, '资产负债率_%'] = df_stock_spot['资产负债率_%'].astype(float) * 100
        df_stock_spot = df_stock_spot[df_stock_spot['资产负债率_%'] < 85] if '资产负债率_%' in df_stock_spot.columns else df_stock_spot
        print(f"df_stock 资产负债率合格股票数量：{len(df_stock_spot)}")

        df_financial = stock.get_stock_border_financial_indicator(market = self.market, date=date, df_stock_spot=df_stock_spot)
        date_financial = self.reportUtils.get_report_year_str(days=365*3,format='%Y-%m-%d')
        if self.market == 'H':
            date_financial = self.reportUtils.get_report_year_str(days=365 * 4, format='%Y-%m-%d')

        set_stocks = self.find_financial_stock_data(date_financial, df_financial, data_type = strategy_filter)
        # # 交易日，市盈率，市盈率 TTM, 市净率，市销率，市销率 TTM, 股息率，股息率 TTM, 总市值
        df_filtered = df_stock_spot[df_stock_spot['股票代码'].isin(set_stocks)]
        df_financial_filter = df_financial[df_financial['股票代码'].isin(set_stocks)]
        self.file_utils.create_middle_file(file_name='股票基本信息',df =df_filtered)
        self.file_utils.create_middle_file(file_name='股票财务信息',df = df_financial_filter)
        return df_filtered

    def find_financial_stock_data(self, date_financial, df_financial, data_type ='continue', threshold_1=0.0,threshold_2=0.0,threshold_3=0.0):
        # 筛选最近三年的数据（利润率为正） data_type 取值是 continue,avg
        if data_type == 'continue':
            col_lrl = '净利润'
            col_lrl_rename = '全年利润率为正'
            set_stocks_lrl = self.stock_strategy.get_stock_continue_postive(df_financial, date_financial, col_lrl,
                                                                            col_lrl_rename,threshold_1)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_lrl)}")
            col_lrl = '净利润同比增长率'
            col_lrl_rename = '利润率同比为正'
            set_stocks_lrl_ratio = self.stock_strategy.get_stock_continue_postive(df_financial, date_financial, col_lrl,
                                                                                  col_lrl_rename,threshold_2)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_lrl_ratio)}")
            col_lrl = '营业总收入同比增长率'
            col_lrl_rename = '全年业务收入增长率为正'
            set_stocks_yy = self.stock_strategy.get_stock_continue_postive(df_financial, date_financial, col_lrl,
                                                                           col_lrl_rename,threshold_3)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_yy)}")
        else:
            col_lrl = '净利润'
            col_lrl_rename = '全年利润率为正'
            set_stocks_lrl = self.stock_strategy.get_stock_avg_postive(df_financial, date_financial, col_lrl,
                                                                            col_lrl_rename,threshold_1)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_lrl)}")
            col_lrl = '净利润同比增长率'
            col_lrl_rename = '利润率同比为正'
            set_stocks_lrl_ratio = self.stock_strategy.get_stock_avg_postive(df_financial, date_financial, col_lrl,
                                                                                  col_lrl_rename,threshold_2)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_lrl_ratio)}")
            col_lrl = '营业总收入同比增长率'
            col_lrl_rename = '全年业务收入增长率为正'
            set_stocks_yy = self.stock_strategy.get_stock_avg_postive(df_financial, date_financial, col_lrl,
                                                                           col_lrl_rename,threshold_3)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_yy)}")
        set_stocks = set_stocks_lrl & set_stocks_yy & set_stocks_lrl_ratio  # 或使用 set_lrl.intersection(set_yy)
        print(f"df_financial  合格股票数量：{len(set_stocks)}")
        return set_stocks

    def get_stock_quality_info(self,df_stock = None,strategy_filter='avg'):
        """
        获取公司本身是合格公司的数据  主要条件如下：
         1、 市值百亿以上
         2、 最近3年盈利>5% ，营业额是正增长>5%
         3、 公司估值在合理区间内  PE<15或者ROE>10 或者股息率>3%
        :return:
        """
        date = self.reportUtils.get_current_report_year_st()
        # 实时数据行情
        # 字段包括[序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收', '量比', '换手率', '市盈率-动态', '市净率', '总市值', '流通市值', '涨速', '5分钟涨跌', '60日涨跌幅', '年初至今涨跌幅']
        stock = stockBorderInfo(market=self.market)
        if df_stock is None:
            df_stock = stock.get_stock_spot()
        df_stock['代码'] = df_stock['代码'].astype(str)
        print(f"df_stock 票数量：{len(df_stock)}")

        df_stock = df_stock[df_stock['总市值']>100*10000*10000]  if '总市值' in df_stock.columns else df_stock  #市值100以以上

        df_stock = df_stock[df_stock['市盈率-动态'] < 30] if '市盈率-动态' in df_stock.columns else df_stock
        df_stock = df_stock[df_stock['市净率'] < 20] if '市净率' in df_stock.columns else df_stock

        df_stock['资产负债率_%'] = df_stock['资产负债率'] if '资产负债率' in df_stock.columns else None
        if '资产负债率_%' in df_stock.columns:
            df_stock = self.stock_utils.pd_convert_to_float(df_stock, '资产负债率_%')
            df_stock['资产负债率_%'] = df_stock['资产负债率_%'].astype(float) * 100
        df_stock = df_stock[df_stock['资产负债率_%'] < 80] if '资产负债率_%' in df_stock.columns else df_stock
        print(f"df_stock 资产负债率合格股票数量：{len(df_stock)}")
        # 财务指标数据
        # 日期, 摊薄每股收益(元), 加权每股收益(元), 每股收益_调整后(元), 扣除非经常性损益后的每股收益(元), 每股净资产_调整前(元), 每股净资产_调整后(元), 每股经营性现金流(元), 每股资本公积金(元),
        # 每股未分配利润(元), 调整后的每股净资产(元), 总资产利润率(%), 主营业务利润率(%), 总资产净利润率(%), 成本费用利润率(%),
        # 营业利润率(%), 主营业务成本率(%), 销售净利率(%), 股本报酬率(%), 净资产报酬率(%), 资产报酬率(%), 销售毛利率(%), 三项费用比重, 非主营比重, 主营利润比重,
        # 股息发放率(%), 投资收益率(%), 主营业务利润(元), 净资产收益率(%), 加权净资产收益率(%), 扣除非经常性损益后的净利润(元),
        # 主营业务收入增长率(%), 净利润增长率(%), 净资产增长率(%), 总资产增长率(%), 应收账款周转率(次), 应收账款周转天数(天), 存货周转天数(天), 存货周转率(次), 固定资产周转率(次),
        # 总资产周转率(次), 总资产周转天数(天), 流动资产周转率(次), 流动资产周转天数(天), 股东权益周转率(次), 流动比率, 速动比率, 现金比率(%), 利息支付倍数, 长期债务与营运资金比率(%), 股东权益比率(%),
        # 长期负债比率(%), 股东权益与固定资产比率(%), 负债与所有者权益比率(%), 长期资产与长期资金比率(%), 资本化比率(%), 固定资产净值率(%), 资本固定化比率(%), 产权比率(%), 清算价值比率(%),
        # 固定资产比重(%), 资产负债率(%), 总资产(元), 经营现金净流量对销售收入比率(%), 资产的经营现金流量回报率(%), 经营现金净流量与净利润的比率(%),
        # 经营现金净流量对负债比率(%), 现金流量比率(%), 短期股票投资(元), 短期债券投资(元), 短期其它经营性投资(元), 长期股票投资(元), 长期债券投资(元), 长期其它经营性投资(元),
        # 1年以内应收帐款(元), 1-2年以内应收帐款(元), 2-3年以内应收帐款(元), 3年以内应收帐款(元), 1年以内预付货款(元), 1-2年以内预付货款(元), 2-3年以内预付货款(元), 3年以内预付货款(元), 1年以内其它应收款(元), 1-2年以内其它应收款(元), 2-3年以内其它应收款(元), 3年以内其它应收款(元)
        #
        df_financial = stock.get_stock_border_financial_indicator(market=self.market, date=date, df_stock_spot=df_stock)
        date_financial = self.reportUtils.get_report_year_str(days=365 * 3, format='%Y-%m-%d')
        # 筛选最近三年的数据（利润率为正）
        set_stocks =self.find_financial_stock_data(date_financial,df_financial,data_type=strategy_filter,threshold_1=5000*10000,threshold_2=0.05,threshold_3=0.05)

        # # 交易日，市盈率，市盈率 TTM, 市净率，市销率，市销率 TTM, 股息率，股息率 TTM, 总市值
        # trade_date,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_mv

        """
        total_mv_value = 100*10000*10000
        df_indicator = stock.get_stock_border_indicator_data(market=self.market, date='20240331', indicator='年报',df_stock_spot = df_stock)
        df_indicator = df_indicator[df_indicator['trade_date']=='2025-03-31']
        df_indicator_mv = df_indicator[df_indicator['总市值']>total_mv_value].copy()

        df_indicator_pe = df_indicator_mv[df_indicator_mv['市盈率']<12]
        df_indicator_dv = df_indicator_mv[df_indicator_mv['股息率'] > 0.03]
        df_indicator_roe = df_indicator_mv[df_indicator_mv['净资产收益率( %)'] > 10]

        set_pe = set(df_indicator_pe['股票代码'])
        set_dv = set(df_indicator_dv['股票代码'])
        set_roe = set(df_indicator_roe['股票代码'])
        # 计算并集（包含所有满足任一条件的股票）
        union_stocks = set_pe & set_dv & set_roe # 或使用 set_pe.union(set_dv)

        set_stocks = set_stocks & union_stocks
        """
        df_filtered = df_stock[df_stock['股票代码'].isin(set_stocks)]
        df_financial_filter = df_financial[df_financial['股票代码'].isin(set_stocks)]
        self.file_utils.create_middle_file(file_name='股票基本信息', df=df_filtered)
        self.file_utils.create_middle_file(file_name='股票财务信息', df=df_financial_filter)

        return df_filtered

