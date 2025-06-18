import concurrent.futures  # 引入线程池模块

import sys
import os
import numpy as np
import json
import traceback

import pandas as pd
import datetime
import akshare as ak
# 添加 stock_analyse 目录到 Python 模块搜索路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
print(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# 调试：打印可能的模块路径
possible_path = os.path.join(os.path.dirname(__file__), '../../', 'stocklib', 'stock_company.py')
print(f"检查模块是否存在: {os.path.exists(possible_path)}")

# 调试：打印所有可能的 stocklib 路径
for path in sys.path:
    lib_path = os.path.join(path, 'stocklib', 'stock_company.py')
    print(f"检查路径: {lib_path}")
    if os.path.exists(lib_path):
        print(f"找到模块: {lib_path}")
from stocklib.stock_border import stockBorderInfo
from stocklib.dcf_model import stockDCFSimpleModel
from .stock_ai_analyzer import  StockAiAnalyzer



# 添加调试日志函数
def debug_log(message):
    print(f"[DEBUG] {message}")


class stockSelectService:
    def __init__(self, market='SZ'):
        # 定义 current_date 并格式化
        self.market = market
        self.data_dir = os.path.join(os.path.dirname(__file__), 'result')

    # 选择指定年份的报告
    def get_report_year_str(self, days=0):
        # 获取当前日期
        current_date = datetime.datetime.now()
        # 计算减去指定天数后的日期
        if days <= 0:
            new_date = current_date
        else:
            new_date = current_date - datetime.timedelta(days=days)
        # 获取减去指定天数后的年份
        target_year = new_date.year
        # 拼接年份和 '0331'
        target_date_str = str(target_year) + '0331'
        return target_date_str

    # 获取最近n年的日期列表
    def get_report_year_str_list(self, years=5):
        today = datetime.datetime.now()
        dates = []
        for i in range(years):
            date = self.get_report_year_str(days = (i)*365)
            dates.append(date)
        return dates
        # 按照股票价格估值选择股票        filter_df_sz = self.filter_by_column_threshold(df = df,col_name ='总市值',threshold=market_value)
        #         filter_df_pe = self.filter_by_column_threshold(df=filter_df_sz,col_name='市盈率-动态',threshold=pe_threshold,comparison='le')
        #         filter_df_pe = self.filter_by_column_threshold(df=filter_df_pe,col_name='市盈率-动态',threshold=0,comparison='ge')
        #
        #         filter_df_pb = self.filter_by_column_threshold(df=filter_df_pe, col_name='市净率', threshold=pb_threshold,
        #                                                     comparison='le')
        #
        #         filter_df_r = filter_df_pb
        #
        #         filter_stocks, filter_annual_reports, filter_simple_reports = self.select_stock_by_report_common(years=5,
        #                                                                                                          profit_growth_threshold=1,
        #                                                                                                          revenue_growth_threshold=1,
        #                                                                                                          debt_ratio_threshold=70,
        #                                                                                                          year_threshold=5)
        #         filter_stocks_str = [str(stock) for stock in filter_stocks]
        #
        #         filter_df = filter_df_r[filter_df_r['代码'].isin(filter_stocks_str)]
        #         filter_df.applymap(self.format_float)

    def select_stock_by_price_valuation(self,market_value=50*10000*10000,pe_threshold=15,pb_threshold=2):
        stock_service = stockBorderInfo(market=self.market)
        df = stock_service.get_stock_spot()

        list_filter_stocks = filter_df['代码'].tolist()
        self.save_results_to_file_by_price(filter_stocks=list_filter_stocks,filter_df = filter_df)
        return filter_df

    def select_stock_by_report(self, years=5,profit_growth_threshold = 6,revenue_growth_threshold=6,debt_ratio_threshold=60,year_threshold=5):
        filter_stocks, filter_annual_reports, filter_simple_reports  = self.select_stock_by_report_common(years,profit_growth_threshold,revenue_growth_threshold,debt_ratio_threshold,year_threshold)
        self.save_results_to_file_by_report(filter_stocks, annual_reports = filter_annual_reports, filter_simple_reports = filter_simple_reports)
        return filter_stocks, filter_annual_reports


    # 按照财报健康度选择股票
    def select_stock_by_report_common(self, years=5,profit_growth_threshold = 6,revenue_growth_threshold=6,debt_ratio_threshold=60,year_threshold=5):
        annual_reports =  self.fetch_stock_recport(years)
        filtered_stocks_profit = self.filter_profit_growth(annual_reports,profit_growth = profit_growth_threshold,threshold=year_threshold)
        filtered_stocks_revenue = self.filter_revenue_growth(annual_reports,revenue_growth_threshold=revenue_growth_threshold,threshold=year_threshold)
        filtered_stocks_debt = self.filter_debt_ratio(annual_reports,debt_ratio_threshold=debt_ratio_threshold,threshold=year_threshold)
        # 打印结果
        print("filtered_stocks_profit:", filtered_stocks_profit)
        print("filtered_stocks_revenue:", filtered_stocks_revenue)
        print("filtered_stocks_debt:", filtered_stocks_debt)

        filter_stocks = list(
            set(filtered_stocks_profit) &
            set(filtered_stocks_revenue) &
            set(filtered_stocks_debt)
        )
        print("filter_stocks:", filter_stocks)
        filter_annual_reports = self.fetch_stock_filter_recport(annual_reports,filter_stocks)
        filter_simple_reports = self.merge_financial_tables_to_wide(filter_stocks, annual_reports)
        return filter_stocks,filter_annual_reports,filter_simple_reports

    def save_results_to_file_by_price(self, filter_stocks, filter_df):
        # 生成时间戳
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        # 构造文件名
        file_name = f"stock_select_估值_{timestamp}.txt"
        # 构造完整文件路径
        file_path = os.path.join(self.data_dir, file_name)

        # 确保目录存在
        os.makedirs(self.data_dir, exist_ok=True)

        # 将结果写入文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("选出的股票编码_stocks:\n")
            f.write(json.dumps(filter_stocks, ensure_ascii=False, indent=4))
            f.write("\n\n选出股票的宽表指标数据:\n")
            with pd.option_context('display.float_format', '{:,.2f}'.format):
                f.write(filter_df.to_string(index=False))
            f.write("\n\n选出股票的历年报表数据:\n")


        print(f"结果已保存到文件: {file_path}")
    def save_results_to_file_by_report(self, filter_stocks, annual_reports, filter_simple_reports):
        # 生成时间戳
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        # 构造文件名
        file_name = f"stock_select_策略_{timestamp}.txt"
        # 构造完整文件路径
        file_path = os.path.join(self.data_dir, file_name)

        # 确保目录存在
        os.makedirs(self.data_dir, exist_ok=True)

        # 将结果写入文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("选出的股票编码_stocks:\n")
            f.write(json.dumps(filter_stocks, ensure_ascii=False, indent=4))
            f.write("\n\n选出股票的宽表指标数据:\n")
            with pd.option_context('display.float_format', '{:,.2f}'.format):
                f.write(filter_simple_reports.to_string(index=False))
            f.write("\n\n选出股票的历年报表数据:\n")
            # f.write(self.convert_filter_annual_reports_to_json(annual_reports))
            for fiscal_year, reports in annual_reports.items():
                f.write(f"\n\n===== 年报日期: {fiscal_year} =====\n")

                for report_type, df in reports.items():
                    f.write(f"\n--- 报表类型: {report_type} ---\n")
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        # 写入 DataFrame 的字符串表示
                        df = df.sort_values(by='股票代码')
                        with pd.option_context('display.float_format', '{:,.2f}'.format):
                            f.write(df.to_string(index=False))
                    else:
                        f.write("<该报表为空或非 DataFrame 类型>")

        print(f"结果已保存到文件: {file_path}")

    def merge_financial_tables_to_wide(self, filter_stocks, annual_reports):
        """
        将三张财务指标宽表（净利润同比、营收增长、资产负债率）按股票代码和年份合并为一张大表

        参数:


        返回:
            pd.DataFrame: 合并后的完整报表
        """
        df1 = self.get_stock_financial_wide_table(annual_reports,stock_codes=filter_stocks, report_type='lrb',column_name='净利润同比')
        df2 = self.get_stock_financial_wide_table(annual_reports, stock_codes=filter_stocks,report_type='lrb', column_name='营业总收入同比')
        df3  = self.get_stock_financial_wide_table(annual_reports, stock_codes=filter_stocks, report_type='zcfz', column_name='资产负债率')

        df4 = self.get_stock_financial_wide_table(annual_reports, stock_codes=filter_stocks, report_type='lrb',
                                                  column_name='净利润')
        df5 = self.get_stock_financial_wide_table(annual_reports, stock_codes=filter_stocks, report_type='lrb',
                                                  column_name='营业总收入')
        df6 = self.get_stock_financial_wide_table(annual_reports, stock_codes=filter_stocks, report_type='zcfz',
                                                  column_name='负债-总负债')
        df7 = self.get_stock_financial_wide_table(annual_reports, stock_codes=filter_stocks, report_type='lrb',
                                                  column_name='净资产收益率（ROE）')


        # 步骤一：先合并前两张表
       #  merged_df = pd.merge(df1, df2, on=['股票代码'], suffixes=('_profit', '_revenue'))
        # 步骤二：再与第三张表合并
       #  merged_df = pd.merge(merged_df, df3, on=['股票代码'])
       # 步骤四：整理列顺序，使年份列按时间排序
       # stock_cols = ['股票代码', '股票简称']
       # year_cols = sorted([col for col in merged_df.columns if '_' in col], key=lambda x: x.split('_')[-1])
        #
        # final_df = merged_df[stock_cols + year_cols]

        combined_df = pd.concat([df2, df1], ignore_index=True)
        combined_df = pd.concat([combined_df, df3], ignore_index=True)
        combined_df = pd.concat([combined_df, df4], ignore_index=True)
        combined_df = pd.concat([combined_df, df5], ignore_index=True)
        combined_df = pd.concat([combined_df, df7], ignore_index=True)
        final_df = pd.concat([combined_df, df6], ignore_index=True)

        # 步骤三：重命名列名，让字段更清晰
        final_df = final_df.sort_values(by='股票代码')
        final_df.apply(lambda x: x.map(self.format_float))
        return final_df

    def fetch_stock_recport(self,years = 5):
        stock_service = stockBorderInfo(market=self.market)
        
        # 获取最近5年的日期列表
        report_dates = self.get_report_year_str_list(years)

        # 存储每年的报表数据
        annual_reports = {}

        # 定义一个函数用于获取单个年份的年报数据
        def fetch_report(date):
            zcfz, lrb, xjll = stock_service.get_stock_border_report(market=self.market, date=date)
            if not zcfz.empty and not lrb.empty and not xjll.empty:
                stock_service.calculate_financial_indicators(zcfz, lrb, xjll)
                return [zcfz, lrb, xjll]
            return None

        # 使用线程池并行获取年报数据
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 并行执行 fetch_report 函数
            results = list(executor.map(fetch_report, report_dates))

        # 将结果合并到 annual_reports 中
        for date, result in zip(report_dates, results):
            if result:
                annual_reports[date] = {'zcfz': result[0], 'lrb': result[1], 'xjll': result[2]}

        return annual_reports

    def fetch_stock_filter_recport(self, annual_reports, filter_stocks):
        # 提取 common_stocks 对应的 annual_reports 数据
        common_annual_reports = {}
        for date, reports in annual_reports.items():
            zcfz = reports['zcfz']
            lrb = reports['lrb']
            xjll = reports['xjll']

            # 过滤与 common_stocks 相关的数据
            filtered_zcfz = zcfz[zcfz['股票代码'].isin(filter_stocks)]
            filtered_lrb = lrb[lrb['股票代码'].isin(filter_stocks)]
            filtered_xjll = xjll[xjll['股票代码'].isin(filter_stocks)]

            filtered_zcfz.apply(lambda x: x.map(self.format_float))
            filtered_lrb.apply(lambda x: x.map(self.format_float))
            filtered_xjll.apply(lambda x: x.map(self.format_float))

            common_annual_reports[date] = {
                'zcfz': filtered_zcfz,
                'lrb': filtered_lrb,
                'xjll': filtered_xjll
            }
        return common_annual_reports

        # 获取最近5年的日期列表
    # 筛选利润率每年增长在10%以上的股票
    def filter_profit_growth(self, annual_reports,profit_growth = 10,threshold=4):
        filtered_stocks = []
        stock_year_count = {}
        for date, reports in annual_reports.items():
            lrb = reports['lrb']
            for _, row in lrb.iterrows():
                stock_code = row['股票代码']
                net_profit_growth = row['净利润同比']
                if net_profit_growth >= profit_growth :
                    stock_year_count[stock_code] = stock_year_count.get(stock_code, 0) + 1
                else:
                    stock_year_count[stock_code] = stock_year_count.get(stock_code, 0) + 0
        filtered_stocks = [stock for stock, count in stock_year_count.items() if count >= threshold]
        filtered_stocks_df = self.get_stock_financial_wide_table(annual_reports,stock_codes=filtered_stocks, report_type='lrb',column_name='净利润同比')
        print("filtered_stocks_df:", filtered_stocks_df.to_string(index=False))

        return filtered_stocks

    # 筛选营业额每年增长在10%以上的股票
    def filter_revenue_growth(self, annual_reports,revenue_growth_threshold=10,threshold=4):
        filtered_stocks = []
        stock_year_count = {}
        for date, reports in annual_reports.items():
            lrb = reports['lrb']
            for _, row in lrb.iterrows():
                stock_code = row['股票代码']
                revenue_growth = row['营业总收入同比']
                if revenue_growth >= revenue_growth_threshold:
                    stock_year_count[stock_code] = stock_year_count.get(stock_code, 0) + 1
                else:
                    stock_year_count[stock_code] = stock_year_count.get(stock_code, 0) + 0
        filtered_stocks = [stock for stock, count in stock_year_count.items() if count >= threshold]
        filtered_stocks_df = self.get_stock_financial_wide_table(annual_reports, stock_codes=filtered_stocks,
                                                                 report_type='lrb', column_name='营业总收入同比')
        print("filtered_stocks_df:", filtered_stocks_df.to_string(index=False))

        return filtered_stocks

    def filter_by_column_threshold(self,df, col_name, threshold, comparison='ge'):
        """
        根据列的数值与阈值比较过滤 DataFrame 数据

        参数:
            df (pd.DataFrame): 输入的 DataFrame
            col_name (str): 列名，如 '净利润同比'
            threshold (float/int): 阈值
            comparison (str): 比较方式:
                              'ge' 大于等于 (>=)
                              'le' 小于等于 (<=)
                              'gt' 大于 (>)
                              'lt' 小于 (<)
                              'eq' 等于 (==)

        返回:
            pd.DataFrame: 过滤后的 DataFrame
        """
        if col_name not in df.columns:
            return df

        if comparison == 'ge':
            return df[df[col_name] >= threshold]
        elif comparison == 'le':
            return df[df[col_name] <= threshold]
        elif comparison == 'gt':
            return df[df[col_name] > threshold]
        elif comparison == 'lt':
            return df[df[col_name] < threshold]
        elif comparison == 'eq':
            return df[df[col_name] == threshold]
        else:
            raise ValueError("comparison 必须是 ge、le、gt、lt 或 eq")

    # 筛选负债率在40%以下的股票
    def filter_debt_ratio(self, annual_reports,debt_ratio_threshold=40,threshold=4):
        stock_year_count = {}
        for date, reports in annual_reports.items():
            zcfz = reports['zcfz']
            for _, row in zcfz.iterrows():
                stock_code = row['股票代码']
                debt_ratio = row['资产负债率']
                if debt_ratio <= debt_ratio_threshold:
                    stock_year_count[stock_code] = stock_year_count.get(stock_code, 0) + 1
                else:
                    stock_year_count[stock_code] = stock_year_count.get(stock_code, 0) + 0
        filtered_stocks = [stock for stock, count in stock_year_count.items() if count >=threshold ]
        filtered_stocks_df = self.get_stock_financial_wide_table(annual_reports, stock_codes=filtered_stocks,
                                                                 report_type='zcfz', column_name='资产负债率')
        print("filtered_stocks_df:", filtered_stocks_df.to_string(index=False))

        return filtered_stocks

    def get_stock_financial_wide_table(self, annual_reports, stock_codes, report_type='lrb', column_name='净利润同比'):
        """
        获取多个股票在指定报表字段上的宽表（年份为列），并新增「股票简称」字段

        参数:
            annual_reports (dict): 年度报表数据 {date: {'zcfz': df, 'lrb': df, 'xjll': df}}
            stock_codes (list): 股票代码列表，例如 ['600139', '000001']
            report_type (str): 报表类型，支持 'zcfz', 'lrb', 'xjll'
            column_name (str): 需要提取的列名，例如 '净利润同比'

        返回:
            pd.DataFrame: 宽表格式 DataFrame，包含「股票简称」
        """
        data = []

        for stock_code in stock_codes:
            row = {'股票代码': stock_code, '指标': column_name}
            company_name = None  # 初始化股票简称

            for fiscal_year, reports in annual_reports.items():
                report_df = reports.get(report_type)
                if report_df is None:
                    continue

                # 筛选该股票的记录
                stock_row = report_df[report_df['股票代码'] == stock_code]

                if not stock_row.empty:
                    # 提取股票简称（只需要一次）
                    if company_name is None and '股票简称' in stock_row.columns:
                        company_name = stock_row['股票简称'].values[0]

                    # 提取目标字段值
                    if column_name in stock_row.columns:
                        value = stock_row[column_name].values[0]
                    else:
                        value = None

                    row[fiscal_year] = value

            row['股票简称'] = company_name
            data.append(row)

        result_df = pd.DataFrame(data)
        cols = ['股票代码', '股票简称', '指标'] + sorted(
            [col for col in result_df.columns if col not in ['股票代码', '股票简称', '指标']])
        result_df = result_df[cols]
        return result_df

    def convert_filter_annual_reports_to_json(self, filter_annual_reports):
        # 递归地将 filter_annual_reports 中的 DataFrame 转换为 dict
        def convert(obj):
            if isinstance(obj, pd.DataFrame):
                return obj.replace({pd.NA: None, float('nan'): None}).to_dict(orient='records')
            elif isinstance(obj, dict):
                return {k: convert(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert(i) for i in obj]
            else:
                return obj

        # 先转换结构
        serializable_data = convert(filter_annual_reports)

        # 再转成 JSON 字符串
        return json.dumps(serializable_data, ensure_ascii=False, indent=4)

    @staticmethod
    def format_float(x):
        if isinstance(x, (float, int)):
            return f"{x:.1f}"
        return x

    # 计算 DCF 模型的价格
    def calculate_stock_dcf_price(self,date='20250331'):
      dcf_service =  stockDCFSimpleModel(self.market)
      stock_service = stockBorderInfo(market=self.market)
     #  "代码","名称", "股东户数统计截止日-本次", "区间涨跌幅", "户均持股市值", "户均持股数量", "总市值", "总股本", "公告日期", "股东户数-本次", "股东户数-上次",  "股东户数-增减", "股东户数-增减比例", "股东户数统计截止日-上次", "最新价","涨跌幅",
      df_stock_gdhs = ak.stock_zh_a_gdhs(symbol='最新')
      df_stock_gdhs['代码'] = df_stock_gdhs['代码'].astype(np.int64)
      zcfz, lrb, xjll = stock_service.get_stock_border_report(market=self.market, date=date)
      merged = pd.merge(zcfz, df_stock_gdhs, left_on='股票代码', right_on='代码', how='left')
      zcfz['资产-总股本'] = merged['总股本']
      zcfz['资产-总市值'] = merged['总市值']
      df = dcf_service.calculate_stock_price_range(zcfz, lrb, xjll)
      stock_zh_a_spot_em_df = stock_service.get_stock_spot()
      stock_zh_a_spot_em_df['代码'] = stock_zh_a_spot_em_df['代码'].astype(np.int64)

      merged_df = pd.merge(df,stock_zh_a_spot_em_df, left_on='股票代码', right_on='代码', how='left')

      merged_df.apply(lambda x: self.format_float(x))
      self.save_results_to_file_by_price(merged_df['股票代码'].tolist(),merged_df)


      return merged_df


    def select_stock_and_analyse(self):
        filter_stocks, filter_annual_reports = self.select_stock_by_report(5)
        current_date = datetime.datetime.now()
        current_date_str = current_date.strftime("%Y-%m-%d")
        ai_platform, api_token, list_model = self.get_ai_token()
        index = 0
        for stock_code in filter_stocks:
            try:
                market = self.market
                stock_code = str(stock_code)
                # 上海证券交易所（6开头）
                if stock_code.startswith('6'):
                    market =  'SH'
                # 深圳证券交易所（00开头或3开头）
                elif  stock_code.startswith('00') or stock_code.startswith('3'):
                    market = 'SZ'
                else:
                    market = self.market
                    print(f'Warn unkonw code :{stock_code}')
                model = list_model[index % len(list_model)]
                stock_code = str(stock_code)
                stock_code = self.stock_analyse(ai_platform, api_token, current_date_str, market, model, stock_code)
                index = index+1
            except Exception as e:
                print(f"股票代码 {stock_code} 分析出错: {e}")
                traceback.print_exc()
                continue

    def stock_analyse_one(self, stock_code,  market, model):
        ai_platform, api_token, list_model = self.get_ai_token()
        stock_code = str(stock_code)
        current_date = datetime.datetime.now()
        current_date_str = current_date.strftime("%Y-%m-%d")
        stock_result = self.stock_analyse(ai_platform, api_token, current_date_str, market, model, stock_code)
        print(stock_result)

    def analyse_stock_and_analyse(self, file_name):
        df = pd.read_csv(file_name)
        # 检查必要的列是否存在
        required_columns = ['代码', 'market']
        if not all(col in df.columns for col in required_columns):
            missing = [col for col in required_columns if col not in df.columns]
            raise ValueError(f"CSV文件中缺少必要的列: {', '.join(missing)}")

        ai_platform,api_token,list_model = self.get_ai_token()

        current_date = datetime.datetime.now()
        current_date_str = current_date.strftime("%Y-%m-%d")

        # 遍历每一行
        for index, row in df.iterrows():
            stock_code = row['代码']  # 获取代码列的值
            market = str(row['market'])  # 获取market列的值
            try:
                # 每处理100次数据后更新list_model
                if index % 100 == 0 and index != 0:
                    ai_platform, api_token, list_model = self.get_ai_token()
                    print(f"已更新list_model，当前值: {list_model}")

                # 通过取模运算循环选择list_model中的元素
                model = list_model[index % len(list_model)]
                stock_code = str(stock_code)
                stock_result = self.stock_analyse(ai_platform, api_token, current_date_str, market, model, stock_code)
                print(stock_result)

            except Exception as e:
                print(f"股票代码 {stock_code} 分析出错: {e}")
                traceback.print_exc()
                continue

    def stock_analyse(self, ai_platform, api_token, current_date_str, market, model, stock_code):
        market, stock_code = self.process_stock_and_market(market, stock_code)
        ai_service_2 = StockAiAnalyzer(model=model, ai_platform=ai_platform,
                                       api_token=api_token)
        report = ai_service_2.stock_indicator_analyse(market=market, symbol=stock_code,
                                                      start_date='2025-01-01',
                                                      end_date=current_date_str)
        print(report)
        ai_service = StockAiAnalyzer(model=model, ai_platform=ai_platform,
                                     api_token=api_token)
        report = ai_service.stock_report_analyse(market=market, symbol=stock_code)
        print(report)
        return stock_code

    def process_stock_and_market(self, market, stock_code):
        if (market == 'SH'):
            # 上海证券交易所（6开头）
            if stock_code.startswith('6'):
                market = 'SH'
            # 深圳证券交易所（00开头或3开头）
            elif stock_code.startswith('00') or stock_code.startswith('3'):
                market = 'SZ'
            else:
                market = self.market
                print(f'Warn unkonw code :{stock_code}')
        else:
            if stock_code.lower().endswith('us'):
                stock_code = stock_code[:-2]  # 移除最后两个字符
                stock_code = f'105.{stock_code}'
            elif stock_code.lower().endswith('hk'):
                stock_code = stock_code[:-2]
            if market == 'HK':
                market = 'H'
            if market == 'USA':
                market = 'usa'
        return market, stock_code

    def get_ai_token(self):
        import json
        from dotenv import load_dotenv

        load_dotenv()
        # 加载环境变量中的 OpenAI API 密钥
        CURRENT_AI = os.getenv('CURRENT_AI')
        DASHSCOPE_API_KEY = os.getenv('DASHSCOPE_API_KEY')

        DASHSCOPE_MODEL_LIST = os.getenv('DASHSCOPE_MODEL_LIST', '')
        DASHSCOPE_MODEL_LIST = json.loads(DASHSCOPE_MODEL_LIST)


        print(DASHSCOPE_MODEL_LIST)  # 输出: ['qwen3-30b-a3b', 'qwen3-14b', ...]

        KIMI_API_KEY = os.getenv('KIMI_API_KEY')
        KIMI_MODEL_LIST = os.getenv('KIMI_MODEL_LIST')
        KIMI_MODEL_LIST = json.loads(KIMI_MODEL_LIST)


        print(KIMI_MODEL_LIST)

        print(f'{CURRENT_AI}_ {DASHSCOPE_API_KEY} _ {DASHSCOPE_MODEL_LIST}')
        if CURRENT_AI == 'qwen':
            return CURRENT_AI,DASHSCOPE_API_KEY,DASHSCOPE_MODEL_LIST
        elif CURRENT_AI == 'kimi':
            return CURRENT_AI,KIMI_API_KEY,KIMI_MODEL_LIST
        else:
            return  CURRENT_AI,DASHSCOPE_API_KEY,DASHSCOPE_MODEL_LIST



