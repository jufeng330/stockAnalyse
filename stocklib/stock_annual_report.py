import akshare as ak
import pandas as pd
import requests
from urllib.parse import urlparse, parse_qs
import tabula
from datetime import datetime, timedelta
import logging
import traceback


class stockAnnualReport:
    def __init__(self):
        # 定义 current_date 并格式化
        self.current_date = datetime.now()
        self.current_date_str = self.current_date.strftime("%Y%m%d")
        self.logger = logging.getLogger(__name__)

    # 定义格式化函数，作为静态方法
    @staticmethod
    def format_float(x):
        if isinstance(x, (float, int)):
            return f"{x:.1f}"
        return x

    def get_date_String(self, date_str):
        if date_str:
            try:
                # 将字符串转换为 datetime 对象
                dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                # 格式化为 yyyy-MM-dd 格式
                formatted_date = dt.strftime('%Y-%m-%d')
                self.logger.debug(formatted_date)
                return formatted_date
            except ValueError:
                self.logger.error("输入的时间字符串格式不符合要求。")
        else:
            self.logger.warn("未获取到有效的时间字符串。")
        return None

    def get_stock_report_file(self, stock_code='601668', market="沪深京", start_date='20200101', end_date='20241231'):
        # 获取特定股票的公告列表
        stock_gg_date_df = ak.stock_zh_a_disclosure_report_cninfo(symbol=stock_code, market=market,
                                                                 start_date=start_date, end_date=end_date)
        # self.logger.debug(stock_gg_date_df.to_string())
        # 筛选出年报公告
        annual_reports = stock_gg_date_df[stock_gg_date_df['公告标题'].str.contains('年度报告')]
        # self.logger.debug(annual_reports.to_string())

        file_list = []
        # 下载年报文件
        for index, row in annual_reports.iterrows():
            url = row['公告链接']
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            file_id = query_params.get('announcementId', [None])[0]
            date = query_params.get('announcementTime', [None])[0]
            date = self.get_date_String(date)
            file_url = f'https://static.cninfo.com.cn/finalpage/{date}/{file_id}.PDF'
            file_name = "/Users/jujinbu/Downloads/stock/report/" + "" + stock_code + "_" + row['公告标题'] + '.pdf'
            try:
                response = requests.get(file_url)
                if response.status_code == 200:
                    with open(file_name, 'wb') as file:
                        file.write(response.content)
                        self.logger.debug(f'{file_name} 下载成功')

                    # 读取 PDF 文件
                    dfs = tabula.read_pdf(file_name, pages='all')

                    # 将表格保存为 Markdown
                    for i, df in enumerate(dfs):
                        file_name_md = file_name.replace(".pdf", f"_table_{i}.md")
                        df.to_csv(file_name_md, sep="|", na_rep="nan")
                    file_list.append(file_name)
                else:
                    self.logger.warn(f'{file_name} 下载失败，状态码: {response.status_code}')
            except Exception as e:
                self.logger.error(f'{file_name} 下载时出现错误: {e}')
        return file_list

    # 主营构成 表格
    def get_stock_zygc(self, stock_code='SH601668', market="沪深京"):
        stock_zygc_em_df = ak.stock_zygc_em(symbol=stock_code)
        stock_zygc_em_df = stock_zygc_em_df.applymap(self.format_float)
        self.logger.debug(stock_zygc_em_df)

        return stock_zygc_em_df

    # 获取所有股票的报表
    def get_stock_border_report(self,  market="SH", date='20241231', indicator='年报'):
        if market == 'SH' or market == 'SZ':
            # 资产负债表
            stock_zcfz_em_df = ak.stock_zcfz_em(date=date)
            # 利润表
            stock_lrb_em_df = ak.stock_lrb_em(date=date)
            # 现金流量表
            stock_xjll_em_df = ak.stock_xjll_em(date=date)
        elif market == 'H':
            # 资产负债表 choice of {"年度", "报告期"
            if indicator == '年报':
                indicator = '年度'
            else:
                indicator = '报告期'
            return None, None, None

        elif market == 'usa':
            return None,None,None
            # {"资产负债表", "综合损益表", "现金流量表"}
            # {"年报", "单季报", "累计季报"}

        stock_zcfz_em_df.applymap(self.format_float)
        stock_lrb_em_df.applymap(self.format_float)
        stock_xjll_em_df.applymap(self.format_float)
        return stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df

    # 财务指标
    def get_stock_report(self, stock_code='601668', market="SH", indicator='年报',years = 5):
        try:
            def convert_and_assign_code(df, source_col, target_col):
                if source_col in df.columns:
                    df[target_col] = df[source_col].astype(str)
                return df

            if market == 'SH' or market == 'SZ':
                # 资产负债表
                # code = f'{market}{stock_code}'
                stock_zcfz_em_df = ak.stock_financial_report_sina(stock=stock_code, symbol="资产负债表")
                # 利润表
                stock_lrb_em_df = ak.stock_financial_report_sina(stock=stock_code, symbol="利润表")
                # 现金流量表
                stock_xjll_em_df = ak.stock_financial_report_sina(stock=stock_code, symbol="现金流量表")

                stock_zcfz_em_df = self.filter_stock_reprt_df(df=stock_zcfz_em_df, years=years)
                stock_lrb_em_df = self.filter_stock_reprt_df(df=stock_lrb_em_df, years=years)
                stock_xjll_em_df = self.filter_stock_reprt_df(df=stock_xjll_em_df, years=years)

                stock_zcfz_em_df = self.filter_stock_reprt_indicator(df=stock_zcfz_em_df, indicator=indicator)
                stock_lrb_em_df = self.filter_stock_reprt_indicator(df=stock_lrb_em_df, indicator=indicator)
                stock_xjll_em_df = self.filter_stock_reprt_indicator(df=stock_xjll_em_df, indicator=indicator)




            elif market == 'H':
                # 资产负债表 choice of {"年度", "报告期"
                if indicator == '年报':
                    indicator = '年度'
                else:
                    indicator = '报告期'
                stock_zcfz_em_df = ak.stock_financial_hk_report_em(
                    stock=stock_code, symbol="资产负债表", indicator=indicator)
                self.logger.debug(stock_zcfz_em_df)
                stock_lrb_em_df = ak.stock_financial_hk_report_em(
                    stock=stock_code, symbol="利润表", indicator=indicator)
                stock_xjll_em_df = ak.stock_financial_hk_report_em(
                    stock=stock_code, symbol="现金流量表", indicator=indicator)

                stock_zcfz_em_df = self.filter_stock_reprt_df(df=stock_zcfz_em_df, years=years,date_column='REPORT_DATE')
                stock_lrb_em_df = self.filter_stock_reprt_df(df=stock_lrb_em_df, years=years,date_column='REPORT_DATE')
                stock_xjll_em_df = self.filter_stock_reprt_df(df=stock_xjll_em_df, years=years,date_column='REPORT_DATE')


                self.logger.debug(stock_zcfz_em_df)
            elif market == 'usa':
                # {"资产负债表", "综合损益表", "现金流量表"}
                # {"年报", "单季报", "累计季报"}
                stock_code = self.get_stock_code(symbol=stock_code)
                stock_zcfz_em_df = ak.stock_financial_us_report_em(
                    stock=stock_code, symbol="资产负债表", indicator=indicator)
                self.logger.debug(stock_zcfz_em_df.to_string())
                stock_lrb_em_df = ak.stock_financial_us_report_em(
                    stock=stock_code, symbol="综合损益表", indicator=indicator)
                stock_xjll_em_df = ak.stock_financial_us_report_em(
                    stock=stock_code, symbol="现金流量表", indicator=indicator)
                stock_zcfz_em_df = self.filter_stock_reprt_df(df=stock_zcfz_em_df, years=years,
                                                              date_column='REPORT_DATE')
                stock_lrb_em_df = self.filter_stock_reprt_df(df=stock_lrb_em_df, years=years, date_column='REPORT_DATE')
                stock_xjll_em_df = self.filter_stock_reprt_df(df=stock_xjll_em_df, years=years,
                                                              date_column='REPORT_DATE')


                self.logger.debug(stock_zcfz_em_df)

            stock_zcfz_em_df = convert_and_assign_code(stock_zcfz_em_df, 'SECURITY_CODE', '股票代码')
            stock_lrb_em_df = convert_and_assign_code(stock_lrb_em_df, 'SECURITY_CODE', '股票代码')
            stock_xjll_em_df = convert_and_assign_code(stock_xjll_em_df, 'SECURITY_CODE', '股票代码')

            stock_zcfz_em_df.apply(lambda x: x.map(self.format_float))
            stock_lrb_em_df.apply(lambda x: x.map(self.format_float))
            stock_xjll_em_df.apply(lambda x: x.map(self.format_float))
            return stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df

        except Exception as e:
            self.logger.error(f"get_stock_report 发生错误 {stock_code}: {e}")

            # traceback.print_exc()
            return None, None, None


    def stock_test(self):
        stock_code_ = "106.BABA"
        stock_code_ = "09988"
        market_ = '港股'
        # get_stock_report_file(stock_code = stock_code_,market=market_,start_date = '20200101',end_date = '20241231')

        stock_code_ = "SH601668"
        stock_zygc_em_df = self.get_stock_zygc(stock_code=stock_code_, market=market_)
        self.logger.debug(stock_zygc_em_df)

    def get_stock_code(self, market='usa',symbol='105.TSLA'):
        if market == 'usa':
            parts = symbol.split('.', 1)
            if len(parts) > 1:
                return parts[1]
            return symbol
        return symbol

    # 过滤掉日期比 5 年前还小的数据
    def filter_stock_reprt_df(self, df,years=5,date_column='报告日'):
        if df is None:
            return None

            # 获取当前日期
        current_date = datetime.now()
        # 计算指定年前的日期
        prev_year = current_date - timedelta(days=365 * years)

        if date_column in df.columns:
            # 确保日期列是日期时间类型
            df[date_column] = pd.to_datetime(df[date_column])
            # 过滤掉日期比指定年前还小的数据
            filtered_df = df[df[date_column] >= prev_year]
            return filtered_df
        else:
            return df
    def filter_stock_reprt_indicator(self, df,date_column='报告日',indicator='年报'):
        if df is None:
            return None
        if date_column in df.columns:
            # 确保日期列是日期时间类型
            df[date_column] = pd.to_datetime(df[date_column])
            if indicator == '年报':
                # 过滤掉日期比指定年前还小的数据
                filtered_df = df[(df[date_column].dt.month == 3) & (df[date_column].dt.day == 31)]
            else:
                # 过滤出报告日不是 03 月 31 日的数据
                filtered_df = df[~((df[date_column].dt.month == 3) & (df[date_column].dt.day == 31))]
            return filtered_df
        else:
            return df