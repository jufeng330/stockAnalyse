import akshare as ak
import datetime
import pandas as pd
from .stock_concept_data import stockConceptData
from .stock_news_data import stockNewsData
from .stock_ak_indicator import stockAKIndicator

# 个股相关信息查询
"""
  需要缓存的数据：
  获取所有股票数据  stock_fund_flow_individual_df = ak.stock_fund_flow_individual(symbol="即时")
  获取所有板块信息: stock_board = stock_concept_service.stock_board_concept_name_ths()
"""
class stockCompanyInfo:
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

    def get_default_df(self ):
        default_data = {
            'item': ['Item'],
            'stock': [self.symbol],
            'market': [self.market]
        }
        default_df = pd.DataFrame(default_data)
        return default_df

    def get_usa_code(self):
        if self.market == self.usa:
            symbol = self.symbol.split('.',1)
            if len(symbol) > 1:
                return symbol[1]
            return self.symbol
        else:
            return self.symbol

    #  获取概念板块名称
    def get_stock_board_all_concept_name(self):
        if self.market  == self.HongKong or self.market == self.usa:
            return self.get_default_df()
        stock_concept_service = stockConceptData()
        stock_board = stock_concept_service.stock_board_concept_name_ths()

        return stock_board

    # 获取概念板块的数据情况
    def get_stock_board_concept_name(self, symbol, df_stock_board):
        if self.market  == self.HongKong or self.market == self.usa:
            return self.get_default_df()
        stock_concept_service = stockConceptData()
        concept_info_df = stock_concept_service.stock_board_concept_info_ths(symbol=symbol,
                                                                     stock_board_ths_map_df=df_stock_board)
        return concept_info_df

    # 主营业务介绍 根据主营业务网络搜索相关事件报道
    def get_stock_zyjs(self):
        if self.market  == self.HongKong or self.market == self.usa:
            return self.get_default_df()
        stock_zyjs_ths_df = ak.stock_zyjs_ths(symbol=self.symbol)
        return stock_zyjs_ths_df

    def get_stock_name(self):
        try:
            if self.market == self.ETF:
                return self.symbol
            elif self.market == self.HongKong:
                stock_individual = ak.stock_individual_basic_info_hk_xq(symbol=self.symbol, token=self.xq_a_token)
                stock_name = stock_individual[stock_individual['item'] == 'comcnname']['value'].values[0]
                return stock_name
            elif self.market == self.usa:
                symbol = self.get_usa_code()
                stock_individual_info_em_df = ak.stock_individual_basic_info_us_xq(symbol=symbol, token=self.xq_a_token)
                stock_name = \
                stock_individual_info_em_df[stock_individual_info_em_df['item'] == 'org_name_cn']['value'].values[0]
                return stock_name
            else:
                stock_individual_info_em_df = ak.stock_individual_info_em(symbol=self.symbol)
                stock_name = \
                stock_individual_info_em_df[stock_individual_info_em_df['item'] == '股票简称']['value'].values[0]
                return stock_name
        except Exception as e:
            print(f"调用方法时发生属性错误，请检查对象是否正确初始化: {e}")
            import traceback
            traceback.print_exc()
            return self.symbol


    # 个股信息查询

    def get_stock_individual_info(self):
        # 个股信息查询
        # 个股信息查询
        if self.market == self.ETF:
            # 创建一个默认的 DataFrame
            default_df = self.get_default_df()
            return default_df
        elif self.market == self.HongKong:
            stock_individual_info_em_df = ak.stock_individual_basic_info_hk_xq(symbol=self.symbol,
                                                                               token=self.xq_a_token)
            return stock_individual_info_em_df
        elif self.market == self.usa:
            symbol = self.get_usa_code()
            stock_individual_info_em_df = ak.stock_individual_basic_info_us_xq(symbol=symbol, token=self.xq_a_token)
            return stock_individual_info_em_df
        else:
            stock_individual_info_em_df = ak.stock_individual_info_em(symbol=self.symbol)
            return stock_individual_info_em_df

    # 个股信息查询
    def get_stock_individual_info_em(self):
        # 个股信息查询
        if self.market == self.ETF:
            # 创建一个默认的 DataFrame
            default_df = self.get_default_df()
            default_list_date = "2000-01-01"
            default_industry = "EFT"
            return default_df, default_list_date, default_industry
        elif self.market  == self.HongKong:
            stock_individual_info_em_df = ak.stock_individual_basic_info_hk_xq(symbol=self.symbol,token=self.xq_a_token)
            list_date = '2010-01-01'
            # 提取行业
            industry = ''
            return stock_individual_info_em_df, list_date, industry
        elif self.market == self.usa:
            symbol = self.get_usa_code()
            stock_individual_info_em_df = ak.stock_individual_basic_info_us_xq(symbol=symbol, token=self.xq_a_token)
            list_date = '2010-01-01'
            industry = ''
            return stock_individual_info_em_df, list_date, industry
        else:
            stock_individual_info_em_df = ak.stock_individual_info_em(symbol=self.symbol)
            # 提取上市时间
            list_date = stock_individual_info_em_df[stock_individual_info_em_df['item'] == '上市时间']['value'].values[
                0]
            # 提取行业
            industry = stock_individual_info_em_df[stock_individual_info_em_df['item'] == '行业']['value'].values[0]
            return stock_individual_info_em_df, list_date, industry

    # 个股新闻查询
    def get_stock_news(self):
        # 个股新闻
        stock_news_em_df = stockNewsData.stock_news_em(symbol=self.symbol, pageSize=10)
        # 删除指定列
        stock_news_em_df = stock_news_em_df.drop(["文章来源", "新闻链接"], axis=1)

        return stock_news_em_df

    # # 获取当前个股所在行业板块情况
    def get_stock_fund_flow(self):
        # 获取当前个股所在行业板块情况
        stock_sector_fund_flow_rank_df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
        return stock_sector_fund_flow_rank_df

    # 历史的个股资金流
    def get_stock_individual_fund_flow(self):
        # 历史的个股资金流
        market_name = self.market.lower()
        if(self.market  == self.HongKong or self.market == self.usa):
            return self.get_default_df()
        if(self.market  == 'zq'):
            market_name = 'sh'
        stock_individual_fund_flow_df = ak.stock_individual_fund_flow(stock=self.symbol, market=market_name)
        # 转换日期列为 datetime 类型，以便进行排序
        stock_individual_fund_flow_df['日期'] = pd.to_datetime(stock_individual_fund_flow_df['日期'])
        # 按日期降序排序
        sorted_data = stock_individual_fund_flow_df.sort_values(by='日期', ascending=False)
        num_records = min(20, len(sorted_data))
        # 提取最近的至少20条记录，如果不足20条则提取所有记录
        recent_data = sorted_data.head(num_records)
        stock_individual_fund_flow_df = recent_data
        return stock_individual_fund_flow_df

     # 财务指标
    def get_stock_financial_analysis_indicator(self,start_year="2024"):
        if self.market  == self.HongKong:
            stock_financial_analysis_indicator_df = ak.stock_financial_hk_analysis_indicator_em(symbol=self.symbol)
            return stock_financial_analysis_indicator_df
        elif self.market == self.usa:
            symbol = self.get_usa_code()
            stock_financial_analysis_indicator_df = ak.stock_financial_us_analysis_indicator_em(symbol=symbol)
            return stock_financial_analysis_indicator_df
        else:
            stock_financial_analysis_indicator_df = ak.stock_financial_analysis_indicator(symbol=self.symbol, start_year=start_year)
            return stock_financial_analysis_indicator_df

    # 即时的个股资金流
    def get_stock_fund_flow_individual(self,stock_name = ''):
        # 即时的个股资金流
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
            print(f'get_stock_share_change_cninfo error stock_code:{self.symbol} {e}')
            default_df = self.get_default_df()
            import traceback
            traceback.print_exc()
            return default_df

    # 分红配送详情
    def get_stock_fhps_detail_ths(self):
        if self.market  == self.HongKong:
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

    def get_stock_history_data(self,start_date_str=None, end_date_str=None):

        current_date = datetime.datetime.now()
        if start_date_str is None :
            previous_date = current_date - datetime.timedelta(days=100)
            start_date_str =  previous_date.strftime("%Y%m%d")
        if end_date_str is None:
            end_date_str = current_date.strftime("%Y%m%d")
        stock_code = self.symbol
        start_date_str = start_date_str.replace('-', '')
        end_date_str = end_date_str.replace('-', '')
        stock = stockAKIndicator()

        stock_us_hist_df = stock.stock_day_data_code(stock_code, self.market, start_date_str, end_date_str)
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
        print(stock_us_hist_df)
        return stock_us_hist_df