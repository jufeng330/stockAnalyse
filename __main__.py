import sys
import os

# 获取项目根目录
root_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_dir)
from stocklib.stock_company import stockCompanyInfo
from stocklib.stock_annual_report import stockAnnualReport
from stocklib.stock_border import stockBorderInfo
from stockAI.stockAgent.stock_ai_analyzer import  StockAiAnalyzer
from scanner.stock_fh_analyser import *
from scanner.stock_financial_analyser import *
from scanner.stock_report_analyser import *
from scanner.stock_analyzer import StockAnalyzer
from stocklib.utils_file_cache import FileCacheUtils
from stocklib.stock_concept_service import stockConcepService
from stocklib.stock_wave_analyser import StockWaveAnalyzer
from stocklib.stock_sentiment_analysis import StockSentimentAnalysis
# matplotlib.use('Agg')
# matplotlib.use('TkAgg')  # 或 'Qt5Agg'

import warnings
warnings.filterwarnings("ignore", message="Valid config keys have changed in V2")



def print_stock_wave():

    stock_sen = StockSentimentAnalysis()
    sentiment_score,sentiment_analysis = stock_sen.get_sentiment_analysis()
    print(sentiment_score)
    print(f"公司情绪分析内容: {sentiment_analysis}")
    # 确保安装最新版本
    print(f"当前AKShare版本: {ak.__version__}")



    stock = StockWaveAnalyzer()
    # 调用函数获取数据
    for code in ['600028','600028','600028','600028','600028']:
        result_df = stock.analysis_stock_wave()
        if result_df is not None:
            print((result_df.to_markdown()))
        df_wave,total_trend,last_trend = stock.analysis_stock_pren_wave()
        print(f'{code}:{total_trend},{last_trend} ,{not df_wave.iloc[-1]}')



    print(f'End')

def print_stock_concept():
    # 确保安装最新版本
    print(f"当前AKShare版本: {ak.__version__}")



    stock = stockConcepService()
    # 调用函数获取数据
    stock.get_all_sectors_and_stocks()

    stock_company = stockCompanyInfo()
    df = stock_company.save_stock_board_all_concept_name()

    print(f'End')


def print_stock_strategy():
    market = 'SH'
    code_list = ['301096','605365', '600028']
    stock = StockAnalyzer(market=market)
    try:

        stockService = stockBorderInfo(market= market)
        # df_stock = stock.get_stock_border_info()
        data = {
            '代码': ['300750'],
            'market': ['SZ']
        }
        # 创建DataFrame
        df_stock = pd.DataFrame(data)
        df_stock_data = stockService.get_stock_spot()

        for code in code_list:
            try:
                df_stock = df_stock_data[df_stock_data['股票代码'] == code].copy()
                df_stock['market'] = market
                summary = stock.analyze_stock(stock=df_stock.iloc[0], market="SH")

                print(f"符合条件的股票数量: {len(summary)}")
                print("达标股票汇总:")

            except Exception as e:
                print(f"调用方法时发生属性错误，请检查对象是否正确初始化: {e}")
                traceback.print_exc()


    except AttributeError as e:
        print(f"调用方法时发生属性错误，请检查对象是否正确初始化: {e}")
    except Exception as e:
        print(f"发生未知错误: {e}")
        traceback.print_exc()

def print_stock_report(market='usa'):

    stock_border = stockBorderInfo(market=market)
    stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = stock_border.get_stock_border_report(market,
                                                                                               date='20241231',
                                                                                               indicator='年报')
    print(stock_zcfz_em_df.head(1).to_markdown())
    print(stock_lrb_em_df.head(1).to_markdown())
    print(stock_xjll_em_df.head(1).to_markdown())


def print_fh_stock():
    stock = StockFenHengAnalyser(market='SH')
    try:
        df_fh, summary = stock.get_fh_codes()
        print(f"符合条件的股票数量: {len(summary)}")
        print("达标股票汇总:")
        print(summary.to_markdown())
    except AttributeError as e:
        print(f"调用方法时发生属性错误，请检查对象是否正确初始化: {e}")
    except Exception as e:
        print(f"发生未知错误: {e}")
        traceback.print_exc()



def print_report_stock():
    stock = StockReportAnalyser(market='SH')

    market_list = ['SH',  'usa']
    date_list = ['20241231','20231231','20221231','20211231','20201231']
    # date_list = ['20221231']

    # market = 'SH'
    # date = '20241231'
    for market in market_list:
        cache_service = FileCacheUtils(market=market, cache_dir='history_' + market)
        for date in date_list:
            try:

                stock_border = stockBorderInfo(market=market)
                stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = stock_border.get_stock_border_report(market,
                                                                                                           date=date,
                                                                                                           indicator='年报')
                cache_service.write_to_cache_db(date, 'zcfz', stock_zcfz_em_df)
                cache_service.write_to_cache_db(date, 'lrb', stock_lrb_em_df)
                cache_service.write_to_cache_db(date, 'xjll', stock_xjll_em_df)

            except Exception as e:
                print(f"调用方法时发生属性错误，请检查对象是否正确初始化: {e}")


    try:
        df_report_filter_zcfz,df_report_filter_lrb,df_report_filter_xjll,df_pivot_zcfz,df_pivot_lrb,df_pivot_xjll = stock.get_report_codes()
        print(f"符合条件的股票数量: {len(df_pivot_zcfz)}")
        print("达标股票汇总:")
        print(df_pivot_zcfz.to_markdown())
        print(df_pivot_lrb.to_markdown())
        print(df_pivot_xjll.to_markdown())
    except AttributeError as e:
        print(f"调用方法时发生属性错误，请检查对象是否正确初始化: {e}")
    except Exception as e:
        print(f"发生未知错误: {e}")
        traceback.print_exc()

def print_financial_stock():
    stock = StockFinancialAnalyser(market='SH')
    try:
        df_fh, summary = stock.get_financial_codes(strategy_filter='continue',threshold_1=5000 * 10000, threshold_2=0.10, threshold_3=0.10)
        print(f"符合条件的股票数量: {len(summary)}")
        print("达标股票汇总:")
        print(summary.to_markdown())
    except AttributeError as e:
        print(f"调用方法时发生属性错误，请检查对象是否正确初始化: {e}")
    except Exception as e:
        print(f"发生未知错误: {e}")
        traceback.print_exc()

def print_stock_border():
    global stock, df_fh, market, result, df_stock_board, e
    report_service = stockAnnualReport()
    stock_border_info = stockBorderInfo(market='SH')
    stock = stockBorderInfo(market='SH')
    try:

        stock_border = stockBorderInfo(market='SH')
        df_fh = stock_border.get_stock_fhps_info(date='20241231')
        print(df_fh.to_markdown())

        stock_border = stockBorderInfo(market='H')
        df_stock = stock_border_info.get_stock_border_info()
        print(df_stock.to_markdown())

        market = 'SH'
        print_stock_report(market)

        market = 'H'
        print_stock_report(market)

        market = 'usa'
        print_stock_report(market)

        df_stock = stock.get_stock_border_info()
        print(df_stock.to_markdown())

        market = 'H'
        stock_border = stockBorderInfo(market=market)
        stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = stock_border.get_stock_border_report(market,
                                                                                                   date='20241231',
                                                                                                   indicator='年报')
        stock_border.write_to_csv_force(stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df, '20241231')
        print(stock_zcfz_em_df.to_markdown())
        print(stock_lrb_em_df.to_markdown())
        print(stock_xjll_em_df.to_markdown())

        market = 'SH'
        stock_border = stockBorderInfo(market=market)
        stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = stock_border.get_stock_border_report(market,
                                                                                                   date='20250331',
                                                                                                   indicator='年报')
        print(stock_zcfz_em_df.to_markdown())
        print(stock_lrb_em_df.to_markdown())
        print(stock_xjll_em_df.to_markdown())

        stock_border_info = stockBorderInfo(market='usa')
        df_stock = stock_border_info.get_stock_border_info()
        print(df_stock.to_markdown())

        stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = stock_border_info.get_stock_border_report(market="H",
                                                                                                        date='20241231',
                                                                                                        indicator='年报')

        stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = report_service.get_stock_border_report()
        print("资产负债表")
        print(stock_zcfz_em_df.to_string(index=False))
        print("利润表")
        print(stock_lrb_em_df.to_string(index=False))
        print("现金流量表")
        print(stock_xjll_em_df.to_string(index=False))



        result = stockSelectService.stock_analyse_one(stock_code='01810', market='H', model='qwen2.5-14B-instruct')
        print(result)

        file = '/Users/jujinbu/PycharmProjects/StockAnalyse/stock_analyse/stock_list_all.csv'
        stockSelectService.analyse_stock_and_analyse(file_name=file)
        stockSelectService.select_stock_and_analyse()

        # 测试 get_stock_border_report 方法
        #  zcfz_df, lrb_df, xjll_df = stockBorderInfo.get_stock_border_report(market="H", date='20240331',
        #                                                                    indicator='年报')

        stockSelectService = stockSelectService(market='H')

        filter_price = stockSelectService.select_stock_by_price_valuation()
        print('所有推荐股票数据 股票编码：')
        print(filter_price.to_string(index=False))

        df = stockSelectService.calculate_stock_dcf_price()
        print('所有推荐股票数据 股票编码：')
        print(df.to_string(index=False))

        filter_stocks, filter_annual_reports = stockSelectService.select_stock_by_report(5)

        print('所有推荐股票数据 股票编码：')
        print(filter_stocks)
        print(stockSelectService.convert_filter_annual_reports_to_json(filter_annual_reports))

        stock_border_info = stock_border_info(market='SH')

        # 测试 get_stock_border_report 方法
        zcfz_df, lrb_df, xjll_df = stock_border_info.get_stock_border_report(market="SH", date='20240331',
                                                                             indicator='年报')
        stock_border_info.calculate_financial_indicators(zcfz=zcfz_df, lrb=lrb_df, xjll=xjll_df)
        print('所有股票的资产负债表')
        if zcfz_df is not None:
            print(zcfz_df.to_string(index=False))
        print('所有股票的利润表')
        if lrb_df is not None:
            print(lrb_df.to_string(index=False))
        print('所有股票的现金流量表')
        if xjll_df is not None:
            print(xjll_df.to_string(index=False))

        # 测试 get_stock_all_info 方法
        df_stock_info = stock_border_info.get_stock_all_info()
        print('所有股票数据的实时信息和市盈率等指标')
        print(df_stock_info.to_string(index=False))

        # 测试 get_stock_spot 方法
        df_stock_spot = stock_border_info.get_stock_spot()
        print('所有股票的实时行情')
        print(df_stock_spot.to_string(index=False))

        # 测试 get_stock_board_all_concept_name 方法
        df_stock_board = stock_border_info.get_stock_board_all_concept_name()
        print('所有的板块信息')
        if df_stock_board is not None:
            print(df_stock_board.to_string(index=False))

        # 测试 get_stock_all_code 方法
        df_stock_code = stock_border_info.get_stock_all_code()
        print('所有股票的代码和名称')
        print(df_stock_code.to_string(index=False))

        # 测试 get_stock_hsgt_hold_stock_em 方法
        df_stock_hsgt = stock_border_info.get_stock_hsgt_hold_stock_em()
        print('北向的持仓数据')
        print(df_stock_hsgt.to_string(index=False))

        stockAIAnalysis = StockAiAnalyzer(model='qwen3', ai_platform='qwen', api_token='8d852738bdd847669e105bbfa2c756')
        report = stockAIAnalysis.stock_report_analyse(market='SH', symbol='600028')
        print(report)
        report = stockAIAnalysis.stock_report_analyse(market='H', symbol='09868')
        print(report)
        report = stockAIAnalysis.stock_report_analyse(market='usa', symbol='105.MSFT')
        print(report)
        stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = report_service.get_stock_report(stock_code='105.MSFT',
                                                                                              market="usa")
        print("资产负债表")
        print(stock_zcfz_em_df.to_string(index=False))
        print("利润表")
        print(stock_lrb_em_df.to_string(index=False))
        print("现金流量表")
        print(stock_xjll_em_df.to_string(index=False))

        stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = report_service.get_stock_report(stock_code='600028',
                                                                                              market="SH")
        print("资产负债表")
        print(stock_zcfz_em_df.to_string(index=False))
        print("利润表")
        print(stock_lrb_em_df.to_string(index=False))
        print("现金流量表")
        print(stock_xjll_em_df.to_string(index=False))

        stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = report_service.get_stock_report(stock_code='09868',
                                                                                              market="H")
        print("资产负债表")
        print(stock_zcfz_em_df.to_string(index=False))
        print("利润表")
        print(stock_lrb_em_df.to_string(index=False))
        print("现金流量表")
        print(stock_xjll_em_df.to_string(index=False))



    except AttributeError as e:
        print(f"调用方法时发生属性错误，请检查对象是否正确初始化: {e}")
    except Exception as e:
        print(f"发生未知错误: {e}")
        traceback.print_exc()


def print_stock_company():
    global df_stock_board, e
    stock_service = stockCompanyInfo('SZ', '002624')
    stock_service = stockCompanyInfo('SH', '600028')
    stock_service = stockCompanyInfo('zq', '511090')
    stock_service = stockCompanyInfo('sz', '300033')
    stock_service = stockCompanyInfo('H', '01810')
    stock_service = stockCompanyInfo('usa', '105.TSLA')
    try:
        functions = [func for func in dir(ak) if '_us_' in func]
        print(functions)

        # 调用 get_stock_name 方法
        stock_name = stock_service.get_stock_name()
        print("股票名称：", stock_name)

        # 调用 get_stock_board_all_concept_name 方法
        board_all_concept_name = stock_service.get_stock_board_all_concept_name()
        print("概念板块名称：", board_all_concept_name.to_string(index=False))

        # 先获取概念板块名称，再调用 get_stock_board_concept_name 方法
        df_stock_board = stock_service.get_stock_board_all_concept_name()
        concept_name = stock_service.get_stock_board_concept_name(stock_service.symbol, df_stock_board)
        print("概念板块的数据情况：", concept_name)

        # 调用 get_stock_zyjs 方法
        zyjs = stock_service.get_stock_zyjs()
        print("主营业务介绍：", zyjs.to_string(index=False))

        # 调用 get_stock_individual_info_em 方法
        individual_info, list_date, industry = stock_service.get_stock_individual_info_em()
        print("个股信息：", individual_info)
        print("上市时间：", list_date)
        print("行业：", industry)

        # 调用 get_stock_news 方法
        news = stock_service.get_stock_news()
        print("个股新闻：", news)

        # 调用 get_stock_fund_flow 方法
        fund_flow = stock_service.get_stock_fund_flow()
        print("当前个股所在行业板块情况：", fund_flow)

        # 调用 get_stock_individual_fund_flow 方法
        individual_fund_flow = stock_service.get_stock_individual_fund_flow()
        print("历史的个股资金流：", individual_fund_flow)

        # 调用 # 财务指标
        financial_indicator = stock_service.get_stock_financial_analysis_indicator()
        print("财务指标：", financial_indicator)

        # 调用 get_stock_fund_flow_individual 方法
        fund_flow_individual = stock_service.get_stock_fund_flow_individual(stock_name)
        print("即时的个股资金流：", fund_flow_individual)

        # 调用 get_stock_share_change_cninfo 方法
        share_change = stock_service.get_stock_share_change_cninfo()
        print("个股公司股本变动：", share_change)

        # 调用 分红配送详情
        fhps_detail = stock_service.get_stock_fhps_detail_ths()
        print("分红配送详情：", fhps_detail)

        # 调用高管持股数据
        ggcg_em = stock_service.get_stock_ggcg_em(stock_name)
        print("高管持股数据：", ggcg_em)

        # 调用 get_stock_history_data 方法
        history_data = stock_service.get_stock_history_data(start_date_str='2025-01-01', end_date_str='2025-05-01')
        print("个股历史数据：", history_data)
    except AttributeError as e:
        print(f"调用方法时发生属性错误，请检查对象是否正确初始化: {e}")
    except Exception as e:
        print(f"发生未知错误: {e}")


if __name__ == '__main__':

    print_stock_concept()
    print_report_stock()

    print_stock_strategy()

    print_financial_stock()

    print_stock_wave()
    print_stock_strategy()

    print_fh_stock()

    print_stock_border()

    print_stock_company()

