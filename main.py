import akshare as ak
import pandas as pd
import datetime
from stocklib.stock_border import stockBorderInfo

class StockMain:
    def __init__(self):
        pass
    
    def get_stock_trading_data(self, stock_code, market, days=30):
        """
        根据股票代码和market获取最近指定天数的成交数据
        
        Args:
            stock_code (str): 股票代码
            market (str): 市场代码，如 'SH', 'SZ', 'H', 'usa'
            days (int): 最近天数，默认为30天
            
        Returns:
            pd.DataFrame: 股票成交数据
        """
        try:
            # 构建完整的股票代码
            if market == 'SH':
                symbol = f"sh{stock_code}"
            elif market == 'SZ':
                symbol = f"sz{stock_code}"
            elif market == 'H':
                symbol = stock_code
            elif market == 'usa':
                symbol = stock_code
            else:
                print(f"不支持的市场类型: {market}")
                return pd.DataFrame()
            
            # 计算开始日期
            end_date = datetime.datetime.now().strftime('%Y%m%d')
            start_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y%m%d')
            
            # 获取股票历史数据
            if market in ['SH', 'SZ']:
                # A股数据
                df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date)
            elif market == 'H':
                # 港股数据
                df = ak.stock_hk_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date)
            elif market == 'usa':
                # 美股数据
                df = ak.stock_us_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date)
            else:
                return pd.DataFrame()
            
            return df
        except Exception as e:
            print(f"获取股票成交数据失败: {str(e)}")
            return pd.DataFrame()
    
    def get_stock_financial_reports(self, stock_code, market):
        """
        根据股票代码和market获取最近10年的财报数据
        
        Args:
            stock_code (str): 股票代码
            market (str): 市场代码，如 'SH', 'SZ', 'H', 'usa'
            
        Returns:
            tuple: (资产负债表, 利润表, 现金流量表)
        """
        try:
            # 创建stockBorderInfo实例
            border_info = stockBorderInfo(market=market)
            
            # 获取当前年份
            current_year = datetime.datetime.now().year
            
            # 存储10年的财报数据
            zcfz_list = []
            lrb_list = []
            xjll_list = []
            
            # 遍历最近10年
            for year in range(current_year - 9, current_year + 1):
                # 构建财报日期
                if market in ['SH', 'SZ']:
                    # A股年报日期通常是次年的3月31日
                    report_date = f"{year}0331"
                else:
                    # 港股和美股年报日期通常是12月31日
                    report_date = f"{year}1231"
                
                try:
                    # 获取财报数据
                    zcfz, lrb, xjll = border_info.get_stock_border_report(market=market, date=report_date, indicator='年报')
                    
                    # 筛选当前股票的数据
                    if not zcfz.empty:
                        zcfz_stock = zcfz[zcfz['股票代码'] == stock_code]
                        if not zcfz_stock.empty:
                            zcfz_list.append(zcfz_stock)
                    
                    if not lrb.empty:
                        lrb_stock = lrb[lrb['股票代码'] == stock_code]
                        if not lrb_stock.empty:
                            lrb_list.append(lrb_stock)
                    
                    if not xjll.empty:
                        xjll_stock = xjll[xjll['股票代码'] == stock_code]
                        if not xjll_stock.empty:
                            xjll_list.append(xjll_stock)
                except Exception as e:
                    print(f"获取{year}年财报数据失败: {str(e)}")
                    continue
            
            # 合并数据
            if zcfz_list:
                zcfz_all = pd.concat(zcfz_list, ignore_index=True)
            else:
                zcfz_all = pd.DataFrame()
            
            if lrb_list:
                lrb_all = pd.concat(lrb_list, ignore_index=True)
            else:
                lrb_all = pd.DataFrame()
            
            if xjll_list:
                xjll_all = pd.concat(xjll_list, ignore_index=True)
            else:
                xjll_all = pd.DataFrame()
            
            return zcfz_all, lrb_all, xjll_all
        except Exception as e:
            print(f"获取财报数据失败: {str(e)}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

if __name__ == "__main__":
    # 示例用法
    stock_main = StockMain()
    
    # 示例1: 获取最近30天的成交数据
    print("\n=== 最近30天成交数据 ===")
    trading_data = stock_main.get_stock_trading_data('600519', 'SH', 30)
    print(trading_data.head())
    
    # 示例2: 获取最近10年的财报数据
    print("\n=== 最近10年财报数据 ===")
    zcfz, lrb, xjll = stock_main.get_stock_financial_reports('600519', 'SH')
    print("资产负债表:")
    print(zcfz.head())
    print("\n利润表:")
    print(lrb.head())
    print("\n现金流量表:")
    print(xjll.head())
