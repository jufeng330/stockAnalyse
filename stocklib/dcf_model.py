import pandas as pd

class stockDCFSimpleModel:
    def __init__(self,market='SZ'):
        # 获取当前日期
        self.market = market
        # 生成当前日期的指定格式字符串
    def calculate_dcf(self, df, discount_rate=0.1, growth_rate=0.03):
        fcf = df['经营性现金流-现金流量净额']
        df['rate'] = df['净利润同比']/100
        df['rate'] = df['rate'].where(df['rate'] <= 1, 1)

        recent_growth_rate = df['rate']

        future_fcf = [fcf.iloc[-1] * (1 + recent_growth_rate) ** i for i in range(1, 6)]

        present_values = []
        for i, fcf in enumerate(future_fcf):
            present_value = fcf / ((1 + discount_rate) ** (i + 1))
            present_values.append(present_value)

        terminal_value = future_fcf[-1] * (1 + growth_rate) / (discount_rate - growth_rate)
        terminal_present_value = terminal_value / ((1 + discount_rate) ** 5)

        dcf_value = sum(present_values) + terminal_present_value
        return dcf_value


    def calculate_stock_price_range(self,zcfz, lrb, xjll):
        df = pd.merge(zcfz, lrb, on='股票代码', how='inner')
        df = pd.merge(df, xjll, on='股票代码', how='inner')

        # 选择所需列
        columns = ['股票代码', '股票简称', '负债-总负债', '资产-货币资金', '资产-总股本', '净利润同比',
                   '经营性现金流-现金流量净额']
        df = df[columns]

        total_debt = df["负债-总负债"]
        cash_equivalents = df["资产-货币资金"]
        total_shares = df["资产-总股本"]


        # 计算正常情况下的DCF和股价
        normal_dcf = self.calculate_dcf(df)
        equity_value = normal_dcf - (total_debt - cash_equivalents)
        normal_stock_price = equity_value / total_shares

        # 保守情景：较高折现率，较低增长率
        conservative_dcf = self.calculate_dcf(df=df, discount_rate=0.12, growth_rate=0.02)
        conservative_equity_value = conservative_dcf - (total_debt - cash_equivalents)
        lower_stock_price = conservative_equity_value / total_shares

        # 乐观情景：较低折现率，较高增长率
        optimistic_dcf = self.calculate_dcf(df=df, discount_rate=0.08, growth_rate=0.04)
        optimistic_equity_value = optimistic_dcf - (total_debt - cash_equivalents)
        upper_stock_price = optimistic_equity_value / total_shares
        df['dcf_lower_stock_price'] = lower_stock_price
        df['dcf_normal_stock_price'] = normal_stock_price
        df['dcf_upper_stock_price'] = upper_stock_price
        return df


    # 示例数据
    def calculate_stock_test(self,zcfz,lrb,xjll):

        total_shares = 100  # 总股本

        lower, normal, upper = self.calculate_stock_price_range(zcfz, lrb, xjll, total_shares)
        print(f"股价下限: {lower}")
        print(f"正常股价: {normal}")
        print(f"股价上限: {upper}")
