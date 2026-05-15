import akshare as ak
try:
    df = ak.stock_a_indicator_lg(symbol="600519")
    print("Success")
except AttributeError:
    print("AttributeError")
except Exception as e:
    print(f"Other Error: {e}")
