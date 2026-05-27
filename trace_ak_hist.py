import sys
import os
import pandas as pd
import akshare as ak
import time

def trace_ak_call():
    print("\nTesting ak.stock_zh_a_hist with EM source (symbol 603223)")
    try:
        start = time.time()
        # 尝试调用东方财富接口
        df = ak.stock_zh_a_hist(symbol="603223", period="daily", start_date="20230101", end_date="20260519", adjust="qfq")
        print(f"EM fetch time: {time.time() - start:.2f}s, rows: {len(df) if df is not None else 0}")
    except Exception as e:
        print(f"EM failed after {time.time() - start:.2f}s: {e}")

if __name__ == "__main__":
    trace_ak_call()
