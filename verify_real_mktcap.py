import akshare as ak
import pandas as pd
import sys
import time

def verify_real_maotai_data():
    print("--- 正在尝试通过不同接口获取贵州茅台 (600519) 真实市场数据 ---")
    
    # 接口 1: stock_zh_a_spot_em
    try:
        df = ak.stock_zh_a_spot_em()
        row = df[df['代码'] == '600519'].iloc[0]
        print(f"[接口1] 名称: {row['名称']}, 最新价: {row['最新价']}, 总市值: {row['总市值']}")
    except Exception as e:
        print(f"[接口1] 失败: {e}")

    time.sleep(1)

    # 接口 2: stock_individual_info_em (个股详情)
    try:
        df = ak.stock_individual_info_em(symbol="600519")
        # 该接口通常返回：总市值、流通市值、行业、上市时间等
        print("[接口2] 个股详情:")
        print(df.to_string(index=False))
    except Exception as e:
        print(f"[接口2] 失败: {e}")

if __name__ == "__main__":
    verify_real_maotai_data()
