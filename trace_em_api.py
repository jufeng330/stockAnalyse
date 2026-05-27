import akshare as ak
import sys
import traceback

def test_em_api():
    symbols = ['600712', '600706', '600711']
    for sym in symbols:
        try:
            print(f"Testing {sym}...")
            df = ak.stock_individual_info_em(symbol=sym)
            print(f"Success for {sym}, df shape: {df.shape}")
        except Exception as e:
            print(f"Error for {sym}: {e}")
            traceback.print_exc()

if __name__ == '__main__':
    test_em_api()
