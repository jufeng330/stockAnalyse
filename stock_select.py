import os
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
import numpy as np
from scanner.top_stock_scanner import TopStockScanner
from scanner.stock_result_utils import  StockFileUtils

# -------------------------------
# **主程序入口**
# -------------------------------
def main():
    """程序主入口"""
    print("\n" + "=" * 80)
    print("Market-Wide High-Score Stock Scanner".center(76))
    print("=" * 80)
    markets = ['SH','H','usa']
    # markets = [ 'usa']
    for market in markets:
            print("\n开始全盘扫描股票……")
            if market == 'SH':
                type_array = [ 1,2,3, 4,5,6]
                # type_array = [5, 6]
            else:
                type_array = [4,6]
            file_utils = None
            for type in type_array:
                try:
                    scanner = TopStockScanner(max_workers=20, market=market,strategy_type = type)  # 已提升至20线程
                    file_utils = scanner.file_utils
                    print(f"\n开始全盘扫描股票{market}_{type}……")
                    high_score_stocks = scanner.scan_high_score_stocks(batch_size=20,type=type,strategy_filter='avg')
                    if not high_score_stocks:
                        print("\n未找到得分大于等于85分的股票。")
                        continue
                    file_utils.save_results_by_price(high_score_stocks)
                    df_high_score_stocks,stats = scanner.backtest_stocks(high_score_stocks,  '2025-06-06')
                    file_utils.create_middle_file('回测结果',df_high_score_stocks)
                    file_utils.create_text_file('回测结果_统计', stats)
                    print(f"\n回测结果：{stats}")
                    print(f"\n分析完成！结果已保存至 scanner 文件夹中：")
                    print("1. 按价格区间保存的详细分析文件（price_XX_YY.txt）")
                    print("2. 汇总报告（summary.txt）")

                    print("\n" + "=" * 80)
                except Exception as e:
                    file_utils.save_error_log(e)
                    print("错误日志已保存至 scanner/error_log.txt")

    input("\n按Enter键退出……")


if __name__ == "__main__":
    main()
