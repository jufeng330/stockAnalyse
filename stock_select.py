
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

    scanner = TopStockScanner(max_workers=20,market='usa')  # 已提升至20线程
    file_utils = scanner.file_utils
    try:
        print("\n开始全盘扫描股票……")
        high_score_stocks = scanner.scan_high_score_stocks(batch_size=20)
        if not high_score_stocks:
            print("\n未找到得分大于等于85分的股票。")
            return
        file_utils.save_results_by_price(high_score_stocks)

        print(f"\n分析完成！结果已保存至 scanner 文件夹中：")
        print("1. 按价格区间保存的详细分析文件（price_XX_YY.txt）")
        print("2. 汇总报告（summary.txt）")
        print("\n" + "=" * 80)
        input("\n按Enter键退出……")

    except Exception as e:
        file_utils.save_error_log(e)
        print("错误日志已保存至 scanner/error_log.txt")
        input("\n按Enter键退出……")

if __name__ == "__main__":
    main()
