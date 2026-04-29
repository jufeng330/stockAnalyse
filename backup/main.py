# Legacy scanner entrypoint retained for compatibility; the full market scan workflow now lives in src/stock_analyse/application/use_cases/run_full_market_scan.py.
from stock_analyse.application.use_cases import run_full_market_scan as run_full_market_scan_use_case


def main():
    print("\n" + "=" * 80)
    print("Market-Wide High-Score Stock Scanner".center(76))
    print("=" * 80)

    result = run_full_market_scan_use_case.execute()
    for summary in result['data']['summaries']:
        market = summary['market']
        strategy_type = summary['strategy_type']
        print(f"\n开始全盘扫描股票{market}_{strategy_type}……")
        if summary['qualified'] == 0:
            print("\n未找到得分大于等于85分的股票。")
            continue
        print(f"\n回测结果：{summary['stats']}")
        print("\n分析完成！结果已保存至 scanner 文件夹中：")
        print("1. 按价格区间保存的详细分析文件（price_XX_YY.txt）")
        print("2. 汇总报告（summary.txt）")
        print("\n" + "=" * 80)

    input("\n按Enter键退出……")


if __name__ == '__main__':
    main()
