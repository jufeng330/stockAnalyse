
import os
import time
import random
import logging
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Union
import pandas as pd
import numpy as np
import sys


class  StockFileUtils:
    """全盘筛选高打分股票的扫描器
    输出的字段包括

    """

    def __init__(self, min_score: float = 30,market = 'SH'):
        """
        初始化扫描器

        Args:
            max_workers: 并发线程数量（已增至20以加速分析）
            min_score: 高分最低阈值
        """
        self.min_score = min_score
        self.logger = logging.getLogger(__name__)
        self.market = market
        now = datetime.now()
        self.time_str = now.strftime("%Y%m%d%H%M%S")
        self.filePath = os.path.join(os.path.dirname(__file__), f'result/tmp_{self.time_str}')
        self.analyseFilePath = os.path.join(os.path.dirname(__file__), f'result/analyse_{self.time_str}')
        os.makedirs(self.filePath, exist_ok=True)
        os.makedirs(self.analyseFilePath, exist_ok=True)

    @staticmethod
    def format_float(x):
        if isinstance(x, (float, int)):
            return f"{x:.1f}"
        return x

    # 保存扫描的结果
    def save_intermediate_results(self, results: List[Dict]) -> None:
        """周期性保存中间结果，便于后续查看进度"""
        try:
            df = pd.DataFrame(results)
            df.apply(lambda x: x.map(self.format_float))
            high_score_stocks = df[df['score'] >= self.min_score].sort_values('score', ascending=False)
            output_lines = [
                "=" * 80,
                f"股票扫描中间结果 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"共分析 {len(results)} 支股票",
                "=" * 80,
                f"\n发现 {len(high_score_stocks)} 支高分股票（得分≥{self.min_score}）："
            ]
            for _, row in high_score_stocks.iterrows():
                output_lines.extend([
                    f"\n股票代码: {row['stock_code']}",
                    f"建议:{row['suggestion']} |得分: {row['score']:.1f} | 价格: ¥{row['price']} | 涨跌幅: {row['price_change']}% \n{row['signal']}"
                ])


            tmp_file = os.path.join(self.filePath,'temp_results.txt')
            with open(tmp_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(output_lines))
            result_file = os.path.join(self.analyseFilePath,'results_all.txt')
            with open(result_file, 'w', encoding='utf-8') as f:
                with pd.option_context('display.float_format', lambda x: f'{x:.2f}'):
                    f.write(df.to_markdown())

        except Exception as e:
            self.logger.error(f"保存中间结果失败：{str(e)}")
            traceback.print_exc()
    def save_high_score_stocks(self, df_results) :
        """保存高分数据的结果"""
        formatted_results = []
        try:
            high_score_stocks = df_results[df_results['score'] >= self.min_score].sort_values('score', ascending=False)
            high_score_stocks.apply(lambda x: x.map(self.format_float))
            for _, row in high_score_stocks.iterrows():
                # 基础格式化字段
                formatted_row = {
                    '股票代码': row['stock_code'],
                    '评分': f"{row['score']:.1f}",
                    '当前价格': f"¥{row['price']}",
                    '涨跌幅': f"{row['price_change']}%",
                    '投资建议': row['suggestion'],
                    '建议详情': row['signal']
                }

                # 动态添加其他所有字段（排除已处理的字段）
                exclude_fields = {'stock_code', 'score', 'price', 'price_change', 'suggestion', 'signal'}
                for col in row.index:
                    if col not in exclude_fields:
                        formatted_row[col] = row[col]

                formatted_results.append(formatted_row)
            result_file = os.path.join(self.analyseFilePath, 'results_high_score.txt')
            with open(result_file, 'w', encoding='utf-8') as f:
                with pd.option_context('display.float_format', lambda x: f'{x:.2f}'):
                    f.write(high_score_stocks.to_markdown())

            return formatted_results
        except Exception as e:
            self.logger.error(f"保存中间结果失败：{str(e)}")
            traceback.print_exc()
            return formatted_results

    # -------------------------------
    # **结果分组与报告生成**
    # -------------------------------
    def format_price_category(self,price: float) -> str:
        """将价格划分为区间（例如 32.5 -> '30-40'）"""
        if price is None:
            return '0'
        base = (price // 10) * 10
        return f"{int(base)}-{int(base + 10)}"

    def save_results_by_price(self,results: List[Dict]) -> None:
        """按价格区间保存分析结果至文件"""
        try:
            os.makedirs('scanner', exist_ok=True)
            price_groups = {}
            for stock in results:
                price = float(stock['当前价格'].replace('¥', ''))
                if price is None:
                    price = 0
                category = self.format_price_category(price)
                price_groups.setdefault(category, []).append(stock)

            for category, stocks in price_groups.items():
                output_lines = [
                    "=" * 80,
                    f"股票分析结果 - 价格区间: {category}元",
                    f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "=" * 80,
                    f"\n该区间共发现 {len(stocks)} 支高分股票（得分≥85）：",
                    "-" * 80
                ]
                stocks.sort(key=lambda x: float(x['评分']), reverse=True)
                for i, stock in enumerate(stocks, 1):
                    output_lines.extend([
                        f"\n{i}. 股票代码: {stock['股票代码']}  股票名称: {stock['stock_name']}  市值: {stock['市值']}",
                        f"   评分: {stock['评分']} | 价格: {stock['当前价格']} | 涨跌幅: {stock['涨跌幅']}",
                        f"   投资建议: {stock['投资建议']}",
                        f"   建议详情: {stock['建议详情']}",
                        "-" * 80
                    ])
                output_lines.extend([
                    f"\n价格区间 {category}元 分析汇总：",
                    f"1. 股票数量: {len(stocks)}",
                    f"2. 平均评分: {np.mean([float(stock['评分']) for stock in stocks]):.1f}",
                    f"3. 买入信号股票数: {sum(1 for stock in stocks if stock['投资建议'] == '建议买入')}"
                ])

                filename = os.path.join(self.analyseFilePath, f'price_{category.replace("-", "_")}.txt')
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(output_lines))
            self.create_summary_file(price_groups)
        except Exception as e:
            logging.error(f"保存结果时发生错误: {str(e)}")
            raise

    def create_summary_file(self,price_groups: Dict[str, List[Dict]]) -> None:
        """生成综合汇总报告"""
        try:
            output_lines = [
                "=" * 80,
                "A股市场优质股票筛选报告",
                f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "=" * 80
            ]
            total_stocks = sum(len(stocks) for stocks in price_groups.values())
            all_scores = [float(stock['评分']) for stocks in price_groups.values() for stock in stocks]

            output_lines.extend([
                "\n整体统计：",
                f"1. 共筛选出 {total_stocks} 支高分股票（得分≥85）",
                f"2. 平均评分: {np.mean(all_scores):.1f}",
                f"3. 最高评分: {max(all_scores):.1f}",
                "\n各价格区间分布：",
                "-" * 80
            ])
            for category, stocks in sorted(price_groups.items(), key=lambda x: float(x[0].split('-')[0])):
                output_lines.extend([
                    f"\n价格区间 {category}元：",
                    f"  - 股票数量: {len(stocks)}",
                    f"  - 平均评分: {np.mean([float(stock['评分']) for stock in stocks]):.1f}"
                ])
            filename = os.path.join(self.analyseFilePath, f'summary.txt')
            with open(filename, 'w', encoding='utf-8') as f:
                f.write('\n'.join(output_lines))
        except Exception as e:
            logging.error(f"生成汇总报告失败：{str(e)}")
            raise
    def save_error_log(self, e: Exception) -> None:
        """保存错误日志"""
        error_msg = f"\n程序错误：{str(e)}\n"
        print("=" * 80)
        print(error_msg)
        print("=" * 80)
        filename = os.path.join(self.filePath, f'error_log.txt')
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("Stock Analysis System Error Report\n")
            f.write("=" * 80 + "\n")
            f.write(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Error: {str(e)}\n")
            f.write("=" * 80 + "\n")
            f.write(f"详细堆栈信息:\n{traceback.format_exc()}")