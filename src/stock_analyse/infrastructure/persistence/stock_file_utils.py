from __future__ import annotations

import logging
import os
import re
import traceback
from datetime import datetime
from typing import Dict, List

import numpy as np
import pandas as pd


class StockFileUtils:
    def __init__(self, min_score: float = 30, market='SH', name='1'):
        self.min_score = min_score
        self.logger = logging.getLogger(__name__)
        self.market = market
        self.currency_symbol = '$' if str(market).lower() == 'usa' else '¥'
        now = datetime.now()
        self.time_str = now.strftime('%Y%m%d%H')
        current_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(current_dir, '../../../..'))
        self.cache_dir = os.path.join(project_root, 'cache/selector_result')
        self.filePath = os.path.join(self.cache_dir, f'{market}_analyse_{name}_{self.time_str}')
        self.analyseFilePath = os.path.join(self.cache_dir, f'{market}_analyse_{name}_{self.time_str}')
        os.makedirs(self.filePath, exist_ok=True)
        os.makedirs(self.analyseFilePath, exist_ok=True)

    @staticmethod
    def format_float(x):
        if isinstance(x, (float, int)):
            return f'{x:.1f}'
        return x

    def _prepare_df_for_markdown(self, df: pd.DataFrame) -> pd.DataFrame:
        """准备用于 Markdown 输出的 DataFrame，转义特殊字符。"""
        if df is None or df.empty:
            return df
        
        # 复制一份以避免修改原始数据
        df_display = df.copy()
        
        # 遍历所有对象（字符串）类型的列
        for col in df_display.select_dtypes(include=['object']):
            # 将列中的 | 替换为 \|，防止 Markdown 表格乱序
            # 同时将换行符替换为 <br> 以在单行单元格内显示换行
            df_display[col] = df_display[col].apply(
                lambda x: str(x).replace('|', '\|').replace('\n', '<br>') if x is not None else x
            )
        return df_display

    def save_intermediate_results(self, results: List[Dict]) -> None:
        try:
            df = pd.DataFrame(results)
            df.apply(lambda x: x.map(self.format_float))
            high_score_stocks = df[df['score'] >= self.min_score].sort_values('score', ascending=False)
            output_lines = [
                '=' * 80,
                f"股票扫描中间结果 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f'共分析 {len(results)} 支股票',
                '=' * 80,
                f'\n发现 {len(high_score_stocks)} 支高分股票（得分≥{self.min_score}）：',
            ]
            for _, row in high_score_stocks.iterrows():
                output_lines.extend([
                    f"\n股票代码: {row['stock_code']}",
                    f"建议:{row['suggestion']} |得分: {row['score']:.1f} | 价格: {self.currency_symbol}{row['price']} | 涨跌幅: {row['price_change']}% \n{row['signal']}",
                ])

            tmp_file = os.path.join(self.filePath, 'temp_results.txt')
            with open(tmp_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(output_lines))
            
            # 处理 Markdown 格式
            df_markdown = self._prepare_df_for_markdown(df)
            result_file = os.path.join(self.analyseFilePath, 'results_all.txt')
            with open(result_file, 'w', encoding='utf-8') as f:
                with pd.option_context('display.float_format', lambda x: f'{x:.2f}'):
                    f.write(df_markdown.to_markdown())
        except Exception as exc:
            self.logger.error(f'保存中间结果失败：{exc}')
            traceback.print_exc()

    def save_high_score_stocks(self, df_results):
        formatted_results = []
        try:
            high_score_stocks = df_results[df_results['score'] >= self.min_score].sort_values('score', ascending=False)
            high_score_stocks.apply(lambda x: x.map(self.format_float))
            for _, row in high_score_stocks.iterrows():
                formatted_row = {
                    '股票代码': row['stock_code'],
                    '评分': f"{row['score']:.1f}",
                    '当前价格': f"{self.currency_symbol}{row['price']}",
                    '涨跌幅': f"{row['price_change']}%",
                    '投资建议': row['suggestion'],
                    '建议详情': row['signal'],
                }
                exclude_fields = {'stock_code', 'score', 'price', 'price_change', 'suggestion', 'signal'}
                for col in row.index:
                    if col not in exclude_fields:
                        formatted_row[col] = row[col]
                formatted_results.append(formatted_row)
            
            # 处理 Markdown 格式
            high_score_markdown = self._prepare_df_for_markdown(high_score_stocks)
            result_file = os.path.join(self.analyseFilePath, 'results_high_score.txt')
            with open(result_file, 'w', encoding='utf-8') as f:
                with pd.option_context('display.float_format', lambda x: f'{x:.2f}'):
                    f.write(high_score_markdown.to_markdown())
            return formatted_results
        except Exception as exc:
            self.logger.error(f'保存中间结果失败：{exc}')
            traceback.print_exc()
            return formatted_results

    def format_price_category(self, price: float) -> str:
        if price is None:
            return '0'
        base = (price // 10) * 10
        if base is None:
            return '0'
        try:
            return f'{int(base)}-{int(base + 10)}'
        except ValueError:
            return '0'

    def save_results_by_price(self, results: List[Dict]) -> None:
        try:
            os.makedirs('scanner', exist_ok=True)
            price_groups = {}
            for stock in results:
                price_str = str(stock['当前价格']).replace('¥', '').replace('$', '')
                price = float(price_str) if price_str else 0.0
                if price is None:
                    price = 0.0
                category = self.format_price_category(price)
                price_groups.setdefault(category, []).append(stock)

            if not price_groups:
                self.create_summary_file(price_groups)
                return

            for category, stocks in price_groups.items():
                output_lines = [
                    '=' * 80,
                    f'股票分析结果 - 价格区间: {category}元',
                    f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    '=' * 80,
                    f'\n该区间共发现 {len(stocks)} 支高分股票（得分≥85）：',
                    '-' * 80,
                ]
                stocks.sort(key=lambda x: float(x['评分']), reverse=True)
                for i, stock in enumerate(stocks, 1):
                    output_lines.extend([
                        f"\n{i}. 股票代码: {stock['股票代码']}  股票名称: {stock['stock_name']}  市值: {stock['市值']}",
                        f"   评分: {stock['评分']} | 价格: {stock['当前价格']} | 涨跌幅: {stock['涨跌幅']}",
                        f"   投资建议: {stock['投资建议']}",
                        f"   建议详情: {stock['建议详情']}",
                        '-' * 80,
                    ])
                output_lines.extend([
                    f'\n价格区间 {category}元 分析汇总：',
                    f'1. 股票数量: {len(stocks)}',
                    f"2. 平均评分: {np.mean([float(stock['评分']) for stock in stocks]):.1f}",
                    f"3. 买入信号股票数: {sum(1 for stock in stocks if stock['投资建议'] == '建议买入')}",
                ])
                filename = os.path.join(self.analyseFilePath, f"price_{category.replace('-', '_')}.txt")
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(output_lines))
            self.create_summary_file(price_groups)
        except Exception as exc:
            logging.error(f'保存结果时发生错误: {exc}')
            raise

    def create_summary_file(self, price_groups: Dict[str, List[Dict]]) -> None:
        try:
            output_lines = [
                '=' * 80,
                'A股市场优质股票筛选报告',
                f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                '=' * 80,
            ]
            total_stocks = sum(len(stocks) for stocks in price_groups.values())
            all_scores = [float(stock['评分']) for stocks in price_groups.values() for stock in stocks]
            if not all_scores:
                output_lines.extend([
                    '\n整体统计：',
                    '1. 共筛选出 0 支高分股票（得分≥85）',
                    '2. 本次没有满足条件的候选股票。',
                ])
            else:
                output_lines.extend([
                    '\n整体统计：',
                    f'1. 共筛选出 {total_stocks} 支高分股票（得分≥85）',
                    f'2. 平均评分: {np.mean(all_scores):.1f}',
                    f'3. 最高评分: {max(all_scores):.1f}',
                    '\n各价格区间分布：',
                    '-' * 80,
                ])
                for category, stocks in sorted(price_groups.items(), key=lambda x: int(re.findall(r'\d+', x[0])[0])):
                    output_lines.extend([
                        f'\n价格区间 {category}元：',
                        f'  - 股票数量: {len(stocks)}',
                        f"  - 平均评分: {np.mean([float(stock['评分']) for stock in stocks]):.1f}",
                    ])
            filename = os.path.join(self.analyseFilePath, 'summary.txt')
            with open(filename, 'w', encoding='utf-8') as f:
                f.write('\n'.join(output_lines))
        except Exception as exc:
            logging.error(f'生成汇总报告失败：{exc}')
            raise

    def save_error_log(self, e: Exception) -> None:
        error_msg = f'\n程序错误：{str(e)}\n'
        print('=' * 80)
        print(error_msg)
        print('=' * 80)
        filename = os.path.join(self.filePath, 'error_log.txt')
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('Stock Analysis System Error Report\n')
            f.write('=' * 80 + '\n')
            f.write(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f'Error: {str(e)}\n')
            f.write('=' * 80 + '\n')
            f.write(f'详细堆栈信息:\n{traceback.format_exc()}')

    def create_middle_file(self, file_name, df: pd.DataFrame) -> None:
        if df is None:
            return
        filename = os.path.join(self.analyseFilePath, f'{file_name}.md')
        
        # 处理 Markdown 格式
        df_markdown = self._prepare_df_for_markdown(df)
        file_content = df_markdown.to_markdown()
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(file_content)

    def create_text_file(self, file_name, file_content) -> None:
        if file_content is None:
            return
        filename = os.path.join(self.analyseFilePath, f'{file_name}.md')
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(file_content)

    def read_text_file(self, file_name) -> str:
        filename = os.path.join(self.analyseFilePath, f'{file_name}')
        if not os.path.exists(filename):
            return ''
        with open(filename, 'r', encoding='utf-8') as f:
            file_content = f.read()
        return file_content
