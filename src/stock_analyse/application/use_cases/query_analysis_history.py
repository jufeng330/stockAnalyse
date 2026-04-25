from __future__ import annotations

import os

import pandas as pd


def execute(analyzer_path: str, stock_code: str, market: str, date_str: str) -> dict:
    if not os.path.exists(analyzer_path):
        raise FileNotFoundError(f'目录不存在: {analyzer_path}')
    if not os.path.isdir(analyzer_path):
        raise NotADirectoryError(f'{analyzer_path} 不是一个目录')

    results = []
    for filename in os.listdir(analyzer_path):
        file_path = os.path.join(analyzer_path, filename)
        if not os.path.isfile(file_path) or 'request' in filename:
            continue

        parts = filename.split('_')
        if len(parts) < 5:
            continue

        stock_name = parts[0]
        indicator = parts[1]
        market_info = parts[2]
        model_name = parts[3]
        analysis_time = parts[-1].rstrip('.txt')
        url = f'/api/history/analyse?stock={stock_name}&market={market_info}&date={analysis_time}'
        results.append(
            {
                '文件名': filename,
                '股票名称': stock_name,
                'indicator': indicator,
                '市场': market_info,
                'model_name': model_name,
                '分析时间': analysis_time,
                'URL': f'[链接]({url})',
            }
        )

    df = pd.DataFrame(results)
    if stock_code and stock_code.strip():
        df = df[df['股票名称'].str.contains(stock_code, na=False)]
    if date_str and date_str.strip():
        df = df[df['分析时间'].str.contains(date_str, na=False)]
    if market and market.strip():
        df = df[df['市场'] == market]

    return {
        'success': True,
        'result': df.to_markdown(index=True),
    }
