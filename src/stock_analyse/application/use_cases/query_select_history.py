from __future__ import annotations

import os

import pandas as pd


def execute(select_path: str, strategy_name: str, market: str, date_str: str) -> dict:
    if not os.path.exists(select_path):
        raise FileNotFoundError(f'目录不存在: {select_path}')
    if not os.path.isdir(select_path):
        raise NotADirectoryError(f'{select_path} 不是一个目录')

    results = []
    for item in os.listdir(select_path):
        item_path = os.path.join(select_path, item)
        if not os.path.isdir(item_path):
            continue

        parts = item.split('_')
        if len(parts) < 5 or parts[1] != 'analyse':
            continue

        market_info = parts[0]
        strategy_name_info = parts[2]
        time_info = parts[4].rstrip('.txt')
        url = f'/api/history/select?strategy={strategy_name_info}&market={market_info}&date={time_info}'
        results.append(
            {
                '目录名': item,
                'market': market_info,
                '策略名': strategy_name_info,
                '时间': time_info,
                'URL': f'[链接]({url})',
            }
        )

    df = pd.DataFrame(results)
    if strategy_name and strategy_name.strip():
        df = df[df['策略名'].str.contains(strategy_name, na=False)]
    if market and market.strip():
        df = df[df['market'] == market]
    if date_str and date_str.strip():
        df = df[df['时间'].str.contains(date_str, na=False)]

    return {
        'success': True,
        'result': df.to_markdown(index=True),
    }
