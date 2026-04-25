from __future__ import annotations

import os


def execute(select_path: str, strategy_name: str, date_str: str, market: str) -> tuple[str, str, str]:
    if not os.path.exists(select_path):
        raise FileNotFoundError(f'目录不存在: {select_path}')
    if not os.path.isdir(select_path):
        raise NotADirectoryError(f'{select_path} 不是一个目录')

    full_dir_path = ''
    for root, dirs, _files in os.walk(select_path):
        for dir_name in dirs:
            if strategy_name in dir_name and date_str in dir_name and market in dir_name and 'analyse' in dir_name:
                full_dir_path = os.path.join(root, dir_name)

    report_high_score = ''
    report_all = ''
    report_summary = ''

    if full_dir_path and os.path.exists(full_dir_path):
        file_high_score = os.path.join(full_dir_path, 'results_high_score.txt')
        file_all = os.path.join(full_dir_path, 'results_all.txt')
        file_summary = os.path.join(full_dir_path, 'summary.txt')

        if os.path.isfile(file_high_score):
            with open(file_high_score, 'r', encoding='utf-8') as file:
                report_high_score = file.read()
        if os.path.isfile(file_all):
            with open(file_all, 'r', encoding='utf-8') as file:
                report_all = file.read()
        if os.path.isfile(file_summary):
            with open(file_summary, 'r', encoding='utf-8') as file:
                report_summary = file.read()

    return report_high_score, report_all, report_summary
