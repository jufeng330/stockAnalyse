from __future__ import annotations

import os


def execute(analyzer_path: str, stock_code: str, date_str: str) -> tuple[str, str, str]:
    if not os.path.exists(analyzer_path):
        raise FileNotFoundError(f'目录不存在: {analyzer_path}')
    if not os.path.isdir(analyzer_path):
        raise NotADirectoryError(f'{analyzer_path} 不是一个目录')

    report_technical_file = ''
    report_financial_file = ''
    report_technical_request_file = ''

    for filename in os.listdir(analyzer_path):
        if stock_code not in filename:
            continue
        file_path = os.path.join(analyzer_path, filename)
        if not os.path.isfile(file_path):
            continue
        if stock_code in filename and date_str in filename:
            if 'indicator' in filename and 'request' not in filename:
                report_technical_file = file_path
            if 'indicator' in filename and 'request' in filename:
                report_technical_request_file = file_path
            if 'report' in filename:
                report_financial_file = file_path

    report_technical = ''
    report_financial = ''
    report_technical_request = ''

    if report_technical_file and os.path.isfile(report_technical_file):
        with open(report_technical_file, 'r', encoding='utf-8') as file:
            report_technical = file.read()
    if report_financial_file and os.path.isfile(report_financial_file):
        with open(report_financial_file, 'r', encoding='utf-8') as file:
            report_financial = file.read()
    if report_technical_request_file and os.path.isfile(report_technical_request_file):
        with open(report_technical_request_file, 'r', encoding='utf-8') as file:
            report_technical_request = file.read()

    return report_technical, report_financial, report_technical_request
