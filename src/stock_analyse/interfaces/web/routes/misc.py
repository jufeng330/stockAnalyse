from __future__ import annotations

import json
from pathlib import Path

from flask import current_app, jsonify, redirect, render_template, request, send_file

from stock_analyse.domain.strategies.selection_strategy_service import STRATEGY_NAMES


PROJECT_ROOT = Path(__file__).resolve().parents[5]
UI_DOC_ROOT = PROJECT_ROOT / 'doc' / 'ui'
CONFIG_PATH = PROJECT_ROOT / 'config.json'


def _trading_decision_service():
    return current_app.extensions['stock_analyse.context'].trading_decision_service


def _mask_secret(value: str) -> str:
    if not value:
        return ''
    if len(value) <= 8:
        return '*' * len(value)
    return f"{value[:4]}{'*' * (len(value) - 8)}{value[-4:]}"


def _load_masked_config() -> dict:
    with CONFIG_PATH.open('r', encoding='utf-8') as file:
        config = json.load(file)

    ai_config = dict(config.get('ai', {}))
    if 'api_key' in ai_config:
        ai_config['api_key'] = _mask_secret(ai_config['api_key'])

    business_config = {key: value for key, value in config.items() if key != 'ai'}
    web_auth = business_config.get('web_auth')
    if isinstance(web_auth, dict) and 'password' in web_auth:
        masked_web_auth = dict(web_auth)
        masked_web_auth['password'] = _mask_secret(masked_web_auth['password'])
        business_config['web_auth'] = masked_web_auth

    return {'ai': ai_config, 'business': business_config}


def _get_stock_selection_strategies() -> list[dict[str, str]]:
    return [
        {'value': str(strategy_id), 'label': strategy_name}
        for strategy_id, strategy_name in sorted(STRATEGY_NAMES.items())
    ]


def register_misc_routes(app):
    @app.route('/', methods=['GET', 'POST'])
    def home():
        return render_template('stock.html')



    # /holding-stocks 已迁移到 trading_decision.py，由真实模板和持仓 API 共同承接。


    @app.route('/holding-records', methods=['GET'])
    def holding_records():
        holding_stock_id = (request.args.get('holding_stock_id') or '').strip()
        if not holding_stock_id:
            return redirect(_trading_decision_service().build_default_holding_records_url())
        try:
            page_data = _trading_decision_service().build_holding_records_page_data(holding_stock_id)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return jsonify({'success': False, 'message': message, 'error': {'code': code, 'message': message}}), status_code
        return render_template('holding_records.html', **page_data)


    @app.route('/stock-analysis-record', methods=['GET'])
    def stock_analysis_record():
        watch_stock_id = (request.args.get('watch_stock_id') or '').strip()
        record_id = (request.args.get('record_id') or '').strip() or None
        if not watch_stock_id:
            return render_template('stock_analysis_record.html')
        try:
            page_data = _trading_decision_service().build_focus_stock_analysis_record_page_data(watch_stock_id, record_id)
        except ValueError as exc:
            if not record_id:
                return render_template('stock_analysis_record.html')
            return jsonify({'success': False, 'message': str(exc), 'error': {'code': 'not_found', 'message': str(exc)}}), 404
        return render_template('stock_analysis_record.html', **page_data)

    # /trade-plan-analysis 已迁移到 trading_decision.py，由真实页面和 API 共同承接。

    @app.route('/portfolio-review', methods=['GET'])
    def portfolio_review():
        return send_file(UI_DOC_ROOT / 'portfolio_review_page.html')


    # /holding-review 已迁移到 trading_decision.py，由真实页面和 API 共同承接。


    @app.route('/holding-status-refresh', methods=['GET'])
    def holding_status_refresh():
        return send_file(UI_DOC_ROOT / 'holding_status_refresh_page.html')


    @app.route('/holding-reanalysis', methods=['GET'])
    def holding_reanalysis():
        holding_stock_id = (request.args.get('holding_stock_id') or '').strip()
        record_id = (request.args.get('record_id') or '').strip() or None
        if not holding_stock_id:
            return jsonify({'success': False, 'message': '缺少 holding_stock_id', 'error': {'code': 'bad_request', 'message': '缺少 holding_stock_id'}}), 400
        try:
            page_data = _trading_decision_service().build_focus_holding_reanalysis_page_context(holding_stock_id, record_id)
        except ValueError as exc:
            message = str(exc)
            code = 'not_found' if '不存在' in message else 'bad_request'
            status_code = 404 if code == 'not_found' else 400
            return jsonify({'success': False, 'message': message, 'error': {'code': code, 'message': message}}), status_code
        return render_template('stock_analysis_record.html', **page_data)


    # /position-decision 已迁移到 trading_decision.py，由真实页面和 API 共同承接。
    @app.route('/add-position-decision', methods=['GET'])
    def add_position_decision():
        return send_file(UI_DOC_ROOT / 'add_position_decision_page.html')


    @app.route('/reduce-position-decision', methods=['GET'])
    def reduce_position_decision():
        return send_file(UI_DOC_ROOT / 'reduce_position_decision_page.html')


    @app.route('/sell-decision', methods=['GET'])
    def sell_decision():
        return send_file(UI_DOC_ROOT / 'sell_decision_page.html')


    @app.route('/weekly-holding-review', methods=['GET'])
    def weekly_holding_review():
        return send_file(UI_DOC_ROOT / 'weekly_holding_review_page.html')


    @app.route('/monthly-holding-review', methods=['GET'])
    def monthly_holding_review():
        return send_file(UI_DOC_ROOT / 'monthly_holding_review_page.html')


    @app.route('/quarterly-holding-review', methods=['GET'])
    def quarterly_holding_review():
        return send_file(UI_DOC_ROOT / 'quarterly_holding_review_page.html')


    @app.route('/stock-screener', methods=['GET'])
    def stock_screener():
        return render_template(
            'stock_screener.html',
            strategies=_get_stock_selection_strategies(),
            default_market='SH',
            default_strategy='1',
        )


    @app.route('/batch-analysis', methods=['GET'])
    def batch_analysis():
        return render_template('batch_analysis.html')


    @app.route('/single-stock-analysis', methods=['GET'])
    def single_stock_analysis():
        return render_template('single_stock_analysis.html')


    @app.route('/ai-config', methods=['GET'])
    def ai_config():
        return send_file(UI_DOC_ROOT / 'ai_config_page.html')


    @app.route('/business-config', methods=['GET'])
    def business_config():
        return send_file(UI_DOC_ROOT / 'business_config_page.html')


    @app.route('/api/config/ai', methods=['GET'])
    def ai_config_api():
        return jsonify(_load_masked_config()['ai'])


    @app.route('/api/config/business', methods=['GET'])
    def business_config_api():
        return jsonify(_load_masked_config()['business'])


    @app.route('/stock_ai', methods=['GET'])
    def stock_ai():
        return render_template('stock_ai.html')


    @app.route('/stockSelector', methods=['GET', 'POST'])
    def stock_selector():
        return render_template('stock.html')


    @app.route('/trading-decision-ui', methods=['GET'])
    def trading_decision_ui_index():
        return send_file(UI_DOC_ROOT / 'README.md')
