from __future__ import annotations

from flask import current_app, render_template, request



def _context():
    return current_app.extensions['stock_analyse.context']



def register_history_routes(app):
    @app.route('/history', methods=['GET', 'POST'])
    def history():
        return render_template('history.html')

    @app.route('/api/history/analyse', methods=['GET', 'POST'])
    def history_analyse():
        context = _context()
        stock_code = request.args.get('stock', default='105.AMZN', type=str)
        date_str = request.args.get('date', default='20250717', type=str)
        report_technical, report_financial, report_technical_request = context.analyzer.find_history_stock_analysis(stock_code, date_str)
        return render_template(
            'history_analyse.html',
            stock_summary=f'###股票代码：{stock_code}\n ###日期：{date_str}',
            fundamental_analysis=report_technical,
            annual_report_analysis=report_financial,
            sentiment_analysis=report_technical_request,
        )

    @app.route('/api/history/select', methods=['GET', 'POST'])
    def history_selector():
        context = _context()
        strategy_name = request.args.get('strategy', default='知名股票筛选策略', type=str)
        date_str = request.args.get('date', default='2025081414', type=str)
        market = request.args.get('market', default='H', type=str)
        report_high_score, report_all, report_summary = context.analyzer.find_history_strategy_analysis(strategy_name, date_str, market)
        return render_template(
            'history_analyse.html',
            stock_summary=f'###股票代码：{strategy_name}\n ###日期：{date_str}',
            fundamental_analysis=report_high_score,
            annual_report_analysis=report_all,
            sentiment_analysis=report_summary,
        )
