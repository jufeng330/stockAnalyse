from __future__ import annotations

from datetime import datetime, timedelta

from stock_analyse.application.orchestrators.stock_selection_orchestrator import StockSelectionOrchestrator
from stocklib.stock_ak_indicator import stockAKIndicator
from stocklib.stock_company import stockCompanyInfo
from stocklib.stock_strategy import StockStrategy



def _get_history_data(market: str, symbol: str):
    stock = stockCompanyInfo(marker=market, symbol=symbol)
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    return stock.get_stock_history_data(start_date_str=start_date, end_date_str=end_date)



def calculate_score(market: str, symbol: str) -> dict:
    try:
        df = _get_history_data(market, symbol)
        if df is None or df.empty:
            return {'success': False, 'data': {}, 'message': '无法获取历史数据'}

        indicator = stockAKIndicator()
        df = indicator.strategy_macd(df)
        df = indicator.strategy_rsi(df)
        df = indicator.strategy_kdj(df)
        df = indicator.strategy_bollinger(df)
        df = indicator.strategy_breakout(df)

        strategy = StockStrategy(market=market)
        score, signals = strategy.calculate_score_indicate(df)
        recommendation = strategy.get_recommendation(score)

        return {
            'success': True,
            'data': {
                'symbol': symbol,
                'market': market,
                'score': score,
                'recommendation': recommendation,
                'signals': signals,
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
            },
            'message': f'评分: {score}, 建议: {recommendation}',
        }
    except Exception as exc:
        return {'success': False, 'data': {}, 'message': f'计算失败: {exc}'}



def get_signals(market: str, symbol: str) -> dict:
    try:
        df = _get_history_data(market, symbol)
        if df is None or df.empty:
            return {'success': False, 'data': {}, 'message': '无法获取历史数据'}

        indicator = stockAKIndicator()
        signals = []

        df_macd = indicator.strategy_macd(df.copy())
        if df_macd is not None and not df_macd.empty:
            latest = df_macd.iloc[-1]
            if latest.get('macd_signal_index') == 1:
                signals.append({'indicator': 'MACD', 'signal': 'buy', 'value': latest.get('macd_dif')})

        df_rsi = indicator.strategy_rsi(df.copy())
        if df_rsi is not None and not df_rsi.empty:
            latest = df_rsi.iloc[-1]
            rsi = latest.get('RSI')
            if rsi < 30:
                signals.append({'indicator': 'RSI', 'signal': 'oversold', 'value': rsi})

        df_kdj = indicator.strategy_kdj(df.copy())
        if df_kdj is not None and not df_kdj.empty:
            latest = df_kdj.iloc[-1]
            if latest.get('kdj_signal') == 1:
                signals.append({'indicator': 'KDJ', 'signal': 'buy', 'value': latest.get('K')})

        df_bb = indicator.strategy_bollinger(df.copy())
        if df_bb is not None and not df_bb.empty:
            latest = df_bb.iloc[-1]
            if latest.get('bb_signal') == 1:
                signals.append({'indicator': 'Bollinger', 'signal': 'buy', 'value': latest.get('收盘')})

        df_break = indicator.strategy_breakout(df.copy())
        if df_break is not None and not df_break.empty:
            latest = df_break.iloc[-1]
            if latest.get('breakout_signal') == 1:
                signals.append({'indicator': 'Breakout', 'signal': 'buy', 'value': latest.get('收盘')})

        return {
            'success': True,
            'data': {
                'symbol': symbol,
                'market': market,
                'signals': signals,
                'signal_count': len(signals),
                'has_buy_signal': len(signals) > 0,
            },
            'message': f'发现 {len(signals)} 个买入信号',
        }
    except Exception as exc:
        return {'success': False, 'data': {}, 'message': f'获取失败: {exc}'}



def get_recommendation(market: str, symbol: str) -> dict:
    try:
        score_result = calculate_score(market, symbol)
        if not score_result['success']:
            return score_result

        signals_result = get_signals(market, symbol)
        data = score_result['data']
        data['signals'] = signals_result.get('data', {}).get('signals', [])

        suggestions = []
        score = data['score']
        if score >= 50:
            suggestions.append('技术指标强劲，建议积极关注')
        elif score >= 30:
            suggestions.append('技术指标向好，可考虑分批建仓')
        elif score >= 10:
            suggestions.append('技术指标中性，建议观望')
        else:
            suggestions.append('技术指标偏弱，建议谨慎')

        signal_count = len(data['signals'])
        if signal_count >= 3:
            suggestions.append(f'多个指标发出买入信号({signal_count}个)，值得关注')
        elif signal_count >= 1:
            suggestions.append(f'部分指标显示买入机会({signal_count}个)')

        data['suggestions'] = suggestions
        return {'success': True, 'data': data, 'message': f"建议: {data['recommendation']}"}
    except Exception as exc:
        return {'success': False, 'data': {}, 'message': f'获取失败: {exc}'}



def batch_analyze(market: str, min_score: int = 30, strategy_type: int = 1, orchestrator: StockSelectionOrchestrator | None = None) -> dict:
    try:
        orchestrator = orchestrator or StockSelectionOrchestrator()
        results = orchestrator.batch_analyze(market=market, min_score=min_score, strategy_type=strategy_type)
        return {
            'success': True,
            'data': {
                'market': market,
                'min_score': min_score,
                'qualified': len(results),
                'top_stocks': results[:5],
            },
            'message': f'分析完成，{len(results)} 只达标',
        }
    except Exception as exc:
        return {'success': False, 'data': {}, 'message': f'分析失败: {exc}'}
