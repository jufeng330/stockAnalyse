from __future__ import annotations

from datetime import datetime

from stock_analyse.application.orchestrators.stock_selection_orchestrator import StockSelectionOrchestrator
from stock_analyse.application.use_cases import analyze_technical_indicators as analyze_technical_indicators_use_case


def _analyze_technical_summary(market: str, symbol: str) -> tuple[dict | None, dict | None]:
    result = analyze_technical_indicators_use_case.execute(action='all', market=market, symbol=symbol)
    if not result.get('success'):
        return None, {'success': False, 'data': {}, 'message': result.get('message', '无法获取历史数据')}
    return result.get('data', {}), None


def _signal_items(technical_data: dict) -> list[dict]:
    indicators = technical_data.get('indicators', {})
    signal_items = []
    for name, result in indicators.items():
        signal = result.get('signal')
        if signal not in {'buy', 'oversold'}:
            continue
        indicator_values = result.get('indicator_values', {})
        value = next(iter(indicator_values.values()), result.get('last_price'))
        signal_items.append({'indicator': name.upper(), 'signal': signal, 'value': value})
    return signal_items


def _suggestions(score: int, signal_count: int) -> list[str]:
    suggestions = []
    if score >= 50:
        suggestions.append('技术指标强劲，建议积极关注')
    elif score >= 30:
        suggestions.append('技术指标向好，可考虑分批建仓')
    elif score >= 10:
        suggestions.append('技术指标中性，建议观望')
    else:
        suggestions.append('技术指标偏弱，建议谨慎')

    if signal_count >= 3:
        suggestions.append(f'多个指标发出买入信号({signal_count}个)，值得关注')
    elif signal_count >= 1:
        suggestions.append(f'部分指标显示买入机会({signal_count}个)')
    return suggestions


def calculate_score(market: str, symbol: str) -> dict:
    try:
        technical_data, error = _analyze_technical_summary(market, symbol)
        if error:
            return error

        summary = technical_data.get('summary', {})
        signals = _signal_items(technical_data)
        score = summary.get('score', 0)
        recommendation = summary.get('recommendation', '建议观望')

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
        technical_data, error = _analyze_technical_summary(market, symbol)
        if error:
            return error

        signals = _signal_items(technical_data)
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

        signals = score_result['data'].get('signals', [])
        data = score_result['data']
        data['suggestions'] = _suggestions(data['score'], len(signals))
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
