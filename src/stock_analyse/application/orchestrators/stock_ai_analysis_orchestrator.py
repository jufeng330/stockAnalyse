from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any

from stock_analyse.application.agents.analysts.fundamentals_analyst import FundamentalsAnalyst
from stock_analyse.application.agents.analysts.market_analyst import MarketAnalyst
from stock_analyse.application.agents.analysts.news_analyst import NewsAnalyst
from stock_analyse.application.agents.managers.research_manager import ResearchManager
from stock_analyse.application.agents.managers.risk_manager import RiskManager
from stock_analyse.application.agents.researchers.bear_researcher import BearResearcher
from stock_analyse.application.agents.researchers.bull_researcher import BullResearcher
from stock_analyse.application.agents.trader.trader_agent import TraderAgent
from stock_analyse.application.dto.stock_ai_analysis_state import StockAIAnalysisState
from stock_analyse.application.services.ai_stock_data_facade import AIStockDataFacade


class StockAIAnalysisOrchestrator:
    def __init__(self, data_facade: AIStockDataFacade | None = None) -> None:
        self.data_facade = data_facade or AIStockDataFacade()

    def run(
        self,
        *,
        stock_code: str,
        market: str,
        trade_date: str | None = None,
        start_date_str: str | None = None,
        end_date_str: str | None = None,
        analysis_depth: str = 'standard',
        include_technical: bool = True,
        include_sentiment: bool = True,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
        callbacks: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        callbacks = callbacks or {}
        state = StockAIAnalysisState(
            request={
                'stock_code': stock_code,
                'market': market,
                'trade_date': trade_date,
                'start_date_str': start_date_str,
                'end_date_str': end_date_str,
                'analysis_depth': analysis_depth,
                'include_technical': include_technical,
                'include_sentiment': include_sentiment,
                'llm_provider': llm_provider,
                'llm_model': llm_model,
            }
        )
        started_at = time.time()

        send_log = callbacks.get('send_log')
        send_progress = callbacks.get('send_progress')

        def progress(percent: int, message: str) -> None:
            if send_log:
                send_log(message, 'header')
            if send_progress:
                send_progress('singleProgress', percent, message)

        progress(5, f'🚀 开始构建AI分析数据快照: {stock_code}')
        state.stock_snapshot = self.data_facade.build_snapshot(
            stock_code=stock_code,
            market=market,
            trade_date=trade_date,
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            include_technical=include_technical,
            include_sentiment=include_sentiment,
        )
        state.add_stage('snapshot_ready')

        common_kwargs = {
            'ai_platform': llm_provider,
            'ai_model': llm_model,
            'api_code': api_code,
            'system_prompt': system_prompt,
        }

        analyst_context = {
            'stock_code': stock_code,
            'market': market,
            'trade_date': state.stock_snapshot.get('trade_date'),
            'stock_snapshot': state.stock_snapshot,
        }

        analyst_agents = {
            'market': MarketAnalyst(**common_kwargs),
            'fundamentals': FundamentalsAnalyst(**common_kwargs),
            'news': NewsAnalyst(**common_kwargs),
        }
        progress(15, f'🚀 开始执行分析师节点: {stock_code}')
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {name: executor.submit(agent.run, analyst_context) for name, agent in analyst_agents.items()}
            for name, future in futures.items():
                try:
                    state.analyst_outputs[name] = future.result()
                except Exception as exc:
                    state.analyst_outputs[name] = {'summary': '', 'signals': [], 'risks': [str(exc)], 'confidence': 0.0, 'evidence': []}
                    state.add_error(f'analyst:{name}', str(exc))
        state.add_stage('analysts_finished')

        research_context = {
            'stock_code': stock_code,
            'market': market,
            'trade_date': state.stock_snapshot.get('trade_date'),
            'analyst_outputs': state.analyst_outputs,
        }
        progress(35, f'🚀 开始执行多空研究节点: {stock_code}')
        bull = BullResearcher(**common_kwargs).run(research_context)
        bear = BearResearcher(**common_kwargs).run(research_context)
        state.research_outputs = {'bull': bull, 'bear': bear}
        state.add_stage('research_finished')

        manager_context = {
            'stock_code': stock_code,
            'market': market,
            'trade_date': state.stock_snapshot.get('trade_date'),
            'analyst_outputs': state.analyst_outputs,
            'research_outputs': state.research_outputs,
            'stock_snapshot': state.stock_snapshot,
        }
        progress(55, f'🚀 开始执行研究经理与风险经理节点: {stock_code}')
        research_manager = ResearchManager(**common_kwargs).run(manager_context)
        risk_manager = RiskManager(**common_kwargs).run(manager_context)
        state.manager_outputs = {
            'research_manager': research_manager,
            'risk_manager': risk_manager,
        }
        state.add_stage('managers_finished')

        trader_context = {
            'stock_code': stock_code,
            'market': market,
            'trade_date': state.stock_snapshot.get('trade_date'),
            'analyst_outputs': state.analyst_outputs,
            'research_outputs': state.research_outputs,
            'manager_outputs': state.manager_outputs,
            'stock_snapshot': state.stock_snapshot,
        }
        progress(75, f'🚀 开始执行交易员决策节点: {stock_code}')
        trader_output = TraderAgent(**common_kwargs).run(trader_context)
        state.trader_output = trader_output
        state.add_stage('trader_finished')

        decision = self._build_decision(state)
        state.decision = decision
        state.final_state = {
            'analyst_outputs': state.analyst_outputs,
            'research_outputs': state.research_outputs,
            'manager_outputs': state.manager_outputs,
            'trader_output': state.trader_output,
        }
        state.meta['model_info'] = llm_model or ''
        state.meta['finished_at'] = datetime.now().isoformat()
        state.meta['duration_ms'] = int((time.time() - started_at) * 1000)
        state.add_stage('decision_ready')
        progress(95, f'🚀 AI个股分析完成: {stock_code}')
        return state.to_dict()

    def _build_decision(self, state: StockAIAnalysisState) -> dict[str, Any]:
        technical_score = float(state.stock_snapshot.get('technical', {}).get('score', 0) or 0)
        sentiment_score = float(state.stock_snapshot.get('sentiment', {}).get('sentiment_score', 0) or 0)
        bull_conf = float(state.research_outputs.get('bull', {}).get('confidence', 0.5) or 0.5)
        bear_conf = float(state.research_outputs.get('bear', {}).get('confidence', 0.5) or 0.5)
        trader_conf = float(state.trader_output.get('confidence', 0.5) or 0.5)

        composite = round((technical_score + sentiment_score + bull_conf * 100 + (1 - bear_conf) * 100 + trader_conf * 100) / 5, 2)
        trader_summary = state.trader_output.get('summary', '')
        structured_summary = trader_summary if isinstance(trader_summary, dict) else {}
        summary_text = self._decision_summary_text(trader_summary)

        action = str(structured_summary.get('action', '') or '').strip().lower()
        if not action:
            action = 'watch'
            if composite >= 75:
                action = 'buy'
            elif composite >= 60:
                action = 'hold'
            elif composite < 40:
                action = 'sell'

        risk_level = 'medium'
        risk_summary_text = self._decision_summary_text(state.manager_outputs.get('risk_manager', {}).get('summary', ''))
        lowered = risk_summary_text.lower()
        if '高' in risk_summary_text or 'high' in lowered:
            risk_level = 'high'
        elif '低' in risk_summary_text or 'low' in lowered:
            risk_level = 'low'

        return {
            'action': action,
            'stance': str(structured_summary.get('stance', '') or '').strip(),
            'confidence': round(trader_conf, 2),
            'risk_level': risk_level,
            'summary': summary_text,
            'logic': str(structured_summary.get('logic', '') or '').strip(),
            'position_suggestion': structured_summary.get('position_suggestion') or self._position_suggestion(action, risk_level),
            'time_horizon': str(structured_summary.get('time_horizon', '3-10 trading days') or '3-10 trading days').strip(),
            'signals': state.trader_output.get('signals', []),
            'risks': state.trader_output.get('risks', []),
            'evidence': state.trader_output.get('evidence', []),
            'scores': {
                'technical': technical_score,
                'sentiment': sentiment_score,
                'composite': composite,
            },
        }

    def _decision_summary_text(self, summary: Any) -> str:
        if isinstance(summary, str):
            return summary
        if isinstance(summary, dict):
            for key in ('logic', 'summary', 'stance', 'action'):
                value = str(summary.get(key, '') or '').strip()
                if value:
                    return value
        return ''

    def _position_suggestion(self, action: str, risk_level: str) -> dict[str, str]:
        if action == 'buy':
            return {
                'target_position': '20%-30%' if risk_level == 'high' else '30%-50%',
                'add_condition': '放量突破关键压力位后分批加仓',
                'reduce_condition': '冲高放量不突破或跌回突破位下方时减仓',
                'stop_loss_reference': '跌破最近关键支撑位时止损',
            }
        if action == 'hold':
            return {
                'target_position': '10%-30%',
                'add_condition': '站稳右侧确认位后小幅加仓',
                'reduce_condition': '跌破观察位或风险事件兑现时降仓',
                'stop_loss_reference': '跌破区间下沿时止损',
            }
        if action == 'sell':
            return {
                'target_position': '0%-10%',
                'add_condition': '暂不主动加仓',
                'reduce_condition': '反弹承压时继续降低仓位',
                'stop_loss_reference': '若已持有，跌破弱势支撑位离场',
            }
        return {
            'target_position': '0%-20%',
            'add_condition': '等待明确信号再加仓',
            'reduce_condition': '信号走弱时继续观望或减仓',
            'stop_loss_reference': '跌破观察区下沿时止损',
        }

