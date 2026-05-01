from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
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

from .models import StockAnalysisInput


class StockAnalysisAgent:
    """股票分析共享执行器。

    用于普通股票分析与持仓二次分析两个场景，负责驱动快照构建、多角色分析、多空研究与最终交易员输出。
    """

    def __init__(self, data_facade: AIStockDataFacade | None = None) -> None:
        self.data_facade = data_facade or AIStockDataFacade()

    def run(self, *, data: StockAnalysisInput, state: StockAIAnalysisState, callbacks: dict[str, Any] | None = None) -> StockAIAnalysisState:
        callbacks = callbacks or {}
        send_log = callbacks.get('send_log')
        send_progress = callbacks.get('send_progress')

        def progress(percent: int, message: str) -> None:
            if send_log:
                send_log(message, 'header')
            if send_progress:
                send_progress('singleProgress', percent, message)

        progress(5, f'🚀 开始构建AI分析数据快照: {data.stock_code}')
        state.stock_snapshot = self.data_facade.build_snapshot(
            stock_code=data.stock_code,
            market=data.market,
            trade_date=data.trade_date,
            start_date_str=data.start_date_str,
            end_date_str=data.end_date_str,
            include_technical=data.include_technical,
            include_sentiment=data.include_sentiment,
        )
        state.add_stage('snapshot_ready')

        common_kwargs = {
            'ai_platform': data.llm_provider,
            'ai_model': data.llm_model,
            'api_code': data.api_code,
            'system_prompt': data.system_prompt,
        }

        analyst_context = {
            'stock_code': data.stock_code,
            'market': data.market,
            'trade_date': state.stock_snapshot.get('trade_date'),
            'stock_snapshot': state.stock_snapshot,
            'analysis_scene': data.analysis_scene,
        }
        analyst_agents = {
            'market': MarketAnalyst(**common_kwargs),
            'fundamentals': FundamentalsAnalyst(**common_kwargs),
            'news': NewsAnalyst(**common_kwargs),
        }
        progress(15, f'🚀 开始执行分析师节点: {data.stock_code}')
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
            'stock_code': data.stock_code,
            'market': data.market,
            'trade_date': state.stock_snapshot.get('trade_date'),
            'analyst_outputs': state.analyst_outputs,
            'analysis_scene': data.analysis_scene,
        }
        progress(35, f'🚀 开始执行多空研究节点: {data.stock_code}')
        bull = BullResearcher(**common_kwargs).run(research_context)
        bear = BearResearcher(**common_kwargs).run(research_context)
        state.research_outputs = {'bull': bull, 'bear': bear}
        state.add_stage('research_finished')

        manager_context = {
            'stock_code': data.stock_code,
            'market': data.market,
            'trade_date': state.stock_snapshot.get('trade_date'),
            'analyst_outputs': state.analyst_outputs,
            'research_outputs': state.research_outputs,
            'stock_snapshot': state.stock_snapshot,
            'analysis_scene': data.analysis_scene,
        }
        progress(55, f'🚀 开始执行研究经理与风险经理节点: {data.stock_code}')
        research_manager = ResearchManager(**common_kwargs).run(manager_context)
        risk_manager = RiskManager(**common_kwargs).run(manager_context)
        state.manager_outputs = {
            'research_manager': research_manager,
            'risk_manager': risk_manager,
        }
        state.add_stage('managers_finished')

        trader_context = {
            'stock_code': data.stock_code,
            'market': data.market,
            'trade_date': state.stock_snapshot.get('trade_date'),
            'analyst_outputs': state.analyst_outputs,
            'research_outputs': state.research_outputs,
            'manager_outputs': state.manager_outputs,
            'stock_snapshot': state.stock_snapshot,
            'analysis_scene': data.analysis_scene,
        }
        progress(75, f'🚀 开始执行交易员决策节点: {data.stock_code}')
        state.trader_output = TraderAgent(**common_kwargs).run(trader_context)
        state.add_stage('trader_finished')
        return state
