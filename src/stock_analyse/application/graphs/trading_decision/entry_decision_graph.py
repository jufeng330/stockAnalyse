from __future__ import annotations

from typing import Any

from stock_analyse.application.agents.entry_decision import EntryDecisionAgent
from stock_analyse.application.agents.entry_decision.models import EntryDecisionInput, EntryDecisionSummaryInput


class FocusEntryDecisionGraph:
    """Focus 进场决策 graph 封装。

    用于关注股票进场优化会话执行时把状态对象转换成结构化 agent 输入，并分别驱动单角色分析与最终摘要生成。
    """

    def __init__(self, *, agent: EntryDecisionAgent | None = None) -> None:
        self.agent = agent or EntryDecisionAgent()

    def run_role(
        self,
        *,
        role_name: str,
        state: Any,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> dict[str, Any]:
        agent_input = EntryDecisionInput.model_validate(
            {
                'session_id': state.session_id,
                'watch_stock': state.watch_stock,
                'request': state.request,
                'auto_context': state.auto_context,
                'manual_inputs': state.manual_inputs,
                'completed_role_outputs': state.role_outputs,
                'derived_inputs': (state.auto_context or {}).get('derived_inputs') or {},
                'target_role': role_name,
            }
        )
        return self.agent.run_role(
            data=agent_input,
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
            system_prompt=system_prompt,
        )

    def build_summary_markdown(
        self,
        *,
        template_markdown: str,
        state: Any,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> str:
        summary_input = EntryDecisionSummaryInput.model_validate(
            {
                'template_markdown': template_markdown,
                'watch_stock': state.watch_stock,
                'auto_context': state.auto_context,
                'manual_inputs': state.manual_inputs,
                'role_outputs': state.role_outputs,
            }
        )
        return self.agent.build_summary_markdown(
            data=summary_input,
            llm_provider=llm_provider,
            llm_model=llm_model,
            api_code=api_code,
            system_prompt=system_prompt,
        )


class EntryDecisionGraph(FocusEntryDecisionGraph):
    """兼容保留的旧进场决策 graph 名称。"""

    pass


def run_focus_entry_decision_role_graph(**kwargs) -> dict[str, Any]:
    return FocusEntryDecisionGraph().run_role(**kwargs)


def run_focus_entry_decision_summary_graph(**kwargs) -> str:
    return FocusEntryDecisionGraph().build_summary_markdown(**kwargs)


def run_entry_decision_role_graph(**kwargs) -> dict[str, Any]:
    return run_focus_entry_decision_role_graph(**kwargs)


def run_entry_decision_summary_graph(**kwargs) -> str:
    return run_focus_entry_decision_summary_graph(**kwargs)
