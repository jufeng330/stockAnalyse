from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


EntryDecisionRoleName = Literal[
    'macro_analysis',
    'asset_classification',
    'value_stage_analysis',
    'price_zone_analysis',
    'buy_plan_analysis',
    'risk_control_analysis',
]


def _json_safe(value: Any) -> Any:
    if hasattr(value, 'to_dict'):
        try:
            if hasattr(value, 'head'):
                return value.head(20).to_dict('records')
            return value.to_dict()
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return value


class _JsonSafeBaseModel(BaseModel):
    model_config = ConfigDict(extra='allow')

    def to_prompt_payload(self) -> dict[str, Any]:
        return _json_safe(self.model_dump())


class EntryDecisionInput(_JsonSafeBaseModel):
    """进场决策单角色执行输入。

    用于进场优化会话中某一角色执行时承载关注股票、自动上下文、人工补充信息与已完成阶段输出。
    """

    session_id: str
    watch_stock: dict[str, Any] = Field(default_factory=dict)
    request: dict[str, Any] = Field(default_factory=dict)
    auto_context: dict[str, Any] = Field(default_factory=dict)
    manual_inputs: dict[str, Any] = Field(default_factory=dict)
    completed_role_outputs: dict[str, Any] = Field(default_factory=dict)
    derived_inputs: dict[str, Any] = Field(default_factory=dict)
    target_role: EntryDecisionRoleName


class EntryDecisionRoleOutputMap(BaseModel):
    """进场决策单角色结构化输出容器。

    用于兼容不同角色返回字段差异，作为进场优化多阶段 agent 的宽松结构化协议。
    """

    model_config = ConfigDict(extra='allow')


class EntryDecisionSummaryInput(_JsonSafeBaseModel):
    """进场决策摘要 markdown 生成输入。

    用于六阶段角色完成后汇总模板、上下文与角色输出，生成页面主渲染所需的 markdown 内容。
    """

    template_markdown: str = ''
    watch_stock: dict[str, Any] = Field(default_factory=dict)
    auto_context: dict[str, Any] = Field(default_factory=dict)
    manual_inputs: dict[str, Any] = Field(default_factory=dict)
    role_outputs: dict[str, Any] = Field(default_factory=dict)
