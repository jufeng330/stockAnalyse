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


class EntryDecisionInput(BaseModel):
    model_config = ConfigDict(extra='allow')

    session_id: str
    watch_stock: dict[str, Any] = Field(default_factory=dict)
    request: dict[str, Any] = Field(default_factory=dict)
    auto_context: dict[str, Any] = Field(default_factory=dict)
    manual_inputs: dict[str, Any] = Field(default_factory=dict)
    completed_role_outputs: dict[str, Any] = Field(default_factory=dict)
    derived_inputs: dict[str, Any] = Field(default_factory=dict)
    target_role: EntryDecisionRoleName

    def to_prompt_payload(self) -> dict[str, Any]:
        return self.model_dump(mode='json')


class EntryDecisionRoleOutputMap(BaseModel):
    model_config = ConfigDict(extra='allow')


class EntryDecisionSummaryInput(BaseModel):
    model_config = ConfigDict(extra='allow')

    template_markdown: str = ''
    watch_stock: dict[str, Any] = Field(default_factory=dict)
    auto_context: dict[str, Any] = Field(default_factory=dict)
    manual_inputs: dict[str, Any] = Field(default_factory=dict)
    role_outputs: dict[str, Any] = Field(default_factory=dict)

    def to_prompt_payload(self) -> dict[str, Any]:
        return self.model_dump(mode='json')
