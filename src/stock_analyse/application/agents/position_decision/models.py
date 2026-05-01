from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PositionDecisionTab(BaseModel):
    """买卖决策页面 tab 结构。

    用于约束持仓买卖决策页面的五个固定标签页，保证前端和历史记录都能按固定顺序展示。
    """

    model_config = ConfigDict(extra='forbid')

    id: Literal['trigger', 'reason', 'execution', 'risk', 'conclusion']
    title: Literal['触发条件', '核心理由', '执行注意事项', '风险分析', '结论']
    summary: str = Field(min_length=1)
    evidence: list[str] = Field(min_length=1)

    @field_validator('summary')
    @classmethod
    def validate_summary(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError('summary 不能为空')
        return normalized

    @field_validator('evidence')
    @classmethod
    def validate_evidence(cls, value: list[str]) -> list[str]:
        normalized = [item.strip() for item in value if str(item).strip()]
        if not normalized:
            raise ValueError('evidence 不能为空')
        return normalized


class PositionDecisionOutput(BaseModel):
    """买卖决策标准输出。

    用于承载推荐动作、状态、结论摘要与五个固定 tab，是持仓买卖决策链路的主输出协议。
    """

    model_config = ConfigDict(extra='forbid')

    recommended_action: Literal['buy', 'reduce', 'sell', 'watch']
    decision_status: Literal['buy_candidate', 'reduce_candidate', 'sell_candidate', 'observe']
    confidence: Literal['high', 'medium', 'low']
    conclusion_summary: str = Field(min_length=1)
    tabs: list[PositionDecisionTab] = Field(min_length=5, max_length=5)

    @field_validator('conclusion_summary')
    @classmethod
    def validate_conclusion_summary(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError('conclusion_summary 不能为空')
        return normalized

    @field_validator('tabs')
    @classmethod
    def validate_tabs(cls, value: list[PositionDecisionTab]) -> list[PositionDecisionTab]:
        expected = [
            ('trigger', '触发条件'),
            ('reason', '核心理由'),
            ('execution', '执行注意事项'),
            ('risk', '风险分析'),
            ('conclusion', '结论'),
        ]
        if len(value) != len(expected):
            raise ValueError('tabs 数量必须为 5')
        for index, (expected_id, expected_title) in enumerate(expected):
            tab = value[index]
            if tab.id != expected_id or tab.title != expected_title:
                raise ValueError(f'第 {index + 1} 个 tab 必须为 {expected_title}')
        return value


class PositionDecisionInput(BaseModel):
    """买卖决策输入。

    用于承载持仓、财务、交易历史与持仓计划等上下文，供持仓股票的减仓/卖出判断单次调用使用。
    """

    model_config = ConfigDict(extra='allow')

    holding_stock: dict[str, Any] = Field(default_factory=dict)
    watch_stock: dict[str, Any] = Field(default_factory=dict)
    request: dict[str, Any] = Field(default_factory=dict)
    financial_context: dict[str, Any] = Field(default_factory=dict)
    trade_history_context: dict[str, Any] = Field(default_factory=dict)
    holding_plan_context: dict[str, Any] = Field(default_factory=dict)
    supporting_context: dict[str, Any] = Field(default_factory=dict)
    data_source: str = 'holding_snapshot'

    def to_prompt_payload(self) -> dict[str, Any]:
        return self.model_dump(mode='json')
