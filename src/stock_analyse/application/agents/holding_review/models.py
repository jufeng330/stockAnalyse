from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class HoldingReviewTab(BaseModel):
    model_config = ConfigDict(extra='forbid')

    id: Literal['execution_review', 'result_review', 'discipline_review', 'next_action']
    title: Literal['执行与卖出复盘', '结果复盘', '方法与纪律', '后续动作']
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


class HoldingReviewOutput(BaseModel):
    model_config = ConfigDict(extra='forbid')

    performance_summary: str = Field(min_length=1)
    execution_summary: str = Field(min_length=1)
    risk_summary: str = Field(min_length=1)
    discipline_summary: str = Field(min_length=1)
    next_action_summary: str = Field(min_length=1)
    conclusion_tag: Literal['logic_ok', 'need_recheck', 'execution_issue', 'risk_rising', 'prepare_reduce', 'prepare_sell']
    tabs: list[HoldingReviewTab] = Field(min_length=4, max_length=4)

    @field_validator('performance_summary', 'execution_summary', 'risk_summary', 'discipline_summary', 'next_action_summary')
    @classmethod
    def validate_text_fields(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError('字段不能为空')
        return normalized

    @field_validator('tabs')
    @classmethod
    def validate_tabs(cls, value: list[HoldingReviewTab]) -> list[HoldingReviewTab]:
        expected = [
            ('execution_review', '执行与卖出复盘'),
            ('result_review', '结果复盘'),
            ('discipline_review', '方法与纪律'),
            ('next_action', '后续动作'),
        ]
        if len(value) != len(expected):
            raise ValueError('tabs 数量必须为 4')
        for index, (expected_id, expected_title) in enumerate(expected):
            tab = value[index]
            if tab.id != expected_id or tab.title != expected_title:
                raise ValueError(f'第 {index + 1} 个 tab 必须为 {expected_title}')
        return value


class HoldingReviewInput(BaseModel):
    model_config = ConfigDict(extra='allow')

    holding_stock: dict[str, Any] = Field(default_factory=dict)
    watch_stock: dict[str, Any] = Field(default_factory=dict)
    request: dict[str, Any] = Field(default_factory=dict)
    trade_history_context: dict[str, Any] = Field(default_factory=dict)
    entry_context: dict[str, Any] = Field(default_factory=dict)
    reanalysis_context: dict[str, Any] = Field(default_factory=dict)
    position_decision_context: dict[str, Any] = Field(default_factory=dict)
    financial_context: dict[str, Any] = Field(default_factory=dict)
    market_context: dict[str, Any] = Field(default_factory=dict)
    review_focus_context: dict[str, Any] = Field(default_factory=dict)
    data_source: str = 'holding_snapshot'

    def to_prompt_payload(self) -> dict[str, Any]:
        return self.model_dump(mode='json')
