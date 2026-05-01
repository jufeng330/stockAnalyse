from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TradePlanPositionSuggestion(BaseModel):
    model_config = ConfigDict(extra='forbid')

    target_position: str = ''
    position_limit: str = ''
    add_condition: str = ''
    reduce_condition: str = ''
    stop_loss_reference: str = ''


class TradePlanDecision(BaseModel):
    model_config = ConfigDict(extra='forbid')

    action: Literal['buy', 'hold', 'watch', 'sell']
    summary: str = Field(min_length=1)
    logic: str = Field(min_length=1)
    risk_level: Literal['low', 'medium', 'high']
    risks: list[str] = Field(default_factory=list)
    time_horizon: str = ''
    position_suggestion: TradePlanPositionSuggestion = Field(default_factory=TradePlanPositionSuggestion)

    @field_validator('summary', 'logic')
    @classmethod
    def validate_non_empty_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError('字段不能为空')
        return normalized

    @field_validator('risks')
    @classmethod
    def validate_risks(cls, value: list[str]) -> list[str]:
        return [str(item).strip() for item in value if str(item).strip()]


class TradePlanMetadata(BaseModel):
    model_config = ConfigDict(extra='forbid')

    template_name: str = Field(min_length=1)
    data_source: Literal['cache_first', 'partial_cache_fallback', 'fallback_only']
    cache_hits: list[str] = Field(default_factory=list)

    @field_validator('template_name')
    @classmethod
    def validate_template_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError('template_name 不能为空')
        return normalized

    @field_validator('cache_hits')
    @classmethod
    def validate_cache_hits(cls, value: list[str]) -> list[str]:
        return [str(item).strip() for item in value if str(item).strip()]


class TradePlanAnalysisOutput(BaseModel):
    model_config = ConfigDict(extra='forbid')

    trade_plan_markdown: str = ''
    decision: TradePlanDecision
    plan_metadata: TradePlanMetadata


class TradePlanAnalysisInput(BaseModel):
    model_config = ConfigDict(extra='allow')

    template_markdown: str = ''
    watch_stock: dict[str, Any] = Field(default_factory=dict)
    request: dict[str, Any] = Field(default_factory=dict)
    cache_context: dict[str, Any] = Field(default_factory=dict)
    fallback_context: dict[str, Any] = Field(default_factory=dict)
    data_source: str = 'fallback_only'

    def to_prompt_payload(self) -> dict[str, Any]:
        return self.model_dump(mode='json')
