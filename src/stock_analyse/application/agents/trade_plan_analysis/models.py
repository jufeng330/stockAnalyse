from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


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


class TradePlanPositionSuggestion(BaseModel):
    """持仓计划分析中的仓位建议结构。

    用于描述计划执行时的目标仓位、加减仓条件与止损参考，供页面摘要卡和历史记录复用。
    """

    model_config = ConfigDict(extra='forbid')

    target_position: str = ''
    position_limit: str = ''
    add_condition: str = ''
    reduce_condition: str = ''
    stop_loss_reference: str = ''


class TradePlanDecision(BaseModel):
    """持仓计划分析的核心决策结构。

    用于约束 action、逻辑、风险等级、时间周期和仓位建议，是持仓计划页面摘要与落库的主协议。
    """

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
    """持仓计划分析的元信息结构。

    用于标记模板名称、缓存命中来源和本次数据来源类型，方便页面展示与排查生成路径。
    """

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
    """持仓计划分析的标准输出。

    用于承载页面主 markdown、决策摘要与元信息，是 trade plan agent 到 orchestrator 的固定协议。
    """

    model_config = ConfigDict(extra='forbid')

    trade_plan_markdown: str = ''
    decision: TradePlanDecision
    plan_metadata: TradePlanMetadata


class TradePlanAnalysisInput(_JsonSafeBaseModel):
    """持仓计划分析输入。

    用于承载模板正文、关注股票、请求参数、缓存命中结果与回退上下文，供单次计划分析调用使用。
    """

    template_markdown: str = ''
    watch_stock: dict[str, Any] = Field(default_factory=dict)
    request: dict[str, Any] = Field(default_factory=dict)
    cache_context: dict[str, Any] = Field(default_factory=dict)
    fallback_context: dict[str, Any] = Field(default_factory=dict)
    data_source: str = 'fallback_only'
