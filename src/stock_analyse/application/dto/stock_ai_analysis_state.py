from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class StockAIAnalysisState:
    """股票分析共享运行状态。

    用于普通股票分析与持仓二次分析流程，承载快照、多角色输出、最终决策与阶段元信息。
    """

    request: dict[str, Any]
    stock_snapshot: dict[str, Any] = field(default_factory=dict)
    analyst_outputs: dict[str, Any] = field(default_factory=dict)
    research_outputs: dict[str, Any] = field(default_factory=dict)
    manager_outputs: dict[str, Any] = field(default_factory=dict)
    trader_output: dict[str, Any] = field(default_factory=dict)
    final_state: dict[str, Any] = field(default_factory=dict)
    decision: dict[str, Any] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.meta.setdefault('started_at', datetime.now().isoformat())
        self.meta.setdefault('errors', [])
        self.meta.setdefault('stages', [])
        self.meta.setdefault('durations', {})

    def add_error(self, stage: str, error: str) -> None:
        self.meta.setdefault('errors', []).append({'stage': stage, 'error': error})

    def add_stage(self, stage: str, detail: str | None = None) -> None:
        item = {'stage': stage, 'timestamp': datetime.now().isoformat()}
        if detail:
            item['detail'] = detail
        self.meta.setdefault('stages', []).append(item)

    def to_dict(self) -> dict[str, Any]:
        return {
            'request': self.request,
            'stock_snapshot': self.stock_snapshot,
            'analyst_outputs': self.analyst_outputs,
            'research_outputs': self.research_outputs,
            'manager_outputs': self.manager_outputs,
            'trader_output': self.trader_output,
            'final_state': self.final_state,
            'decision': self.decision,
            'meta': self.meta,
        }
