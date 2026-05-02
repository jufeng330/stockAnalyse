from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class EntryDecisionState:
    """进场决策会话状态。

    用于进场优化的暂停/继续执行场景，承载会话级上下文、阶段产物、缺失字段与最终结果。
    """

    session_id: str
    watch_stock_id: str
    request: dict[str, Any]
    watch_stock: dict[str, Any] = field(default_factory=dict)
    auto_context: dict[str, Any] = field(default_factory=dict)
    manual_inputs: dict[str, Any] = field(default_factory=dict)
    role_outputs: dict[str, Any] = field(default_factory=dict)
    current_role: str = 'macro_analysis'
    status: str = 'running'
    missing_fields: list[str] = field(default_factory=list)
    pause_prompt: str = ''
    final_result: dict[str, Any] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.meta.setdefault('started_at', datetime.now().isoformat())
        self.meta.setdefault('updated_at', datetime.now().isoformat())
        self.meta.setdefault('completed_roles', [])
        self.meta.setdefault('errors', [])
        self.meta.setdefault('timeline', [])

    def mark_role_completed(self, role_name: str) -> None:
        completed_roles = self.meta.setdefault('completed_roles', [])
        if role_name not in completed_roles:
            completed_roles.append(role_name)
        self.current_role = role_name
        self.meta['updated_at'] = datetime.now().isoformat()

    def add_timeline(self, role_name: str, message: str, status: str = 'completed') -> None:
        self.meta.setdefault('timeline', []).append(
            {
                'role': role_name,
                'message': message,
                'status': status,
                'timestamp': datetime.now().isoformat(),
            }
        )
        self.meta['updated_at'] = datetime.now().isoformat()

    def add_error(self, role_name: str, error: str) -> None:
        self.meta.setdefault('errors', []).append(
            {
                'role': role_name,
                'error': error,
                'timestamp': datetime.now().isoformat(),
            }
        )
        self.meta['updated_at'] = datetime.now().isoformat()

    @property
    def completed_roles(self) -> list[str]:
        return self.meta.setdefault('completed_roles', [])

    @property
    def timeline(self) -> list[dict[str, Any]]:
        return self.meta.setdefault('timeline', [])

    @property
    def errors(self) -> list[dict[str, Any]]:
        return self.meta.setdefault('errors', [])

    def to_dict(self) -> dict[str, Any]:
        return {
            'session_id': self.session_id,
            'watch_stock_id': self.watch_stock_id,
            'request': self.request,
            'watch_stock': self.watch_stock,
            'auto_context': self.auto_context,
            'manual_inputs': self.manual_inputs,
            'role_outputs': self.role_outputs,
            'current_role': self.current_role,
            'status': self.status,
            'missing_fields': self.missing_fields,
            'pause_prompt': self.pause_prompt,
            'final_result': self.final_result,
            'meta': self.meta,
        }
