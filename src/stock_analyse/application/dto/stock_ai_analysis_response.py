from __future__ import annotations

from typing import Any


def build_success_response(*, message: str, data: dict[str, Any]) -> dict[str, Any]:
    return {
        'success': True,
        'message': message,
        'data': data,
    }



def build_error_response(*, message: str, error: str) -> dict[str, Any]:
    return {
        'success': False,
        'message': message,
        'error': error,
    }
