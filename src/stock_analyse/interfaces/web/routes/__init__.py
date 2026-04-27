from __future__ import annotations

from .analysis import register_analysis_routes
from .auth import register_auth_routes
from .history import register_history_routes
from .misc import register_misc_routes
from .trading_decision import register_trading_decision_routes

__all__ = [
    'register_analysis_routes',
    'register_auth_routes',
    'register_history_routes',
    'register_misc_routes',
    'register_trading_decision_routes',
]
