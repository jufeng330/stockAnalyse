from __future__ import annotations

import copy
import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_SELECT_CONFIG: dict[str, Any] = {
    "version": 1,
    "defaults": {
        "meta": {
            "enabled": True,
        },
        "filters": {
            "financial": {
                "mode": "avg",
                "lookback_years": 3,
                "continuous_years": 3,
                "profit_min": 0,
                "profit_growth_min": 0,
                "revenue_growth_min": 0,
            }
        },
    },
    "strategies": {
        "1": {
            "meta": {
                "code": 1,
                "name": "高股息选股策略_1",
                "enabled": True,
                "markets": ["SH", "SZ", "H", "usa"],
            },
            "filters": {
                "spot": {
                    "market_cap_min": 10000000000,
                    "pe_dynamic_max": 15,
                    "fallback_roe_avg_min": 15,
                    "fallback_revenue_growth_min": 20,
                },
                "dividend": {
                    "mode": "avg",
                    "dividend_yield_min": 0.03,
                    "min_dividend_years": 3,
                },
                "risk": {
                    "debt_ratio_max": 70,
                },
                "financial": {
                    "mode": "avg",
                    "lookback_years": 3,
                    "profit_min": 0,
                    "profit_growth_min": 0,
                    "revenue_growth_min": 0,
                },
            },
        },
        "2": {
            "meta": {
                "code": 2,
                "name": "优质股筛选策略_2",
                "enabled": True,
                "markets": ["SH", "SZ", "H", "usa"],
            },
            "filters": {
                "spot": {
                    "market_cap_min": 10000000000,
                    "pe_dynamic_max": 15,
                    "fallback_roe_avg_min": 15,
                    "fallback_revenue_growth_min": 20,
                    "fallback_net_profit_growth_min": 10,
                    "pb_max": 5,
                    "revenue_total_min": 1000000000,
                },
                "risk": {
                    "debt_ratio_max": 80,
                },
                "financial": {
                    "mode": "avg",
                    "lookback_years": 3,
                    "profit_min": 50000000,
                    "profit_growth_min": 0.05,
                    "revenue_growth_min": 0.05,
                },
            },
        },
        "3": {
            "meta": {
                "code": 3,
                "name": "保守型筛选策略_3",
                "enabled": True,
                "markets": ["SH", "SZ"],
            },
            "filters": {
                "spot": {
                    "market_cap_min": 50000000000,
                    "pe_dynamic_max": 15,
                },
                "risk": {
                    "debt_ratio_max": 60,
                },
                "dividend": {
                    "mode": "avg",
                    "dividend_yield_min": 0.03,
                    "min_dividend_years": 5,
                },
            },
        },
        "4": {
            "meta": {
                "code": 4,
                "name": "成长型筛选策略_4",
                "enabled": True,
                "markets": ["SH", "SZ", "H", "usa"],
            },
            "filters": {
                "spot": {
                    "revenue_total_min": 1000000000,
                },
                "financial": {
                    "mode": "avg",
                    "lookback_years": 2,
                    "profit_min": 0,
                    "profit_growth_min": 0.2,
                    "revenue_growth_min": 0.3,
                },
            },
            "market_overrides": {
                "usa": {
                    "filters": {
                        "financial": {
                            "profit_growth_min": 20,
                            "revenue_growth_min": 30,
                        }
                    }
                }
            },
        },
        "5": {
            "meta": {
                "code": 5,
                "name": "价值型筛选策略_5",
                "enabled": True,
                "markets": ["SH", "SZ", "H", "usa"],
            },
            "filters": {
                "spot": {
                    "market_cap_min": 50000000000,
                    "pe_dynamic_max": 12,
                    "pb_max": 1.5,
                },
                "financial": {
                    "mode": "avg",
                    "lookback_years": 3,
                    "profit_min": 0,
                    "profit_growth_min": 0,
                    "revenue_growth_min": 0,
                    "roe_min": 15,
                },
            },
        },
        "6": {
            "meta": {
                "code": 6,
                "name": "知名股票筛选策略_6",
                "enabled": True,
                "markets": ["SH", "SZ", "H", "usa"],
            },
            "filters": {
                "spot": {
                    "pe_dynamic_max": 50,
                },
                "financial": {
                    "mode": "avg",
                    "lookback_years": 3,
                    "profit_min": 0,
                    "profit_growth_min": 0.05,
                    "revenue_growth_min": 0.05,
                },
            },
            "market_overrides": {
                "usa": {
                    "filters": {
                        "financial": {
                            "profit_growth_min": 5,
                            "revenue_growth_min": 10,
                        }
                    }
                }
            },
        },
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _resolve_config_path(path: str | Path = "config_select.json") -> Path:
    config_path = Path(path)
    if config_path.is_absolute():
        return config_path
    project_root = Path(__file__).resolve().parents[3]
    return project_root / config_path


def get_default_select_config() -> dict[str, Any]:
    return copy.deepcopy(DEFAULT_SELECT_CONFIG)


@lru_cache(maxsize=4)
def load_select_config(path: str | Path = "config_select.json") -> dict[str, Any]:
    config_path = _resolve_config_path(path)
    defaults = get_default_select_config()
    if not config_path.exists():
        logger.warning("选股配置文件不存在，已回退默认选股参数: %s", config_path)
        return defaults
    try:
        with config_path.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except json.JSONDecodeError as exc:
        logger.warning("选股配置 JSON 非法，已回退默认选股参数: %s | error=%s", config_path, exc)
        return defaults
    except OSError as exc:
        logger.warning("选股配置读取失败，已回退默认选股参数: %s | error=%s", config_path, exc)
        return defaults
    if not isinstance(data, dict):
        logger.warning("选股配置格式错误，已回退默认选股参数: %s", config_path)
        return defaults
    return _deep_merge(defaults, data)


def reload_select_config(path: str | Path = "config_select.json") -> dict[str, Any]:
    load_select_config.cache_clear()
    return load_select_config(path)
