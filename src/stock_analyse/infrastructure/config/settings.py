from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import json
import os
from typing import Any

DEFAULT_SYSTEM_PROMPT = "你作为A股分析专家,请详细分析市场趋势、行业前景，揭示潜在投资机会,请确保提供充分的数据支持和专业见解。"
DEFAULT_PROMPT_TEMPLATE = """当前股票主营业务介绍:
                {stock_zyjs_ths_df}

                当前股票所在的行业资金流数据:
                {single_industry_df}

                当前股票所在的概念板块的数据:
                {concept_info_df}

                当前股票基本数据:
                {stock_individual_info_em_df}

                当前股票历史行情数据和K线技术指标::
                {stock_zh_a_hist_df}

                当前股票最近的新闻:
                {stock_news_em_df}

                当前股票历史的资金流动:
                {stock_individual_fund_flow_df}

                当前股票的财务指标数据:
                {stock_financial_analysis_indicator_df}

                """
DEFAULT_FLASK_SECRET_KEY = "stock-analyse-dev-secret"
DEFAULT_CURRENT_AI = "qwen"


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _read_json_env_list(name: str) -> list[str]:
    raw = os.getenv(name, "")
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return value if isinstance(value, list) else []


def _mask_secret(value: str | None) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}***{value[-4:]}"


@dataclass(frozen=True)
class AISettings:
    platform: str
    model_name: str
    api_key: str
    system_prompt: str
    prompt_template: str
    max_tokens: int
    temperature: float
    api_base_urls: dict[str, str]
    provider_keys: dict[str, str]
    current_ai: str
    dashscope_model_list: list[str]
    kimi_model_list: list[str]

    def model_list_for_current_ai(self) -> list[str]:
        if self.current_ai == "kimi":
            return self.kimi_model_list
        return self.dashscope_model_list

    def resolve_api_base_url(self) -> str:
        return self.api_base_urls.get(self.platform) or self.api_base_urls.get("openai", "")


@dataclass(frozen=True)
class WebSettings:
    flask_secret_key: str
    auth_enabled: bool
    auth_password: str
    session_timeout: int


@dataclass(frozen=True)
class Settings:
    path: Path
    data: dict[str, Any]
    ai: AISettings
    web: WebSettings

    @classmethod
    def from_file(cls, path: str | Path = "config.json") -> "Settings":
        settings_path = Path(path)
        if not settings_path.is_absolute():
            project_root = Path(__file__).resolve().parents[4]
            settings_path = project_root / settings_path
        data = _read_json_file(settings_path)

        ai_config = data.get("ai", {})
        api_keys = data.get("api_keys", {})
        web_auth = data.get("web_auth", {})
        api_base_urls = ai_config.get("api_base_urls", {})

        provider_keys = {
            "openai": os.getenv("OPENAI_API_KEY", api_keys.get("openai", "")),
            "anthropic": os.getenv("ANTHROPIC_API_KEY", api_keys.get("anthropic", "")),
            "zhipu": os.getenv("ZHIPU_API_KEY", api_keys.get("zhipu", "")),
            "baichuan": os.getenv("BAICHUAN_API_KEY", ""),
            "qwen": os.getenv("DASHSCOPE_API_KEY", ""),
            "kimi": os.getenv("KIMI_API_KEY", ""),
        }

        platform = ai_config.get("model_plat", "qwen")
        current_ai = os.getenv("CURRENT_AI", platform or DEFAULT_CURRENT_AI)
        resolved_api_key = os.getenv("STOCK_ANALYSE_AI_API_KEY", ai_config.get("api_key", ""))
        if not resolved_api_key:
            resolved_api_key = provider_keys.get(platform, "")

        system_prompt = ai_config.get("system_prompt", DEFAULT_SYSTEM_PROMPT)
        prompt_template = ai_config.get("prompt_template", DEFAULT_PROMPT_TEMPLATE)

        ai = AISettings(
            platform=platform,
            model_name=ai_config.get("model_name", "qwen-turbo-2025-07-15"),
            api_key=resolved_api_key,
            system_prompt=system_prompt,
            prompt_template=prompt_template,
            max_tokens=ai_config.get("max_tokens", 4000),
            temperature=ai_config.get("temperature", 0.7),
            api_base_urls={key: value for key, value in api_base_urls.items() if isinstance(value, str)},
            provider_keys=provider_keys,
            current_ai=current_ai,
            dashscope_model_list=_read_json_env_list("DASHSCOPE_MODEL_LIST"),
            kimi_model_list=_read_json_env_list("KIMI_MODEL_LIST"),
        )

        web = WebSettings(
            flask_secret_key=os.getenv("STOCK_ANALYSE_FLASK_SECRET_KEY", DEFAULT_FLASK_SECRET_KEY),
            auth_enabled=bool(web_auth.get("enabled", False)),
            auth_password=os.getenv("STOCK_ANALYSE_WEB_AUTH_PASSWORD", web_auth.get("password", "")),
            session_timeout=int(web_auth.get("session_timeout", 3600)),
        )

        return cls(path=settings_path, data=data, ai=ai, web=web)

    def as_service_config(self) -> dict[str, Any]:
        config = json.loads(json.dumps(self.data)) if self.data else {}
        config.setdefault("ai", {})
        config.setdefault("api_keys", {})
        config.setdefault("web_auth", {})

        config["ai"]["model_plat"] = self.ai.platform
        config["ai"]["model_name"] = self.ai.model_name
        config["ai"]["api_key"] = self.ai.api_key
        config["ai"]["system_prompt"] = self.ai.system_prompt
        config["ai"]["prompt_template"] = self.ai.prompt_template
        config["ai"]["max_tokens"] = self.ai.max_tokens
        config["ai"]["temperature"] = self.ai.temperature
        config["ai"]["api_base_urls"] = self.ai.api_base_urls

        for name, value in self.ai.provider_keys.items():
            if value:
                config["api_keys"][name] = value

        config["web_auth"]["enabled"] = self.web.auth_enabled
        config["web_auth"]["password"] = self.web.auth_password
        config["web_auth"]["session_timeout"] = self.web.session_timeout
        return config

    def mask_secret(self, value: str | None) -> str:
        return _mask_secret(value)


@lru_cache(maxsize=1)
def get_settings(path: str | Path = "config.json") -> Settings:
    return Settings.from_file(path)


def load_settings(path: str | Path = "config.json") -> Settings:
    get_settings.cache_clear()
    return get_settings(path)
