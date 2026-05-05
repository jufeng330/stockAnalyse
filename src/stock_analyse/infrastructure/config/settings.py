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
class MarketDataSettings:
    default_provider: str
    providers_by_market: dict[str, str]
    provider_options: dict[str, dict[str, Any]]

    @staticmethod
    def normalize_market(market: str) -> str:
        market_text = str(market or '').strip()
        return 'H' if market_text.upper() == 'HK' else market_text

    def provider_for_market(self, market: str) -> str:
        normalized_market = self.normalize_market(market)
        provider = self.providers_by_market.get(normalized_market)
        if provider:
            return str(provider).strip().lower()
        fallback = self.providers_by_market.get(str(market or '').strip())
        if fallback:
            return str(fallback).strip().lower()
        return self.default_provider

    def uses_provider(self, market: str, provider: str) -> bool:
        return self.provider_for_market(market) == str(provider or '').strip().lower()

    @property
    def futu_enabled(self) -> bool:
        options = self.provider_options.get('futu', {})
        if 'enabled' in options:
            return bool(options.get('enabled'))
        return os.getenv('FUTU_ENABLE', 'false').strip().lower() in {'1', 'true', 'yes', 'on'}

    @property
    def futu_host(self) -> str:
        options = self.provider_options.get('futu', {})
        value = options.get('opend_host')
        return str(value).strip() if value else os.getenv('FUTU_OPEND_HOST', '127.0.0.1')

    @property
    def futu_port(self) -> int:
        options = self.provider_options.get('futu', {})
        value = options.get('opend_port')
        if value is not None:
            return int(value)
        return int(os.getenv('FUTU_OPEND_PORT', '11111'))


@dataclass(frozen=True)
class Settings:
    path: Path
    data: dict[str, Any]
    ai: AISettings
    web: WebSettings
    market_data: MarketDataSettings

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
        market_data_config = data.get("market_data", {})
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
        market_data = MarketDataSettings(
            default_provider=str(market_data_config.get("default_provider", "akshare")).strip().lower() or "akshare",
            providers_by_market={
                str(key).strip(): str(value).strip().lower()
                for key, value in market_data_config.get("providers_by_market", {}).items()
                if str(key).strip() and str(value).strip()
            },
            provider_options={
                str(key).strip().lower(): value
                for key, value in market_data_config.get("providers", {}).items()
                if str(key).strip() and isinstance(value, dict)
            },
        )

        return cls(path=settings_path, data=data, ai=ai, web=web, market_data=market_data)

    def as_service_config(self) -> dict[str, Any]:
        config = json.loads(json.dumps(self.data)) if self.data else {}
        config.setdefault("ai", {})
        config.setdefault("api_keys", {})
        config.setdefault("web_auth", {})
        config.setdefault("market_data", {})

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

        config["market_data"]["default_provider"] = self.market_data.default_provider
        config["market_data"]["providers_by_market"] = self.market_data.providers_by_market
        providers = config["market_data"].get("providers")
        config["market_data"]["providers"] = providers if isinstance(providers, dict) else {}
        config["market_data"]["providers"]["futu"] = {
            "enabled": self.market_data.futu_enabled,
            "opend_host": self.market_data.futu_host,
            "opend_port": self.market_data.futu_port,
        }
        return config

    def mask_secret(self, value: str | None) -> str:
        return _mask_secret(value)


@lru_cache(maxsize=1)
def get_settings(path: str | Path = "config.json") -> Settings:
    return Settings.from_file(path)


def load_settings(path: str | Path = "config.json") -> Settings:
    get_settings.cache_clear()
    return get_settings(path)
