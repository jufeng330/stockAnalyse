from __future__ import annotations

from typing import Any

from stock_analyse.infrastructure.config.settings import get_settings


class LangChainDependencyError(ImportError):
    pass


def _import_chat_openai():
    try:
        from langchain_openai import ChatOpenAI
    except ImportError as exc:
        raise LangChainDependencyError(
            '缺少 langchain_openai 依赖，请先安装 requirements 中新增的 LangChain 相关依赖。'
        ) from exc
    return ChatOpenAI


def build_langchain_chat_model(
    *,
    llm_provider: str | None = None,
    llm_model: str | None = None,
    api_code: str | None = None,
    temperature: float | None = None,
    max_tokens: int | None = None,
    timeout: int = 300,
    model_kwargs: dict[str, Any] | None = None,
):
    settings = get_settings().ai
    provider = llm_provider or settings.platform
    api_key = api_code or settings.api_key or settings.provider_keys.get(provider, '')
    base_url = settings.api_base_urls.get(provider) or settings.resolve_api_base_url() or None
    ChatOpenAI = _import_chat_openai()
    kwargs: dict[str, Any] = {
        'model': llm_model or settings.model_name,
        'api_key': api_key,
        'temperature': settings.temperature if temperature is None else temperature,
        'max_tokens': settings.max_tokens if max_tokens is None else max_tokens,
        'timeout': timeout,
    }
    if base_url:
        kwargs['base_url'] = base_url
    if model_kwargs:
        kwargs['model_kwargs'] = model_kwargs
    return ChatOpenAI(**kwargs)
