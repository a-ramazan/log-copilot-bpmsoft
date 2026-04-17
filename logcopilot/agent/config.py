from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from langchain_openai import ChatOpenAI
from dotenv import load_dotenv


LOCAL_PROVIDER = "local"
YANDEX_PROVIDER = "yandex"
DEFAULT_DB_PATH = "out/logcopilot.sqlite"
DEFAULT_TEMPERATURE = 0.1


@dataclass
class AgentModelConfig:
    provider: str
    model: str
    base_url: str
    api_key: str
    temperature: float
    folder_id: str | None = None


def resolve_model_config(
    provider: str = LOCAL_PROVIDER,
    model: str | None = None,
    base_url: str | None = None,
    api_key: str | None = None,
    folder_id: str | None = None,
) -> AgentModelConfig:
    load_dotenv()

    # Если провайдер яндекс
    if provider == YANDEX_PROVIDER:


        folder_id = folder_id or os.getenv("YC_FOLDER_ID")
        api_key = api_key or os.getenv("YC_AI_API_KEY")


        if not folder_id:
            raise ValueError("В .env не задан YC_FOLDER_ID.")
        if not api_key:
            raise ValueError("В .env не задан YC_AI_API_KEY.")


        return AgentModelConfig(
            provider=provider,
            model=model or os.getenv("YC_MODEL", f"gpt://{folder_id}/yandexgpt/latest"),
            base_url=base_url or os.getenv("YC_BASE_URL", "https://llm.api.cloud.yandex.net/v1"),
            api_key=api_key,
            temperature = float(os.getenv("LLM_TEMPERATURE", DEFAULT_TEMPERATURE)),
            folder_id = folder_id,
        )
    # Если запуск производиться локально
    return AgentModelConfig(
        provider = provider,
        model=model or os.getenv("LOCAL_LLM_MODEL", "qwen/qwen3.5-9b"),
        base_url=base_url or os.getenv("LOCAL_LLM_BASE_URL", "http://127.0.0.1:1234/v1"),
        api_key=api_key or os.getenv("LOCAL_LLM_API_KEY", "lm-studio"),
        temperature=float(os.getenv("LLM_TEMPERATURE", DEFAULT_TEMPERATURE)),
        folder_id=None,
    )


def build_chat_model(config: AgentModelConfig) -> ChatOpenAI:
    kwargs = {
        "model": config.model,
        "base_url": config.base_url,
        "api_key": config.api_key,
        "temperature": config.temperature,
    }
    if config.provider == YANDEX_PROVIDER and config.folder_id:
        kwargs["default_headers"] = {"OpenAI-Project": config.folder_id}
    return ChatOpenAI(**kwargs)
