from __future__ import annotations
import json
import re
from typing import Any
import httpx


class LLMClient:
    def __init__(self, base_url: str, api_key: str, model: str, mode: str = "api") -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.mode = mode

    async def json_response(
        self,
        system: str,
        user: str,
        *,
        temperature: float = 0.2,
        max_tokens: int = 5000,
    ) -> dict[str, Any]:
        if self.mode == "stub":
            return self._stub(system, user)
        if not self.api_key or not self.model:
            raise ValueError("Для LLM_MODE=api нужны LLM_API_KEY и LLM_MODEL")
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"},
        }
        async with httpx.AsyncClient(timeout=180) as client:
            for _ in range(4):
                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    json=payload,
                )
                if response.status_code != 400:
                    break
                error = response.text.lower()
                if "response_format" in error and "response_format" in payload:
                    payload.pop("response_format", None)
                    continue
                if "max_tokens" in error and "max_tokens" in payload:
                    payload["max_completion_tokens"] = payload.pop("max_tokens")
                    continue
                if "temperature" in error and "temperature" in payload:
                    payload.pop("temperature", None)
                    continue
                break
            response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        return self._parse_json(content)

    @staticmethod
    def _stub(system: str, user: str) -> dict[str, Any]:
        if "topic_ideas" in system:
            return {
                "title": "Тестовый пакет материалов",
                "category": "Тест",
                "what_happened": "Тестовый разбор без обращения к модели.",
                "confirmed_facts": [],
                "important_date_or_embargo": "",
                "missing_information": [],
                "topic_ideas": [
                    {
                        "topic": "Тестовая тема",
                        "angle": "Проверка рабочего процесса",
                        "fact_ready": True,
                    }
                ],
                "readability_notes": [],
            }
        if '"items"' in system:
            return {"items": []}
        return {
            "text": "Тестовый текст КИФА. Он создан в режиме LLM_MODE=stub.",
            "used_fact_indexes": [],
            "missing_facts": [],
            "visual_needed": False,
        }

    @staticmethod
    def _parse_json(content: str) -> dict[str, Any]:
        cleaned = content.strip()
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        value = json.loads(cleaned)
        if not isinstance(value, dict):
            raise ValueError("Модель вернула JSON не в виде объекта")
        return value
