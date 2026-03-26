import json
import logging
import httpx
import threading
import time
import hashlib
from app.config import settings

logger = logging.getLogger(__name__)


class AzureOpenAIService:
    _sdk_mismatch_warned = False
    _cache_lock = threading.Lock()
    _response_cache = {}

    def __init__(self):
        if not settings.AZURE_OPENAI_API_KEY or not settings.AZURE_OPENAI_ENDPOINT:
            raise ValueError(
                "AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT are required."
            )

        self.api_key = settings.AZURE_OPENAI_API_KEY
        self.api_version = settings.AZURE_OPENAI_API_VERSION
        self.azure_endpoint = settings.AZURE_OPENAI_ENDPOINT.rstrip("/")
        self.deployment = settings.AZURE_OPENAI_DEPLOYMENT
        self.client = None
        self._use_sdk = True

        try:
            # Lazy import to avoid startup crashes when OpenAI SDK/httpx is mismatched.
            from openai import AzureOpenAI
            self.client = AzureOpenAI(
                api_key=self.api_key,
                api_version=self.api_version,
                azure_endpoint=self.azure_endpoint,
            )
        except TypeError as e:
            if "proxies" in str(e):
                if not AzureOpenAIService._sdk_mismatch_warned:
                    logger.warning(
                        "AzureOpenAI SDK/httpx mismatch detected (%s). Falling back to direct REST API calls.",
                        str(e),
                    )
                    AzureOpenAIService._sdk_mismatch_warned = True
                self._use_sdk = False
            else:
                raise

    def chat(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
        max_tokens: int = 4000,
        json_mode: bool = False,
    ) -> str:
        kwargs = {
            "model": self.deployment,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        cache_key = None
        if settings.LLM_CACHE_ENABLED:
            cache_key = self._build_cache_key(kwargs)
            cached = self._cache_get(cache_key)
            if cached is not None:
                return cached

        if self._use_sdk:
            content = self._chat_via_sdk_with_retry(**kwargs)
            if cache_key:
                self._cache_set(cache_key, content)
            return content

        content = self._chat_via_rest_with_retry(**kwargs)
        if cache_key:
            self._cache_set(cache_key, content)
        return content

    def chat_json(self, system_prompt: str, user_prompt: str) -> dict:
        response = self.chat(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            json_mode=True,
        )
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            cleaned = self._extract_json_object(response)
            if cleaned is None:
                logger.error("Model returned non-JSON content: %s", response)
                raise ValueError("Model response was not valid JSON.")
            return json.loads(cleaned)

    @staticmethod
    def _extract_json_object(text: str | None) -> str | None:
        if not text:
            return None

        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.replace("```json", "").replace("```", "").strip()

        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None

        return cleaned[start : end + 1]

    def _chat_via_rest(self, **kwargs) -> str:
        url = (
            f"{self.azure_endpoint}/openai/deployments/{self.deployment}/chat/completions"
            f"?api-version={self.api_version}"
        )

        payload = {
            "messages": kwargs["messages"],
            "temperature": kwargs.get("temperature", 0.2),
            "max_tokens": kwargs.get("max_tokens", 4000),
        }
        if "response_format" in kwargs:
            payload["response_format"] = kwargs["response_format"]

        headers = {
            "api-key": self.api_key,
            "Content-Type": "application/json",
        }

        with httpx.Client(timeout=120.0) as client:
            response = client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()

        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as e:
            logger.error("Unexpected Azure OpenAI REST response format: %s", data)
            raise ValueError(f"Invalid Azure OpenAI REST response format: {e}")

    def _chat_via_sdk_with_retry(self, **kwargs) -> str:
        retries = max(int(getattr(settings, "AZURE_OPENAI_RETRY_COUNT", 2) or 2), 0)
        delay_seconds = max(float(getattr(settings, "AZURE_OPENAI_RETRY_DELAY_SECONDS", 1.0) or 1.0), 0.1)
        last_error = None
        for attempt in range(retries + 1):
            try:
                response = self.client.chat.completions.create(**kwargs)
                return response.choices[0].message.content
            except Exception as e:
                last_error = e
                if not self._is_rate_limit_error(e) or attempt >= retries:
                    raise
                logger.warning("Azure OpenAI rate limit hit; retrying in %.1fs (attempt %s/%s).", delay_seconds, attempt + 1, retries + 1)
                time.sleep(delay_seconds)
        raise last_error

    def _chat_via_rest_with_retry(self, **kwargs) -> str:
        retries = max(int(getattr(settings, "AZURE_OPENAI_RETRY_COUNT", 2) or 2), 0)
        delay_seconds = max(float(getattr(settings, "AZURE_OPENAI_RETRY_DELAY_SECONDS", 1.0) or 1.0), 0.1)
        last_error = None
        for attempt in range(retries + 1):
            try:
                return self._chat_via_rest(**kwargs)
            except Exception as e:
                last_error = e
                if not self._is_rate_limit_error(e) or attempt >= retries:
                    raise
                logger.warning("Azure OpenAI REST rate limit hit; retrying in %.1fs (attempt %s/%s).", delay_seconds, attempt + 1, retries + 1)
                time.sleep(delay_seconds)
        raise last_error

    @staticmethod
    def _is_rate_limit_error(error: Exception) -> bool:
        status_code = getattr(error, "status_code", None)
        if status_code == 429:
            return True
        response = getattr(error, "response", None)
        if getattr(response, "status_code", None) == 429:
            return True
        return "ratelimit" in str(error).lower() or "429" in str(error)

    def _build_cache_key(self, kwargs: dict) -> str:
        serialized = json.dumps(
            {
                "model": kwargs.get("model"),
                "messages": kwargs.get("messages"),
                "temperature": kwargs.get("temperature"),
                "max_tokens": kwargs.get("max_tokens"),
                "response_format": kwargs.get("response_format"),
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _cache_get(self, key: str) -> str | None:
        now = time.time()
        ttl = max(settings.LLM_CACHE_TTL_SECONDS, 0)
        with AzureOpenAIService._cache_lock:
            item = AzureOpenAIService._response_cache.get(key)
            if not item:
                return None
            if ttl and now - item["ts"] > ttl:
                AzureOpenAIService._response_cache.pop(key, None)
                return None
            return item["value"]

    def _cache_set(self, key: str, value: str) -> None:
        with AzureOpenAIService._cache_lock:
            AzureOpenAIService._response_cache[key] = {"value": value, "ts": time.time()}
