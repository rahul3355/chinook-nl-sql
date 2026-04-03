from openai import OpenAI
from src.config import OPENROUTER_API_KEY, OPENROUTER_BASE_URL, MODEL_NAME


_client = OpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url=OPENROUTER_BASE_URL,
)


def call_llm(system_prompt: str, user_prompt: str) -> str:
    """Send a prompt to the LLM and return the text response."""
    return call_llm_routed(system_prompt, user_prompt)


def call_llm_routed(
    system_prompt: str,
    user_prompt: str,
    *,
    model: str | None = None,
    provider_order: list[str] | None = None,
    allow_fallbacks: bool = True,
    max_tokens: int | None = None,
    temperature: float | None = None,
) -> str:
    """Send a prompt to a specific OpenRouter model/provider route and return text."""
    extra_body = None
    if provider_order:
        extra_body = {
            "provider": {
                "order": provider_order,
                "allow_fallbacks": allow_fallbacks,
            }
        }

    request = {
        "model": model or MODEL_NAME,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    if extra_body is not None:
        request["extra_body"] = extra_body
    if max_tokens is not None:
        request["max_tokens"] = max_tokens
    if temperature is not None:
        request["temperature"] = temperature

    response = _client.chat.completions.create(
        **request,
    )
    content = response.choices[0].message.content
    if content is None:
        raise ValueError(f"LLM returned empty response for model {model or MODEL_NAME}")
    return content.strip()
