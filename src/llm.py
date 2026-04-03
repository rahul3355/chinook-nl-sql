import json
import time

from openai import OpenAI
from src.config import (
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    MODEL_NAME,
    REASONING_MODEL_NAME,
    REASONING_EFFORT,
)


_client = OpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url=OPENROUTER_BASE_URL,
)

_token_stats = {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cost": 0.0}


def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _estimate_message_tokens(messages: list[dict]) -> int:
    total = 0
    for msg in messages:
        total += _estimate_tokens(str(msg.get("content", "")))
    return max(1, total)


def _log_token_cost(model: str, input_tokens: int, output_tokens: int, elapsed: float):
    input_rate = 0.50 if "gemini" in model.lower() else 0.26
    output_rate = 3.00 if "gemini" in model.lower() else 0.38
    input_cost = (input_tokens / 1_000_000) * input_rate
    output_cost = (output_tokens / 1_000_000) * output_rate
    total_cost = input_cost + output_cost
    total_tokens = input_tokens + output_tokens

    _token_stats["calls"] += 1
    _token_stats["input_tokens"] += input_tokens
    _token_stats["output_tokens"] += output_tokens
    _token_stats["cost"] += total_cost

    print(
        f"[LLM] Token usage: input={input_tokens}, output={output_tokens}, total={total_tokens}"
    )
    print(
        f"[LLM] Token cost: input=${input_cost:.6f}, output=${output_cost:.6f}, total=${total_cost:.6f}"
    )
    print(f"[LLM] Response time: {elapsed:.2f}s")
    print(
        f"[LLM] Cumulative: {_token_stats['calls']} calls, ${_token_stats['cost']:.6f} total cost"
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

    t0 = time.time()
    response = _client.chat.completions.create(**request)
    elapsed = time.time() - t0

    content = response.choices[0].message.content
    if content is None:
        raise ValueError(f"LLM returned empty response for model {model or MODEL_NAME}")

    model_name = model or MODEL_NAME
    usage = getattr(response, "usage", None)
    if usage:
        input_tokens = getattr(usage, "prompt_tokens", 0)
        output_tokens = getattr(usage, "completion_tokens", 0)
    else:
        input_tokens = _estimate_message_tokens(request["messages"])
        output_tokens = _estimate_tokens(content)

    _log_token_cost(model_name, input_tokens, output_tokens, elapsed)

    return content.strip()


def call_reasoning_llm(
    messages: list[dict], max_tokens: int = 8000
) -> tuple[list[dict], str]:
    """Call Gemini 3 Flash with reasoning enabled.
    Returns (reasoning_details, content)."""
    print(f"[LLM] === REASONING LLM CALL ===")
    print(f"[LLM] Model: {REASONING_MODEL_NAME}")
    print(f"[LLM] Messages: {len(messages)}")
    for i, msg in enumerate(messages):
        role = msg.get("role", "unknown")
        content_preview = msg.get("content", "")[:150]
        print(f"[LLM]   Message {i}: role={role}, content='{content_preview}...'")

    request = {
        "model": REASONING_MODEL_NAME,
        "messages": messages,
        "max_tokens": max_tokens,
        "extra_body": {
            "reasoning": {"effort": REASONING_EFFORT},
        },
    }
    print(
        f"[LLM] Request: model={request['model']}, max_tokens={request['max_tokens']}"
    )
    print(f"[LLM] Extra body: {request.get('extra_body')}")

    t0 = time.time()
    try:
        response = _client.chat.completions.create(**request)
        elapsed = time.time() - t0
        print(f"[LLM] Response received")
        msg = response.choices[0].message
        print(f"[LLM] Message type: {type(msg)}")
        print(f"[LLM] Message attrs: {[a for a in dir(msg) if not a.startswith('_')]}")

        reasoning = []
        if hasattr(msg, "reasoning_details") and msg.reasoning_details:
            reasoning = msg.reasoning_details
            print(f"[LLM] Reasoning details found: {len(reasoning)} items")
            for i, step in enumerate(reasoning):
                step_text = str(step)[:200]
                print(f"[LLM]   Reasoning step {i + 1}: {step_text}...")
        else:
            print(f"[LLM] No reasoning_details attribute found")

        content = msg.content or ""
        print(f"[LLM] Content length: {len(content)} chars")
        print(f"[LLM] Content preview: {content[:500]}...")

        if not content and not reasoning:
            print(f"[LLM] WARNING: Both content and reasoning are empty!")
            print(f"[LLM] Full response: {response}")

        usage = getattr(response, "usage", None)
        if usage:
            input_tokens = getattr(usage, "prompt_tokens", 0)
            output_tokens = getattr(usage, "completion_tokens", 0)
        else:
            input_tokens = _estimate_message_tokens(messages)
            output_tokens = _estimate_tokens(content) + sum(
                _estimate_tokens(str(r)) for r in reasoning
            )

        _log_token_cost(REASONING_MODEL_NAME, input_tokens, output_tokens, elapsed)

        return reasoning, content
    except Exception as e:
        print(f"[LLM] === LLM CALL FAILED ===")
        print(f"[LLM] Error: {e}")
        import traceback

        traceback.print_exc()
        raise
