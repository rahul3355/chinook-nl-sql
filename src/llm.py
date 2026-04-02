from openai import OpenAI
from src.config import OPENROUTER_API_KEY, OPENROUTER_BASE_URL, MODEL_NAME


_client = OpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url=OPENROUTER_BASE_URL,
)


def call_llm(system_prompt: str, user_prompt: str) -> str:
    """Send a prompt to the LLM and return the text response."""
    response = _client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    return response.choices[0].message.content.strip()
