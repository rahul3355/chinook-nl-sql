import os
from dotenv import load_dotenv

load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
MODEL_NAME = "x-ai/grok-4.1-fast"  # Stick to Grok as requested
DB_PATH = "data/olist.sqlite"
HISTORY_PATH = "history/query_history.json"
PROMPTS_DIR = "prompts"
