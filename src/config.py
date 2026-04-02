import os
from dotenv import load_dotenv

load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
MODEL_NAME = "x-ai/grok-4.1-fast"
DB_PATH = "data/Chinook.sqlite"
HISTORY_PATH = "history/query_history.json"
PROMPTS_DIR = "prompts"
