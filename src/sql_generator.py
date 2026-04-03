from src.vanna_logic import vn_engine

def generate_sql(user_question: str) -> str:
    """
    Convert a natural-language question into a SQL query using the 
    DirectSchemaVanna engine.
    """
    # Vanna's generator handles the prompt construction and LLM call
    sql = vn_engine.generate_sql(user_question)
    return sql
