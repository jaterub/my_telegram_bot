from dotenv import load_dotenv
load_dotenv(override=True)  # antes de importar utils que leen env
from utils.llm_with_sqlite_context import chat_with_context

print(chat_with_context("Hola, prueba contexto"))
