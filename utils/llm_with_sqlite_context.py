# utils/llm_with_sqlite_context.py
import os, json, sqlite3
from openai import OpenAI
from typing import Optional
from dotenv import load_dotenv

load_dotenv(override=True)

DB_PATH = "audits.db"
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL = os.getenv("OPENAI_MODEL_CHAT", "gpt-4o")

# 1️⃣ Obtener las últimas auditorías
def get_recent_audits(limit: int = 3):
    """Devuelve las últimas auditorías guardadas en SQLite."""
    if not os.path.exists(DB_PATH):
        return []
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT filename, created_at, summary_json 
            FROM audits 
            ORDER BY created_at DESC 
            LIMIT ?
        """, (limit,)).fetchall()

    audits = []
    for r in rows:
        try:
            audits.append({
                "filename": r["filename"],
                "created_at": r["created_at"],
                "summary": json.loads(r["summary_json"])
            })
        except Exception:
            pass
    return audits

# 2️⃣ Construir el prompt contextual
def build_prompt(user_query: str, audits):
    if not audits:
        return f"Pregunta del usuario: {user_query}"

    audits_text = "\n\n".join([
        f"📄 {a['filename']} ({a['created_at']})\n"
        f"{json.dumps(a['summary'], ensure_ascii=False)[:1500]}"
        for a in audits
    ])
    return (
        "Eres un analista financiero IA. Usa los siguientes resultados históricos de auditoría "
        "para responder con claridad, precisión y tono profesional.\n\n"
        f"=== Auditorías recientes ===\n{audits_text}\n\n"
        f"=== Pregunta del usuario ===\n{user_query}"
    )

# 3️⃣ Consulta al modelo con contexto SQLite
def chat_with_context(user_query: str, limit: int = 3) -> str:
    """Envía la consulta al modelo con el contexto de auditorías locales."""
    audits = get_recent_audits(limit)
    prompt = build_prompt(user_query, audits)

    rsp = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "Eres un experto contable y auditor que responde en español."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3,
        max_tokens=1000,
    )

    return rsp.choices[0].message.content.strip()
