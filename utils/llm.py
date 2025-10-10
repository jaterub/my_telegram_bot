# utils/llm.py
import os, json
from typing import List, Dict, Any, Optional
from openai import OpenAI




_OPENAI_MODEL_CHAT = os.getenv("OPENAI_MODEL_CHAT", "gpt-4o-mini")
_OPENAI_MODEL_WHISPER = os.getenv("OPENAI_MODEL_WHISPER", "whisper-1")

_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_AUDIT = (
    "Eres un auditor financiero que escribe en español claro, conciso y profesional. "
    "Resalta incidencias con viñetas, usa títulos breves y evita jerga innecesaria."
)

def chat_simple(user_text: str, system: Optional[str] = None) -> str:
    sys = system or "Eres un asistente útil que responde en español."
    rsp = _client.chat.completions.create(
        model=_OPENAI_MODEL_CHAT,
        messages=[
            {"role": "system", "content": sys},
            {"role": "user", "content": user_text},
        ],
        temperature=0.2,
    )
    return rsp.choices[0].message.content.strip()

def summarize_audit_spanish(summary_json: Dict[str, Any]) -> str:
    """
    Recibe el JSON crudo del notebook (ya parseado) y devuelve un resumen 'bonito' en español.
    """
    prompt = (
        "Formatea el siguiente JSON de auditoría como un resumen claro en español, con títulos y viñetas, "
        "mostrando totales y hasta 10 ejemplos por cada categoría si existen. Mantén los números tal cual.\n\n"
        f"JSON:\n{json.dumps(summary_json, ensure_ascii=False)}"
    )
    return chat_simple(prompt, system=SYSTEM_AUDIT)

def transcribe_audio(filepath: str) -> str:
    """
    Transcribe un audio (ogg/mp3/wav) a texto usando Whisper.
    """
    with open(filepath, "rb") as f:
        tx = _client.audio.transcriptions.create(
            model=_OPENAI_MODEL_WHISPER,
            file=f
        )
    return tx.text.strip()
