"""
Handlers para conversar con el LLM via comandos /chat y /reset.
Soporta texto y notas de voz (transcripcion con Whisper).
"""

from __future__ import annotations

import json
import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes, MessageHandler, filters

from utils.db import get_last_audits
from utils.llm import chat_simple, transcribe_audio

# Historial por chat_id: lista de (role, content)
_CONVERSATIONS: Dict[int, List[Tuple[str, str]]] = {}
_MAX_TURNS = 6  # numero maximo de turnos (usuario/bot) almacenados

SYSTEM_PROMPT = (
    "Eres un asesor financiero y contable util. "
    "Responde siempre en espanol, con claridad y sin inventar datos."
)


def _history(chat_id: int) -> List[Tuple[str, str]]:
    return _CONVERSATIONS.setdefault(chat_id, [])


def _collect_audit_context(limit: int = 5) -> List[Dict[str, object]]:
    """
    Prepara un resumen ligero de las ultimas auditorias para dar contexto al LLM.
    Incluye los contadores clave para que pueda razonar sobre tendencias.
    """
    rows = get_last_audits(limit=limit)
    context: List[Dict[str, object]] = []
    for (aid, filename, created_at, summary_json, run_url, llm_summary) in rows:
        entry: Dict[str, object] = {
            "id": aid,
            "filename": filename,
            "created_at": created_at,
        }
        if run_url:
            entry["run_url"] = run_url
        if llm_summary:
            entry["llm_summary"] = llm_summary

        metrics = {}
        if summary_json:
            try:
                summary = json.loads(summary_json)
            except Exception:
                summary = None

            if isinstance(summary, dict):
                if "rows" in summary:
                    entry["rows_total"] = summary.get("rows")

                for key in (
                    "invalid_date",
                    "duplicates_tx",
                    "unbalanced_tx",
                    "required_nulls",
                    "invalid_currency",
                    "inconsistent_dates",
                ):
                    value = summary.get(key)
                    if isinstance(value, dict):
                        metrics[key] = {
                            k: value.get(k)
                            for k in ("count", "percentage")
                            if isinstance(value.get(k), (int, float))
                            or isinstance(value.get(k), str)
                        }
                    elif isinstance(value, (int, float)):
                        metrics[key] = value
                if metrics:
                    entry["metrics"] = metrics

        context.append(entry)

    return context


def _build_prompt(
    history: List[Tuple[str, str]],
    context_data: List[Dict[str, object]],
) -> str:
    """Convierte el historial en un bloque de texto para el LLM."""
    recent = history[-(_MAX_TURNS * 2) :]
    lines = []
    for role, content in recent:
        prefix = "Usuario" if role == "user" else "Asistente"
        lines.append(f"{prefix}: {content}")
    lines.append("Asistente:")

    conversation = "\n".join(lines)
    context_json = json.dumps(context_data, ensure_ascii=False)

    return (
        "Contexto de auditorias recientes (JSON):\n"
        f"{context_json}\n\n"
        "Conversacion:\n"
        f"{conversation}"
    )


async def _ask_llm(chat_id: int, user_text: str) -> str:
    history = _history(chat_id)
    history.append(("user", user_text))
    context_data = _collect_audit_context(limit=5)
    prompt = _build_prompt(history, context_data)

    try:
        reply = await asyncio.to_thread(chat_simple, prompt, SYSTEM_PROMPT)
    except Exception:
        logging.exception("LLM chat call failed")
        history.pop()  # revert ultimo mensaje para no duplicar historial
        raise

    history.append(("assistant", reply))
    if len(history) > _MAX_TURNS * 2:
        _CONVERSATIONS[chat_id] = history[-(_MAX_TURNS * 2) :]
    return reply


async def _handle_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    prefix: str | None = None,
) -> None:
    chat_id = update.effective_chat.id
    try:
        reply = await _ask_llm(chat_id, text)
    except Exception:
        await update.message.reply_text("No pude obtener respuesta del asistente. Intenta mas tarde.")
        return

    message = f"{prefix}\n\n{reply}" if prefix else reply
    await update.message.reply_text(message)


async def chat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Activa el modo chat y, si hay texto, envia la consulta al LLM."""
    context.user_data["llm_chat_enabled"] = True
    user_text = " ".join(context.args).strip()

    if not user_text:
        await update.message.reply_text(
            "Modo chat activado. Envia mensajes de texto o notas de voz para continuar. "
            "Usa /reset para limpiar el historial."
        )
        return

    await _handle_text(update, context, user_text)


async def reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Limpia el historial de conversacion del chat actual."""
    chat_id = update.effective_chat.id
    _CONVERSATIONS.pop(chat_id, None)
    context.user_data["llm_chat_enabled"] = True
    await update.message.reply_text("Historial de conversacion reiniciado.")


async def chat_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Responde a mensajes de texto si el modo chat esta activo."""
    if not context.user_data.get("llm_chat_enabled", True):
        return
    if not update.message or not update.message.text:
        return
    if update.message.text.startswith("/"):
        return

    await _handle_text(update, context, update.message.text.strip())


async def chat_voice_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Transcribe notas de voz/audio y las envia al LLM."""
    if not context.user_data.get("llm_chat_enabled", True):
        return
    message = update.message
    if not message:
        return

    voice = message.voice or message.audio
    if not voice:
        return

    file = await voice.get_file()
    suffix = Path(file.file_path or "").suffix or ".ogg"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp_path = Path(tmp.name)
    try:
        await file.download_to_drive(custom_path=str(tmp_path))
        transcript = await asyncio.to_thread(transcribe_audio, str(tmp_path))
    except Exception:
        logging.exception("Voice transcription failed")
        await message.reply_text("No pude transcribir el audio. Intenta de nuevo.")
        tmp_path.unlink(missing_ok=True)
        return
    finally:
        tmp_path.unlink(missing_ok=True)

    transcript = transcript.strip()
    if not transcript:
        await message.reply_text("No se detecto texto en el audio.")
        return

    await _handle_text(
        update,
        context,
        transcript,
        prefix=f"[voz] Texto detectado: {transcript}",
    )


def register_handlers(app) -> None:
    """Registra comandos y handlers necesarios para el chat LLM."""
    app.add_handler(CommandHandler("chat", chat_cmd))
    app.add_handler(CommandHandler("reset", reset_cmd))
    app.add_handler(
        MessageHandler(filters.TEXT & (~filters.COMMAND), chat_text_message)
    )
    app.add_handler(
        MessageHandler((filters.VOICE | filters.AUDIO), chat_voice_message)
    )
