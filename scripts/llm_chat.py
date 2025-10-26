# handlers/llm_chat.py
import os, tempfile
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler, MessageHandler, filters
from telegram.constants import ChatAction

# usamos el modelo con contexto SQLite
from utils.llm_with_sqlite_context import chat_with_context
from utils.llm import transcribe_audio

HELP_TEXT = (
    "🤖 *Modo chat inteligente:*\n"
    "• Escribe cualquier mensaje de texto — el bot responde directamente con GPT-4o\n"
    "• Envía audio (nota de voz) — se transcribe y se responde\n"
    "• /reset — limpia el contexto conversacional local\n"
    "• /llmhelp — muestra esta ayuda\n"
)

# --- TEXTO LIBRE ---
async def llm_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Procesa texto libre del usuario (sin /comando)."""
    user_q = update.message.text.strip()
    if not user_q:
        return

    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id,
        action=ChatAction.TYPING
    )

    try:
        reply = chat_with_context(user_q, limit=3)
        await update.message.reply_text(reply)
    except Exception as e:
        await update.message.reply_text(f"❌ Error LLM: {e}")

# --- AUDIO/VOZ ---
async def voice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Procesa notas de voz y responde con el modelo."""
    voice = update.message.voice
    if not voice:
        return

    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id,
        action=ChatAction.RECORD_VOICE
    )

    with tempfile.TemporaryDirectory() as td:
        local_path = os.path.join(td, "audio.ogg")
        file = await context.bot.get_file(voice.file_id)
        await file.download_to_drive(local_path)
        try:
            text = transcribe_audio(local_path)
            answer = chat_with_context(f"Transcripción del usuario: {text}")
            await update.message.reply_text(f"🗣️ Tú dijiste: {text}\n\n🤖 {answer}")
        except Exception as e:
            await update.message.reply_text(f"❌ Error al transcribir/responder: {e}")

# --- RESET ---
async def reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🧹 Contexto limpiado (local).")

# --- HELP ---
async def llm_help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_markdown(HELP_TEXT)

# --- REGISTRO ---
def register_handlers(app):
    """Registra los comandos y eventos para el LLM."""
    app.add_handler(CommandHandler("reset", reset_cmd))
    app.add_handler(CommandHandler("llmhelp", llm_help_cmd))
    app.add_handler(MessageHandler(filters.VOICE, voice_handler))
    # texto libre: si no es comando, lo manda al LLM
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, llm_text_handler))
