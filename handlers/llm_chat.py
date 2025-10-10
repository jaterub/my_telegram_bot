# handlers/llm_chat.py
import os, tempfile
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler, MessageHandler, filters
from telegram.constants import ChatAction
from utils.llm import chat_simple, transcribe_audio

HELP_TEXT = (
    "🤖 Modo chat LLM:\n"
    "• Escribe cualquier mensaje de texto — el bot responde directamente\n"
    "• Envía audio (nota de voz) — se transcribe y se responde\n"
    "• /reset — limpia el contexto conversacional\n"
)

# --- TEXTO LIBRE ---
async def llm_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_q = update.message.text.strip()
    if not user_q:
        return

    # Mostrar "escribiendo..."
    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id,
        action=ChatAction.TYPING
    )

    try:
        reply = chat_simple(user_q)
        await update.message.reply_text(reply)
    except Exception as e:
        await update.message.reply_text(f"❌ Error LLM: {e}")

# --- AUDIO/VOZ ---
async def voice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            answer = chat_simple(f"Transcripción del usuario: {text}\n\nResponde a lo pedido.")
            await update.message.reply_text(f"🗣️ Tú dijiste: {text}\n\n🤖 {answer}")
        except Exception as e:
            await update.message.reply_text(f"❌ Error al transcribir/responder: {e}")

# --- RESET ---
async def reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # si luego guardas contexto conversacional, aquí lo reseteas
    await update.message.reply_text("🧹 Contexto limpiado (local).")

# --- HELP ---
async def llm_help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT)

# --- REGISTRO ---
def register_handlers(app):
    app.add_handler(CommandHandler("reset", reset_cmd))
    app.add_handler(CommandHandler("llmhelp", llm_help_cmd))
    app.add_handler(MessageHandler(filters.VOICE, voice_handler))
    # Captura cualquier texto que no sea comando y lo manda al LLM
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, llm_text_handler))
