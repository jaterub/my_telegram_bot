from db import sqlite_store as store

try:
    summary_text = await asyncio.to_thread(_get_output_sync, run_id)  # string JSON
    await update.message.reply_text(f"Resumen auditoría:\n{summary_text}\n{run_url}")

    # Guarda en SQLite
    store.init()   #DDL
    store.save_audit(
        chat_id=update.effective_chat.id,
        file_name=doc.file_name,
        summary=summary_text,   # puede ser str JSON
        run_id=run_id,
        run_url=run_url,
    )
except Exception as e:
    await update.message.reply_text(f"Terminado. Revisa detalles: {run_url}\n(no se pudo leer output: {e})")
