# handlers/audit.py
# ─────────────────────────────────────────────────────────────────────────────
# /audit → guía
# Documento .csv → lo codifica en base64 y lanza un Job (run-now) en Databricks
# Polling → get-output → responde resumen y guarda log en SQLite
# ─────────────────────────────────────────────────────────────────────────────

import os, asyncio, base64, requests
from pathlib import Path
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler, MessageHandler, filters
from db import sqlite_store as store

# Config Databricks (.env)
DBX_HOST  = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
DBX_TOKEN = os.getenv("DATABRICKS_TOKEN") or ""
JOB_ID    = int(os.getenv("DATABRICKS_JOB_ID_AUDIT", "0"))

MAX_SIZE = 350_000  # ~350 KB (la base64 crece ~33%)

def _h():
    return {"Authorization": f"Bearer {DBX_TOKEN}", "Content-Type": "application/json"}

def _url(p: str) -> str:
    return f"{DBX_HOST}{p}"

# ---- Llamadas síncronas a la API (se usarán con asyncio.to_thread) ----

def _run_now_b64_sync(job_id: int, csv_b64: str, file_name: str) -> int:
    payload = {
        "job_id": job_id,
        "notebook_params": {
            "csv_b64": csv_b64,
            "file_name": file_name
        },
    }
    r = requests.post(_url("/api/2.2/jobs/run-now"), headers=_h(), json=payload, timeout=60)
    r.raise_for_status()
    return r.json()["run_id"]

def _get_state_sync(run_id: int) -> dict:
    r = requests.get(_url("/api/2.2/jobs/runs/get"), headers=_h(), params={"run_id": run_id}, timeout=60)
    r.raise_for_status()
    return r.json()["state"]

def _get_output_sync(run_id: int) -> str:
    r = requests.get(_url("/api/2.1/jobs/runs/get-output"), headers=_h(), params={"run_id": run_id}, timeout=60)
    r.raise_for_status()
    out = r.json().get("notebook_output", {})
    return out.get("result", "(sin resultado)")

# ---- Handlers ----

async def audit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra cómo usar /audit."""
    if not (DBX_HOST and DBX_TOKEN and JOB_ID):
        return await update.message.reply_text("⚠️ Configura DATABRICKS_HOST, DATABRICKS_TOKEN y DATABRICKS_JOB_ID_AUDIT en .env")
    await update.message.reply_text(
        "Envía un archivo .csv (como documento) y lo auditaré en Databricks.\n"
        "Tamaño máximo para la demo: ~350 KB."
    )

async def audit_doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recibe un CSV → run-now (csv_b64) → polling → get-output → responde y guarda log."""
    if not (DBX_HOST and DBX_TOKEN and JOB_ID):
        return await update.message.reply_text("⚠️ Config de Databricks incompleta. Revisa .env")

    doc = update.message.document
    if not doc or not doc.file_name.lower().endswith(".csv"):
        return await update.message.reply_text("Adjunta un archivo con extensión .csv.")

    # Descargar a tmp local del bot
    tmp_dir = Path("tmp"); tmp_dir.mkdir(exist_ok=True)
    local_path = tmp_dir / f"{update.effective_chat.id}_{doc.file_name}"
    tg_file = await context.bot.get_file(doc.file_id)
    await tg_file.download_to_drive(str(local_path))

    data = local_path.read_bytes()
    if len(data) > MAX_SIZE:
        return await update.message.reply_text("CSV demasiado grande para este PoC (máx ~350 KB).")

    b64 = base64.b64encode(data).decode("utf-8")

    # Lanzar Job
    run_id = await asyncio.to_thread(_run_now_b64_sync, JOB_ID, b64, doc.file_name)
    run_url = f"{DBX_HOST}/jobs/runs/{run_id}"
    await update.message.reply_text(f"Job lanzado ✅ run_id={run_id}\n{run_url}\nConsultando estado…")

    # Polling (hasta ~2 min)
    for _ in range(20):
        state = await asyncio.to_thread(_get_state_sync, run_id)
        if state.get("life_cycle_state") in {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}:
            break
        await asyncio.sleep(6)

    # Output + persistencia
    try:
        summary_text = await asyncio.to_thread(_get_output_sync, run_id)  # ← DENTRO de una función async
        await update.message.reply_text(f"Resumen auditoría:\n{summary_text}\n{run_url}")

        store.init()
        store.save_audit(
            chat_id=update.effective_chat.id,
            file_name=doc.file_name,
            summary=summary_text,
            run_id=run_id,
            run_url=run_url,
        )
    except Exception as e:
        await update.message.reply_text(
            f"Terminado. Revisa detalles en el run:\n{run_url}\n(no se pudo leer el output: {e})"
        )

def register_handlers(app):
    app.add_handler(CommandHandler("audit", audit_cmd))
    app.add_handler(MessageHandler(filters.Document.MimeType("text/csv"), audit_doc))
    app.add_handler(MessageHandler(filters.Document.ALL & filters.Regex(r"\.csv$"), audit_doc))
