# handlers/audit_xlsx.py
# ─────────────────────────────────────────────────────────────────────────────
# /audit: recibe .xlsx → lo codifica en base64 → Databricks Jobs run-now
# Polling → get-output → formatea JSON en viñetas y responde en Telegram
# (opcional) guarda histórico en SQLite si está disponible
# ─────────────────────────────────────────────────────────────────────────────

import os, asyncio, base64, json, requests
from pathlib import Path
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler, MessageHandler, filters

# ---------- Configuración (leída en runtime para evitar problemas de orden) ----------
def _cfg():
    host  = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN") or ""
    job   = int(os.getenv("DATABRICKS_JOB_ID_AUDIT", "0") or "0")
    return host, token, job

def _h(token: str):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def _url(host: str, p: str) -> str:
    return f"{host}{p}"

# ---------- Llamadas síncronas a la API (se ejecutan en threads) ----------
def _run_now_sync(job_id: int, file_b64: str, host: str, token: str) -> int:
    payload = {"job_id": job_id, "notebook_params": {"file_b64": file_b64}}
    r = requests.post(f"{host}/api/2.2/jobs/run-now",
                      headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                      json=payload, timeout=60)
    if r.status_code >= 400:
        # 👇 imprime el cuerpo para saber la causa (param no válido, tipo de tarea, etc.)
        raise requests.HTTPError(f"{r.status_code} {r.reason}: {r.text}")
    return r.json()["run_id"]


def _get_state_sync(run_id: int, host: str, token: str) -> dict:
    r = requests.get(_url(host, "/api/2.2/jobs/runs/get"), headers=_h(token), params={"run_id": run_id}, timeout=60)
    r.raise_for_status()
    return r.json()["state"]

def _get_output_sync(run_id: int, host: str, token: str) -> str:
    r = requests.get(_url(host, "/api/2.1/jobs/runs/get-output"), headers=_h(token), params={"run_id": run_id}, timeout=60)
    r.raise_for_status()
    # Esperamos un result string (JSON) de notebook_output
    return r.json().get("notebook_output", {}).get("result", "")

# ---------- (A) FORMATEADOR EN VIÑETAS PARA TELEGRAM ----------
def _fmt_summary(summary: dict) -> str:
    """Convierte el JSON del notebook a un texto legible con viñetas."""
    def sec(title, key, emoji):
        blk = summary.get(key, {}) or {}
        cnt = blk.get("count", 0)
        items = blk.get("items", [])[:5]  # muestra hasta 5
        lines = [f"{emoji} *{title}*: *{cnt}*"]
        for it in items:
            if key == "unbalanced_tx":
                lines.append(
                    f"  • tx `{it.get('tx_id')}` — debit={it.get('sum_debit')} "
                    f"credit={it.get('sum_credit')} diff={it.get('diff')}  _{it.get('suggestion')}_"
                )
            else:
                lines.append(
                    f"  • fila {it.get('row')} tx `{it.get('tx_id')}` — {it.get('reason')}  _{it.get('suggestion')}_"
                )
        return "\n".join(lines)

    rows = summary.get("rows", 0)
    parts = [
        "📊 *Auditoría contable*",
        f"Total de filas: `{rows}`",
        sec("Fechas inválidas", "invalid_date", "🗓️"),
        sec("Duplicados (tx_id)", "duplicates_tx", "🔁"),
        sec("Desbalances", "unbalanced_tx", "⚖️"),
        sec("Obligatorios nulos", "required_nulls", "❗"),
    ]
    return "\n".join(parts)

# ---------- Persistencia opcional ----------
try:
    from db import sqlite_store as store
    _HAS_STORE = True
except Exception:
    _HAS_STORE = False

MAX_SIZE = 2_000_000  # ~2 MB para la demo

# ---------- Handlers ----------
async def audit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    host, token, job_id = _cfg()
    if not (host and token and job_id):
        return await update.message.reply_text("⚠️ Configura DATABRICKS_HOST, DATABRICKS_TOKEN y DATABRICKS_JOB_ID_AUDIT en .env")
    await update.message.reply_text(
        "🔎 Auditoría contable: envíame tu Excel (.xlsx) como *documento*.\n"
        "Validaré fechas, duplicados, desbalances y campos obligatorios. ⚖️"
    )
async def audit_doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 0) Config en runtime (evita problemas de orden de imports)
    host, token, job_id = _cfg()
    if not (host and token and job_id):
        return await update.message.reply_text("⚠️ Config de Databricks incompleta. Revisa .env")

    # 1) Validación del documento
    doc = update.message.document
    if not doc or not doc.file_name.lower().endswith(".xlsx"):
        return await update.message.reply_text("⚠️ Necesito un Excel con extensión .xlsx (envíalo como *documento*).")

    # 2) Descargar localmente (bot)
    tmp_dir = Path("tmp"); tmp_dir.mkdir(exist_ok=True)
    local_path = tmp_dir / f"{update.effective_chat.id}_{doc.file_name}"
    tg_file = await context.bot.get_file(doc.file_id)
    await tg_file.download_to_drive(str(local_path))

    data = local_path.read_bytes()
    if len(data) > MAX_SIZE:
        return await update.message.reply_text("Archivo demasiado grande para esta demo (máx ~2 MB).")

    file_b64 = base64.b64encode(data).decode("utf-8")

    # 3) Lanzar Job en Databricks
    run_id = await asyncio.to_thread(_run_now_sync, job_id, file_b64, host, token)
    await update.message.reply_text(
        f"🚀 Ejecutando auditoría en Databricks…\nrun_id={run_id}\nTe aviso al terminar."
    )
    run_url = f"{host}/jobs/runs/{run_id}"

    # 4) Polling ROBUSTO: espera a que termine (con timeout)
    max_secs = 600   # hasta 10 min
    interval = 5     # consulta cada 5s
    waited = 0
    final_state = None

    while waited < max_secs:
        state = await asyncio.to_thread(_get_state_sync, run_id, host, token)
        life  = state.get("life_cycle_state")
        rstate = state.get("result_state")
        # Termina cuando ya no está en estado en ejecución
        if life in {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}:
            final_state = state
            break
        await asyncio.sleep(interval)
        waited += interval

    if not final_state:
        # Timeout
        return await update.message.reply_text(
            "⏱️ La ejecución está tardando más de lo previsto (sigue en RUNNING). "
            "Vuelve a intentar en unos minutos."
        )

    # 5) Lectura del output con reintentos (a veces tarda en publicarse)
    output = ""
    for _ in range(12):  # ~60s de margen extra
        try:
            output = await asyncio.to_thread(_get_output_sync, run_id, host, token)
            if output:
                break
        except Exception:
            pass
        await asyncio.sleep(5)

    if not output:
        status = f"{final_state.get('life_cycle_state')}/{final_state.get('result_state')}"
        return await update.message.reply_text(
            "⚠️ No pude leer la salida del Job todavía.\n"
            f"Estado final: {status}\n"
            "Asegúrate de que el notebook termine con dbutils.notebook.exit(JSON)."
        )

    # 6) Formatear el JSON a viñetas “humanas” y responder
    try:
        summary = json.loads(output) if isinstance(output, str) else output
        text = _fmt_summary(summary)
        await update.message.reply_markdown(text)

        # 7) Persistencia opcional en SQLite
        if _HAS_STORE:
            try:
                store.init()
                store.save_audit(
                    chat_id=update.effective_chat.id,
                    file_name=doc.file_name,
                    summary=json.dumps(summary, ensure_ascii=False),
                    run_id=run_id,
                    run_url=run_url,
                )
            except Exception:
                pass

    except Exception as e:
        await update.message.reply_text(f"⚠️ Salida no válida (no es JSON parseable): {e}")


def register_handlers(app):
    app.add_handler(CommandHandler("audit", audit_cmd))
    # Excel por MIME y por extensión
    app.add_handler(MessageHandler(filters.Document.MimeType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"), audit_doc))
    app.add_handler(MessageHandler(filters.Document.FileExtension("xlsx"), audit_doc))
