# handlers/audit_xlsx.py
# ─────────────────────────────────────────────────────────────────────────────
# /audit: recibe .xlsx → (sube a DBFS) → Databricks Jobs run-now con file_path
# Polling → get-output → formatea JSON en viñetas y responde en Telegram
# (opcional) guarda histórico en SQLite si está disponible
# ─────────────────────────────────────────────────────────────────────────────

# (imports únicos)
import os, json, base64, asyncio, requests
from pathlib import Path
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler, MessageHandler, filters
from formatters.audit import fmt_summary
DBFS_BASE = os.getenv("DATABRICKS_DBFS_BASE", "").rstrip("/")

# Constantes + cfg únicas
TASK_KEY = os.getenv("DATABRICKS_TASK_KEY", "")
MAX_SIZE = 15_000_000

def _cfg():
    host  = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN") or ""
    job   = int(os.getenv("DATABRICKS_JOB_ID_AUDIT", "0") or "0")
    return host, token, job


def _h(token: str):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def _url(host: str, p: str) -> str:
    return f"{host}{p}"

# ===== DBFS helpers (upload por bloques) =====================================
import base64 as _b64

def _dbfs_create_sync(path: str, host: str, token: str, overwrite: bool = True) -> int:
    r = requests.post(
        f"{host}/api/2.0/dbfs/create",
        headers=_h(token),
        json={"path": path, "overwrite": overwrite},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["handle"]

def _dbfs_add_block_sync(handle: int, data_bytes: bytes, host: str, token: str):
    chunk_b64 = _b64.b64encode(data_bytes).decode("utf-8")
    r = requests.post(
        f"{host}/api/2.0/dbfs/add-block",
        headers=_h(token),
        json={"handle": handle, "data": chunk_b64},
        timeout=60,
    )
    r.raise_for_status()

def _dbfs_close_sync(handle: int, host: str, token: str):
    r = requests.post(
        f"{host}/api/2.0/dbfs/close",
        headers=_h(token),
        json={"handle": handle},
        timeout=60,
    )
    r.raise_for_status()

def _dbfs_upload_sync(path: str, data: bytes, host: str, token: str, chunk_size: int = 1024 * 1024):
    """Sube 'data' a DBFS en bloques (~1 MB)."""
    h = _dbfs_create_sync(path, host, token, overwrite=True)
    try:
        for i in range(0, len(data), chunk_size):
            _dbfs_add_block_sync(h, data[i:i + chunk_size], host, token)
    finally:
        _dbfs_close_sync(h, host, token)

# ===== Jobs API helpers =======================================================
def _run_now_sync_with_path(job_id: int, file_path: str, host: str, token: str) -> int:
    """Lanza el Job pasando 'file_path' (dbfs:/...) como parámetro de notebook."""
    payload = {"job_id": job_id, "notebook_params": {"file_path": file_path}}
    r = requests.post(
        f"{host}/api/2.2/jobs/run-now",
        headers=_h(token),
        json=payload,
        timeout=60,
    )
    if r.status_code >= 400:
        raise requests.HTTPError(
            f"{r.status_code} {r.reason}: {r.text}  (host={host}, job_id={job_id})"
        )
    return r.json()["run_id"]

def _get_state_sync(run_id: int, host: str, token: str) -> dict:
    r = requests.get(_url(host, "/api/2.2/jobs/runs/get"), headers=_h(token), params={"run_id": run_id}, timeout=60)
    r.raise_for_status()
    return r.json()["state"]

def _get_output_sync(run_id: int, host: str, token: str) -> str:
    r = requests.get(_url(host, "/api/2.1/jobs/runs/get-output"), headers=_h(token), params={"run_id": run_id}, timeout=60)
    r.raise_for_status()
    return (r.json().get("notebook_output") or {}).get("result", "") or ""

def _get_run_sync(run_id: int, host: str, token: str) -> dict:
    r = requests.get(f"{host}/api/2.1/jobs/runs/get", headers=_h(token), params={"run_id": run_id}, timeout=60)
    r.raise_for_status()
    return r.json()

def _list_task_run_ids_sync(parent_run_id: int, host: str, token: str) -> list[int]:
    """Devuelve los run_id de tareas si el run es 'parent' (multi-task); si no, lista vacía."""
    data = _get_run_sync(parent_run_id, host, token)
    tasks = data.get("tasks") or []
    return [t.get("run_id") for t in tasks if t.get("run_id")]

# ----------



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



MAX_SIZE = 15_000_000

# ---------- Handlers ----------
# Persistencia opcional (SQLite)
try:
    from db import sqlite_store as store
    _HAS_STORE = True
except Exception:
    _HAS_STORE = False

# ===== Handlers ===============================================================

async def audit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    host, token, job_id = _cfg()
    if not (host and token and job_id):
        return await update.message.reply_text(
            "⚠️ Configura DATABRICKS_HOST, DATABRICKS_TOKEN y DATABRICKS_JOB_ID_AUDIT en .env"
        )
    await update.message.reply_text(
        "🔎 Auditoría contable: envíame tu Excel (.xlsx) como *documento*.\n"
        "Validaré fechas, duplicados, desbalances y campos obligatorios. ⚖️"
    )

async def audit_doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # 0) Config y validaciones iniciales
        host, token, job_id = _cfg()
        if not (host and token and job_id):
            return await update.message.reply_text("⚠️ Config de Databricks incompleta. Revisa .env")

        doc = update.message.document
        if not doc or not doc.file_name.lower().endswith(".xlsx"):
            return await update.message.reply_text("⚠️ Necesito un Excel .xlsx (envíalo como *documento*).")

        await update.message.reply_text("📥 Recibido. Descargando archivo…")
        tmp_dir = Path("tmp")
        tmp_dir.mkdir(exist_ok=True)
        local_path = tmp_dir / f"{update.effective_chat.id}_{doc.file_name}"
        tg_file = await context.bot.get_file(doc.file_id)
        await tg_file.download_to_drive(str(local_path))

        data = local_path.read_bytes()
        if len(data) > MAX_SIZE:
            return await update.message.reply_text(
                f"⚠️ Archivo demasiado grande para esta demo (máx ~{MAX_SIZE//1_000_000} MB)."
            )

        # 1) Subir a DBFS y lanzar Job con file_path (evita límite 10KB en notebook_params)
        dbfs_rel_path = f"/tmp/bot_audit/{update.effective_chat.id}_{doc.file_name}"
        dbfs_uri = f"dbfs:{dbfs_rel_path}"
        await update.message.reply_text(f"📤 Subiendo a DBFS…\n{dbfs_rel_path}")
        try:
            await asyncio.to_thread(_dbfs_upload_sync, dbfs_rel_path, data, host, token)
        except Exception as e:
            return await update.message.reply_text(f"❌ Error subiendo a DBFS: {e}")

        await update.message.reply_text("🔐 Lanzando auditoría en Databricks…")
        try:
            run_id = await asyncio.to_thread(_run_now_sync_with_path, job_id, dbfs_uri, host, token)
        except Exception as e:
            return await update.message.reply_text(f"❌ Error al lanzar Job: {e}")

        run_url = f"{host}/jobs/runs/{run_id}"
        await update.message.reply_text(
            f"🚀 Ejecutando auditoría…\nJob: {job_id}\nHost: {host}\nrun_id={run_id}\n{run_url}"
        )

        # 2) Polling del estado del run (parent)
        max_secs = 600
        interval = 5
        waited = 0
        final_state = None

        while waited < max_secs:
            state = await asyncio.to_thread(_get_state_sync, run_id, host, token)
            life = state.get("life_cycle_state")
            if life in {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}:
                final_state = state
                break
            if waited in (0, 30, 60):
                await update.message.reply_text(f"⏳ Estado: {life or 'N/A'}…")
            await asyncio.sleep(interval)
            waited += interval

        if not final_state:
            return await update.message.reply_text(
                "⏱️ Timeout: la ejecución sigue en curso. Vuelve a intentarlo en unos minutos."
            )

        # 3) Obtener output (soporta parent-run y task-runs)
        output = ""
        task_run_ids = []
        for attempt in range(24):  # ~120s extra
            # 3.1 intenta directo en el run_id recibido (si el job tiene 1 task, a veces ya es task-run)
            output = await asyncio.to_thread(_get_output_sync, run_id, host, token)
            if output:
                break

            # 3.2 si no hay, intenta en las task-runs del parent
            if not task_run_ids:
                try:
                    task_run_ids = await asyncio.to_thread(_list_task_run_ids_sync, run_id, host, token)
                except Exception:
                    task_run_ids = []

            if task_run_ids:
                for tr in task_run_ids:
                    output = await asyncio.to_thread(_get_output_sync, tr, host, token)
                    if output:
                        break
                if output:
                    break

            await asyncio.sleep(5)

        if not output:
            status = f"{final_state.get('life_cycle_state')}/{final_state.get('result_state')}"
            extra = f"\n(run_id job={run_id}, task_runs={task_run_ids or 'N/A'})"
            return await update.message.reply_text(
                "⚠️ No pude leer la salida del Job todavía.\n"
                f"Estado final: {status}{extra}\n"
                "Asegúrate de que el notebook termine con dbutils.notebook.exit(JSON)."
            )

        # 4) Formatear JSON y responder
        try:
            summary = json.loads(output) if isinstance(output, str) else output
        except Exception as e:
            return await update.message.reply_text(
                f"⚠️ Salida no válida (no es JSON parseable): {e}\n\nOutput crudo:\n{output[:2000]}"
            )

        text = fmt_summary(summary)
        await update.message.reply_markdown(text)

        # 5) Guardado opcional en SQLite
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
        # fallback
        await update.message.reply_text(f"❌ Error inesperado en audit_doc: {e}")

def register_handlers(app):
    app.add_handler(CommandHandler("audit", audit_cmd))
    # Excel por MIME y por extensión
    app.add_handler(
        MessageHandler(
            filters.Document.MimeType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            audit_doc,
        )
    )
    app.add_handler(MessageHandler(filters.Document.FileExtension("xlsx"), audit_doc))
