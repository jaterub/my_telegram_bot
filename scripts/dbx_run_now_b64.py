# scripts/dbx_run_now_b64.py
import os, time, base64, requests, json
from dotenv import load_dotenv

# 1) Carga .env
load_dotenv()
HOST  = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN") or ""
JOBID = int(os.getenv("DATABRICKS_JOB_ID_AUDIT", "0"))

def _h():
    return {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def _url(p: str) -> str:
    return f"{HOST}{p}"

def main():
    if not (HOST and TOKEN and JOBID):
        raise RuntimeError("Faltan DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_JOB_ID_AUDIT en .env")

    # 2) CSV de prueba pequeñito (una fecha inválida)
    csv_text = (
        "tx_id,date,account,debit,credit,desc\n"
        "1,2025-01-01,430,100,0,venta A\n"
        "1,2025-01-01,700,0,100,contrapartida A\n"
        "2,2025-13-05,430,50,0,fecha invalida\n"
    )
    b64 = base64.b64encode(csv_text.encode("utf-8")).decode("utf-8")

    # 3) run-now (con debug)
    print(f"HOST: {HOST}")
    print(f"JOBID: {JOBID}")

    payload = {"job_id": JOBID, "notebook_params": {"csv_b64": b64, "file_name": "mini.csv"}}
    r = requests.post(_url("/api/2.2/jobs/run-now"), headers=_h(), json=payload, timeout=60)

    if r.status_code != 200:
        print("STATUS:", r.status_code)
        print("REQUEST PAYLOAD:", json.dumps(payload)[:400])
        print("RESPONSE TEXT:", r.text[:1000])
        r.raise_for_status()

    run_id = r.json()["run_id"]  # ← IMPORTANTE: definir run_id
    run_url = f"{HOST}/jobs/runs/{run_id}"
    print(f"RUN lanzado: {run_id}\n{run_url}")

    # 4) Polling (hasta ~2 min)
    for _ in range(20):
        s = requests.get(_url("/api/2.2/jobs/runs/get"), headers=_h(), params={"run_id": run_id}, timeout=60)
        s.raise_for_status()
        state = s.json()["state"]
        life  = state.get("life_cycle_state")
        res   = state.get("result_state")
        print(f"Estado: life={life}, result={res}")
        if life in {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}:
            break
        time.sleep(6)

    # 5) Output JSON
    out = requests.get(_url("/api/2.1/jobs/runs/get-output"), headers=_h(), params={"run_id": run_id}, timeout=60)
    out.raise_for_status()
    notebook_output = out.json().get("notebook_output", {})
    result = notebook_output.get("result")

    print("\n=== RESUMEN JSON ===")
    if result:
        try:
            parsed = json.loads(result)
            print(json.dumps(parsed, ensure_ascii=False, indent=2))
        except Exception:
            print(result)
    else:
        print("(sin resultado)")
        print("Revisa el run:", run_url)

if __name__ == "__main__":
    main()
