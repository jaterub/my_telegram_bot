# scripts/dbx_list_jobs.py
import os, requests
from dotenv import load_dotenv

load_dotenv()
HOST  = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN") or ""

def _h():
    return {"Authorization": f"Bearer {TOKEN}"}

if not HOST or not TOKEN:
    raise RuntimeError("Faltan DATABRICKS_HOST o DATABRICKS_TOKEN en .env")

r = requests.get(f"{HOST}/api/2.1/jobs/list", headers=_h(), timeout=30)
r.raise_for_status()
jobs = r.json().get("jobs", [])
print(f"Jobs visibles por este token: {len(jobs)}")
for j in jobs:
    print(f"- ID={j.get('job_id')}  name={j.get('settings',{}).get('name')}")
