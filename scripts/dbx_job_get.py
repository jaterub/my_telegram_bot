import os, requests
from dotenv import load_dotenv
load_dotenv()
HOST=(os.getenv("DATABRICKS_HOST") or "").rstrip("/")
TOKEN=os.getenv("DATABRICKS_TOKEN") or ""
JOBID=os.getenv("DATABRICKS_JOB_ID_AUDIT") or ""
if not (HOST and TOKEN and JOBID):
    raise RuntimeError("Falta HOST/TOKEN/JOBID en .env")
r=requests.get(f"{HOST}/api/2.1/jobs/get",
               headers={"Authorization":f"Bearer {TOKEN}"},
               params={"job_id": JOBID}, timeout=30)
print("STATUS:", r.status_code)
print(r.text[:1500])
