from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
import uvicorn
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import os
from pydantic import BaseModel
from dotenv import load_dotenv
from monitor.monitor_drift_realtime import monitor_drift
from services.forecast import forecast_demand

# Load env
load_dotenv()

# DB connection
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT")
PG_DB = os.getenv("POSTGRES_DB")

pg_url = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(pg_url)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/monitor-drift")
def monitor():
    drift_result = monitor_drift()

    return FileResponse(drift_result["report_path"], media_type="text/html", filename="drift_report.html")


@app.post("/forecast-demand")
async def forecast(request: Request):
    try:
        payload = await request.json()
        if "data" not in payload:
            return JSONResponse(status_code=400, content={"error": "Missing 'data' field in JSON."})

        df = pd.DataFrame(payload["data"])

        return forecast_demand(df)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

if __name__ == "__main__":
    uvicorn.run(app, port=8000)
