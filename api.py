
import os
import secrets
import logging
import threading
import uuid
from datetime import date
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.graph.trading_graph import TradingAgentsGraph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tradingagents-api")

app = FastAPI(title="TradingAgents Research API", version="1.0.0")

executor = ThreadPoolExecutor(max_workers=1)
jobs = {}
jobs_lock = threading.Lock()


class AnalysisRequest(BaseModel):
    ticker: str = Field(
        min_length=1,
        max_length=24,
        pattern=r"^[A-Za-z0-9.^=_-]+$"
    )
    trade_date: date


def authenticate(x_api_key):
    expected = os.getenv("API_ACCESS_TOKEN", "")
    if not expected:
        raise HTTPException(503, "API authentication is not configured")
    if not x_api_key or not secrets.compare_digest(x_api_key, expected):
        raise HTTPException(401, "Invalid API key")


def run_analysis(job_id, request):
    try:
        config = DEFAULT_CONFIG.copy()
        graph = TradingAgentsGraph(debug=False, config=config)
        state, decision = graph.propagate(
            request.ticker.upper(),
            request.trade_date.isoformat()
        )

        result = {
            "ticker": request.ticker.upper(),
            "trade_date": request.trade_date.isoformat(),
            "decision": str(decision),
            "research_only": True,
            "live_orders_enabled": False
        }

        with jobs_lock:
            jobs[job_id] = {"status": "completed", "result": result}

    except Exception as exc:
        logger.exception("Analysis failed")
        with jobs_lock:
            jobs[job_id] = {
                "status": "failed",
                "error": type(exc).__name__
            }


@app.get("/health")
def health():
    return {"status": "ok", "service": "tradingagents-api"}


@app.post("/analyze", status_code=202)
def analyze(
    request: AnalysisRequest,
    x_api_key: str | None = Header(default=None)
):
    authenticate(x_api_key)

    if request.trade_date > date.today():
        raise HTTPException(422, "Date cannot be in the future")

    with jobs_lock:
        if sum(j["status"] in ("queued", "running") for j in jobs.values()) >= 5:
            raise HTTPException(429, "Analysis queue is full")

        job_id = str(uuid.uuid4())
        jobs[job_id] = {"status": "queued"}

    def worker():
        with jobs_lock:
            jobs[job_id] = {"status": "running"}
        run_analysis(job_id, request)

    executor.submit(worker)
    return {"job_id": job_id, "status": "queued"}


@app.get("/jobs/{job_id}")
def get_job(
    job_id: str,
    x_api_key: str | None = Header(default=None)
):
    authenticate(x_api_key)
    with jobs_lock:
        job = jobs.get(job_id)

    if job is None:
        raise HTTPException(404, "Job not found")

    return {"job_id": job_id, **job}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
