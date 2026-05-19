from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from datetime import datetime
import os
import json
import uuid
import time
import random
import logging

# =========================
# APP SETUP
# =========================

app = FastAPI(title="Telemetry Ingestion API")

# =========================
# CONFIG
# =========================

RAW_EVENT_DIR = "data/raw/events"
LOG_DIR = "data/raw/logs"

FAILURE_RATE = 0.05  # simulate unstable ingestion

os.makedirs(RAW_EVENT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# =========================
# LOGGING (console)
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("ingestion-api")


# =========================
# EVENT MODEL
# =========================

class Event(BaseModel):
    event_id: str
    event_type: str
    event_ts: str
    user_id: int
    country: str

    # allow extra fields like page/query/amount/etc
    model_config = {"extra": "allow"}


# =========================
# HELPERS
# =========================

def write_event(event_dict: dict):
    path = os.path.join(
        RAW_EVENT_DIR,
        f"{event_dict['event_type']}_{event_dict['event_id']}.json"
    )

    with open(path, "w") as f:
        json.dump(event_dict, f)


def write_log(record: dict):
    date = datetime.utcnow().strftime("%Y-%m-%d")
    path = os.path.join(LOG_DIR, f"ingestion_logs_{date}.jsonl")

    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


# =========================
# MIDDLEWARE (REQUEST TRACING)
# =========================

@app.middleware("http")
async def trace_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())
    start_time = time.time()

    request.state.request_id = request_id

    base_log = {
        "request_id": request_id,
        "method": request.method,
        "path": request.url.path,
        "start_time": datetime.utcnow().isoformat(),
        "type": "request"
    }

    logger.info(f"[{request_id}] START {request.method} {request.url.path}")

    try:
        response = await call_next(request)

        duration_ms = round((time.time() - start_time) * 1000, 2)

        log_record = {
            **base_log,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
            "status": "success"
        }

        logger.info(
            f"[{request_id}] END {request.method} "
            f"{request.url.path} status={response.status_code} "
            f"duration_ms={duration_ms}"
        )

        write_log(log_record)

        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time-ms"] = str(duration_ms)

        return response

    except Exception as e:
        duration_ms = round((time.time() - start_time) * 1000, 2)

        log_record = {
            **base_log,
            "status_code": 500,
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(e)
        }

        logger.error(
            f"[{request_id}] ERROR {request.method} "
            f"{request.url.path} error={str(e)}"
        )

        write_log(log_record)

        raise


# =========================
# INGESTION ENDPOINT
# =========================

@app.post("/events")
def ingest_event(event: Event, request: Request):
    request_id = request.state.request_id

    start = time.time()

    # simulate transient failures
    if random.random() < FAILURE_RATE:
        logger.warning(f"[{request_id}] simulated failure event_id={event.event_id}")
        raise HTTPException(status_code=500, detail="Simulated ingestion failure")

    event_dict = event.model_dump()

    # ingestion metadata
    event_dict["ingested_at"] = datetime.utcnow().isoformat()
    event_dict["request_id"] = request_id

    # write raw event (bronze input)
    write_event(event_dict)

    duration_ms = round((time.time() - start) * 1000, 2)

    logger.info(
        f"[{request_id}] INGESTED event_id={event.event_id} "
        f"type={event.event_type} write_ms={duration_ms}"
    )

    # structured log (event-level)
    write_log({
        "type": "event_ingested",
        "request_id": request_id,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "write_ms": duration_ms,
        "ingested_at": event_dict["ingested_at"]
    })

    return {
        "status": "success",
        "event_id": event.event_id,
        "request_id": request_id
    }


# =========================
# HEALTH CHECK
# =========================

@app.get("/health")
def health():
    return {"status": "ok"}