import random
import time
import uuid
from datetime import datetime, timedelta
from collections import deque
import requests

# =========================
# CONFIG
# =========================

API_URL = "http://localhost:8000/events"

EVENTS_PER_MINUTE = 30
RUN_DURATION_SECONDS = 60

BACKFILL_RATIO = 0.2

FAILURE_RATE = 0.15        # 15% of requests fail
DUPLICATE_RATE = 0.10      # 10% events are duplicates

MAX_RETRY_ATTEMPTS = 3

USERS = list(range(100, 200))
COUNTRIES = ["US", "CA", "GB", "DE", "FR"]

PAGES = ["/home", "/search", "/product", "/checkout", "/pricing", "/account"]

SEARCH_QUERIES = [
    "data engineering jobs",
    "spark parquet tutorial",
    "airflow dag example",
    "dbt incremental model",
    "fastapi ingestion api"
]

ERROR_MESSAGES = [
    "TimeoutError",
    "NullPointerException",
    "SchemaMismatchError",
    "ConnectionRefused"
]

retry_queue = deque()

# Store recent events for duplication simulation
recent_events = deque(maxlen=50)


# =========================
# EVENT GENERATION
# =========================

def generate_event_ts(late=False):
    base_time = datetime.utcnow()

    if late:
        delay_minutes = random.randint(30, 24 * 60)
        event_time = base_time - timedelta(minutes=delay_minutes)
    else:
        event_time = base_time

    return event_time.isoformat()


def base_event(event_type: str, late=False, event_id=None):
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": generate_event_ts(late=late),
        "user_id": random.choice(USERS),
        "country": random.choice(COUNTRIES),
    }


def generate_page_view(late=False, event_id=None):
    e = base_event("page_view", late, event_id)
    e["page"] = random.choice(PAGES)
    return e


def generate_search(late=False, event_id=None):
    e = base_event("search", late, event_id)
    e["query"] = random.choice(SEARCH_QUERIES)
    return e


def generate_purchase(late=False, event_id=None):
    e = base_event("purchase", late, event_id)
    e["amount"] = round(random.uniform(5, 250), 2)
    e["currency"] = "USD"
    return e


def generate_error(late=False, event_id=None):
    e = base_event("error", late, event_id)
    e["error_message"] = random.choice(ERROR_MESSAGES)
    e["severity"] = random.choice(["low", "medium", "high"])
    return e


EVENT_GENERATORS = [
    generate_page_view,
    generate_search,
    generate_purchase,
    generate_error,
]


# =========================
# FAILURE SIMULATION
# =========================

def simulate_network_failure():
    return random.random() < FAILURE_RATE


# =========================
# RETRY LOGIC
# =========================

def send_event(event, attempt=1):
    try:
        # Simulate random API failure BEFORE request
        if simulate_network_failure():
            raise Exception("Simulated network failure")

        response = requests.post(API_URL, json=event, timeout=3)

        if response.status_code != 200:
            raise Exception(f"Bad response: {response.status_code} {response.text}")

        print(f"[OK] {event['event_type']} sent (attempt {attempt})")
        return True

    except Exception as e:
        print(f"[FAIL] attempt {attempt}: {e}")

        if attempt < MAX_RETRY_ATTEMPTS:
            retry_queue.append((event, attempt + 1))

        return False


def process_retry_queue():
    for _ in range(len(retry_queue)):
        event, attempt = retry_queue.popleft()

        time.sleep(min(2 ** attempt, 10))
        send_event(event, attempt=attempt)


# =========================
# DUPLICATION LOGIC
# =========================

def maybe_duplicate(event):
    """
    Occasionally resend a previously seen event.
    """
    if recent_events and random.random() < DUPLICATE_RATE:
        duplicate = random.choice(list(recent_events))

        # clone exact event (same event_id = intentional duplicate)
        dup_event = dict(duplicate)

        print(f"[DUPLICATE] Re-sending event_id={dup_event['event_id']}")
        return dup_event

    return event


# =========================
# STREAM GENERATION
# =========================

def generate_event():
    fn = random.choice(EVENT_GENERATORS)

    is_late = random.random() < BACKFILL_RATIO
    event = fn(late=is_late)

    # store for duplication simulation
    recent_events.append(event)

    return event


def run_stream():
    start_time = time.time()
    event_count = 0

    print("Starting telemetry stream with failures + duplicates...")

    while time.time() - start_time < RUN_DURATION_SECONDS:

        # 1. Generate event
        event = generate_event()

        # 2. Introduce duplication behavior
        event = maybe_duplicate(event)

        # 3. Send event
        send_event(event)

        event_count += 1

        # 4. Retry queue processing
        if len(retry_queue) > 0 and event_count % 5 == 0:
            print(f"[INFO] Processing retry queue ({len(retry_queue)} items)")
            process_retry_queue()

        time.sleep(60 / EVENTS_PER_MINUTE)

    # final flush
    if retry_queue:
        print(f"[INFO] Final retry flush ({len(retry_queue)} items)")
        process_retry_queue()

    print(f"Done. Sent ~{event_count} events.")


if __name__ == "__main__":
    run_stream()