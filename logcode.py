from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional, Dict
from datetime import datetime
from dateutil import parser
import os
import hashlib

LOG_DIR = "./logs"

app = FastAPI(title="Log File Analysis API")


# -------------------------
# Utility Functions
# -------------------------

def generate_log_id(raw_line: str) -> str:
    """Generate deterministic unique ID from log line."""
    return hashlib.sha1(raw_line.encode()).hexdigest()


def parse_log_line(line: str) -> Optional[Dict]:
    parts = line.strip().split("\t")
    if len(parts) != 4:
        return None

    timestamp, level, component, message = parts
    try:
        return {
            "id": generate_log_id(line),
            "timestamp": parser.parse(timestamp),
            "level": level,
            "component": component,
            "message": message,
        }
    except Exception:
        return None


def read_logs():
    """Generator that yields parsed log entries."""
    if not os.path.exists(LOG_DIR):
        return

    for file in os.listdir(LOG_DIR):
        if file.endswith(".log"):
            with open(os.path.join(LOG_DIR, file), "r", encoding="utf-8") as f:
                for line in f:
                    log = parse_log_line(line)
                    if log:
                        yield log


# -------------------------
# API Endpoints
# -------------------------

@app.get("/logs")
def get_logs(
    level: Optional[str] = Query(None),
    component: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
):
    logs = []

    try:
        start_dt = parser.parse(start_time) if start_time else None
        end_dt = parser.parse(end_time) if end_time else None
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid timestamp format")

    for log in read_logs():
        if level and log["level"] != level:
            continue
        if component and log["component"] != component:
            continue
        if start_dt and log["timestamp"] < start_dt:
            continue
        if end_dt and log["timestamp"] > end_dt:
            continue

        log_copy = log.copy()
        log_copy["timestamp"] = log_copy["timestamp"].isoformat()
        logs.append(log_copy)

    return {
        "count": len(logs),
        "logs": logs
    }


@app.get("/logs/stats")
def get_log_stats():
    total = 0
    level_counts = {}
    component_counts = {}

    for log in read_logs():
        total += 1
        level_counts[log["level"]] = level_counts.get(log["level"], 0) + 1
        component_counts[log["component"]] = component_counts.get(log["component"], 0) + 1

    return {
        "total_logs": total,
        "by_level": level_counts,
        "by_component": component_counts
    }


@app.get("/logs/{log_id}")
def get_log_by_id(log_id: str):
    for log in read_logs():
        if log["id"] == log_id:
            log["timestamp"] = log["timestamp"].isoformat()
            return log

    raise HTTPException(status_code=404, detail="Log entry not found")
