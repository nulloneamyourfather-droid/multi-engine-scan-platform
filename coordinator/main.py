"""Coordinator entrypoint.

Usage:
    uvicorn coordinator.main:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import os

from coordinator.api import build_app
from storage.sqlite import SQLiteStore

DB_PATH = os.environ.get("SCAN_DB_PATH", "coordinator.db")
SCHEDULER_INTERVAL = float(os.environ.get("SCAN_SCHEDULER_INTERVAL", "15"))

store = SQLiteStore(DB_PATH)
app = build_app(store, scheduler_interval=SCHEDULER_INTERVAL)
