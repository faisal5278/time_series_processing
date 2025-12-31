from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

from src.api.schemas import (
    StatsRequest,
    StatsResponse,
    StatsFromDBRequest,
)
from src.domain.stats_service import compute_basic_stats
from src.adapters.db import get_db
from src.adapters.repository import fetch_series_values

app = FastAPI(title="Statistical Analysis Service")


@app.get("/health")
def health():
    return {"status": "ok", "service": "stats_service"}


@app.get("/")
def root():
    return {"message": "Statistical Analysis Service is running"}


@app.post("/stats", response_model=StatsResponse)
def calculate_stats(payload: StatsRequest):
    """
    Compute basic statistics from values provided directly in the request body.
    """
    stats = compute_basic_stats(payload.values)
    return StatsResponse(**stats)


@app.post("/stats_from_db", response_model=StatsResponse)
def calculate_stats_from_db(
    payload: StatsFromDBRequest,
    db: Session = Depends(get_db),
):
    """
    Compute basic statistics using values fetched from TimescaleDB
    (time_series_preprocessed).
    """
    _, values = fetch_series_values(
        db=db,
        series_id=payload.series_id,
        column=payload.column,
        limit=payload.limit,
    )

    stats = compute_basic_stats(values)
    return StatsResponse(**stats)
