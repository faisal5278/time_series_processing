import asyncio
import os

from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

from src.api.schemas import (
    AnomalyRequest,
    AnomalyResponse,
    AnomalyFromDBRequest,
)
from src.application.anomaly_use_case import DetectAnomaliesUseCase
from src.adapters.db import get_db
from src.adapters.repository import fetch_series_values

# Kafka consumer (runs in background)
from src.adapters.input.kafka.consumer import AnomalyKafkaConsumer

app = FastAPI(title="Anomaly Detection Service")


@app.on_event("startup")
async def startup_event():
    """
    Start Kafka consumer in the background.
    If Kafka env vars are not set, it will skip safely.
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.getenv("KAFKA_TOPIC")
    group_id = os.getenv("KAFKA_GROUP_ID")

    if not bootstrap_servers or not topic or not group_id:
        print("Kafka not configured — skipping Kafka consumer startup")
        return

    consumer = AnomalyKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
    )
    asyncio.create_task(consumer.start())


@app.get("/health")
def health():
    return {"status": "ok", "service": "anomaly_service"}


@app.get("/")
def root():
    return {"message": "Anomaly Detection Service is running"}


@app.post("/detect", response_model=AnomalyResponse)
def detect_anomalies(payload: AnomalyRequest):
    """
    Detect anomalies using a simple Z-score method
    from values provided directly in the request body.
    """
    use_case = DetectAnomaliesUseCase()
    anomaly_indices, z_scores = use_case.execute(payload.values)

    return AnomalyResponse(
        anomaly_indices=anomaly_indices,
        z_scores=z_scores,
    )


@app.post("/detect_from_db", response_model=AnomalyResponse)
def detect_anomalies_from_db(
    payload: AnomalyFromDBRequest,
    db: Session = Depends(get_db),
):
    """
    Detect anomalies using values fetched from TimescaleDB
    (time_series_preprocessed table).
    """
    _, values = fetch_series_values(
        db=db,
        series_id=payload.series_id,
        column=payload.column,
        limit=payload.limit,
    )

    use_case = DetectAnomaliesUseCase()
    anomaly_indices, z_scores = use_case.execute(values)

    return AnomalyResponse(
        anomaly_indices=anomaly_indices,
        z_scores=z_scores,
    )
