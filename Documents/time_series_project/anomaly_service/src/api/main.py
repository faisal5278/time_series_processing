import asyncio
import os
import logging
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

from src.api.schemas import AnomalyRequest, AnomalyResponse, AnomalyFromDBRequest
from src.application.anomaly_use_case import DetectAnomaliesUseCase
from src.adapters.db import get_db
from src.adapters.repository import fetch_series_values

from src.adapters.output.kafka.producer import AnomalyKafkaProducer
from src.adapters.input.kafka.message_handler import PreprocessingCompletedHandler
from src.adapters.input.kafka.consumer import AnomalyKafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Anomaly Detection Service")

producer: AnomalyKafkaProducer | None = None


@app.on_event("startup")
async def startup_event():
    global producer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    input_topic = os.getenv("KAFKA_TOPIC", "data.preprocessing.completed")
    group_id = os.getenv("KAFKA_GROUP_ID", "anomaly-service-group")
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "data.anomaly.completed")

    # Start producer
    producer = AnomalyKafkaProducer(bootstrap)
    await producer.start()

    # Build handler with producer
    handler = PreprocessingCompletedHandler(producer=producer, output_topic=output_topic)

    # Start consumer in background
    consumer = AnomalyKafkaConsumer(
        bootstrap_servers=bootstrap,
        topic=input_topic,
        group_id=group_id,
        handler=handler,
    )
    asyncio.create_task(consumer.start())

    logger.info(f"Anomaly service ready. Consuming: {input_topic} | Publishing: {output_topic}")


@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        await producer.stop()


@app.get("/")
def root():
    return {"message": "Anomaly Detection Service is running"}


@app.get("/health")
def health():
    return {"status": "ok", "service": "anomaly_service"}


@app.post("/detect", response_model=AnomalyResponse)
def detect_anomalies(payload: AnomalyRequest):
    use_case = DetectAnomaliesUseCase()
    anomaly_indices, z_scores = use_case.execute(payload.values)
    return AnomalyResponse(anomaly_indices=anomaly_indices, z_scores=z_scores)


@app.post("/detect_from_db", response_model=AnomalyResponse)
def detect_anomalies_from_db(payload: AnomalyFromDBRequest, db: Session = Depends(get_db)):
    _, values = fetch_series_values(
        db=db,
        series_id=payload.series_id,
        column=payload.column,
        limit=payload.limit,
    )

    use_case = DetectAnomaliesUseCase()
    anomaly_indices, z_scores = use_case.execute(values)
    return AnomalyResponse(anomaly_indices=anomaly_indices, z_scores=z_scores)
