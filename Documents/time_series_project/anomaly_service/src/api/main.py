import asyncio
import os
from fastapi import FastAPI

from src.adapters.output.kafka.producer import AnomalyKafkaProducer
from src.adapters.input.kafka.consumer import AnomalyKafkaConsumer
from src.adapters.input.kafka.message_handler import PreprocessingCompletedHandler
from src.shared.job_tracker import JobTracker

app = FastAPI(title="Anomaly Detection Service")

producer: AnomalyKafkaProducer | None = None
consumer: AnomalyKafkaConsumer | None = None
tracker: JobTracker | None = None

@app.on_event("startup")
async def startup_event():
    global producer, consumer, tracker

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    # ✅ Init status tracker with separate status database
    status_db_url = os.getenv("STATUS_DATABASE_URL")
    tracker = JobTracker(
        status_db_url=status_db_url,
        table_name="anomaly_job_status"
    )
    tracker.create_tables()

    producer = AnomalyKafkaProducer(bootstrap_servers)
    handler = PreprocessingCompletedHandler(producer, tracker)

    consumer = AnomalyKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic="data.preprocessing.completed",
        group_id="anomaly-service-group",
        handler=handler,
    )

    asyncio.create_task(producer.start())
    asyncio.create_task(consumer.start())

@app.on_event("shutdown")
async def shutdown_event():
    if consumer:
        await consumer.stop()
    if producer:
        await producer.stop()

@app.get("/health")
def health():
    return {"status": "ok", "service": "anomaly_service"}
