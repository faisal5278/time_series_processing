import asyncio
import os
from fastapi import FastAPI

from src.adapters.output.kafka.producer import StatsKafkaProducer
from src.adapters.input.kafka.consumer import StatsKafkaConsumer
from src.adapters.input.kafka.message_handler import PreprocessingCompletedStatsHandler

app = FastAPI(title="Statistical Analysis Service")

producer: StatsKafkaProducer | None = None
consumer: StatsKafkaConsumer | None = None


@app.on_event("startup")
async def startup_event():
    global producer, consumer

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    producer = StatsKafkaProducer(bootstrap_servers)
    handler = PreprocessingCompletedStatsHandler(producer)

    consumer = StatsKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic="data.preprocessing.completed",
        group_id="stats-service-group",
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
    return {"status": "ok", "service": "stats_service"}
