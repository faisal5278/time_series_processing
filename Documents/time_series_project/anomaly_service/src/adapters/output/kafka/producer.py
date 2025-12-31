import json
import logging
from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)


class AnomalyKafkaProducer:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer: AIOKafkaProducer | None = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self.producer.start()
        logger.info("Kafka producer started")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")

    async def publish(self, topic: str, message: dict):
        if not self.producer:
            raise RuntimeError("Producer not started")
        await self.producer.send_and_wait(topic, message)
        logger.info(f"Published message to {topic}: {message}")
