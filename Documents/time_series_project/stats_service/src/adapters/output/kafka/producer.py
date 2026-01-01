import json
import logging
import asyncio
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

logger = logging.getLogger(__name__)


class StatsKafkaProducer:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None

    async def start(self):
        while True:
            try:
                self._producer = AIOKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                await self._producer.start()
                logger.info("Stats Kafka producer connected")
                break
            except KafkaConnectionError:
                logger.warning(
                    "Kafka not ready for stats producer, retrying in 5 seconds..."
                )
                await asyncio.sleep(5)

    async def publish(self, topic: str, message: dict):
        if not self._producer:
            logger.warning("Stats producer not ready, message skipped")
            return

        await self._producer.send_and_wait(topic, message)

    async def stop(self):
        if self._producer:
            await self._producer.stop()
