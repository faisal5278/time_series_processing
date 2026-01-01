import json
import logging
import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from src.adapters.input.kafka.message_handler import PreprocessingCompletedStatsHandler

logger = logging.getLogger(__name__)


class StatsKafkaConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        handler: PreprocessingCompletedStatsHandler,
    ):
        self.topic = topic
        self.handler = handler
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
        )

    async def start(self):
        while True:
            try:
                await self.consumer.start()
                logger.info("Stats Kafka consumer connected")
                break
            except KafkaConnectionError:
                logger.warning(
                    "Kafka not ready for stats consumer, retrying in 5 seconds..."
                )
                await asyncio.sleep(5)

        try:
            async for msg in self.consumer:
                await self.handler.handle(msg.value)
        finally:
            await self.consumer.stop()
            logger.info("Stats Kafka consumer stopped")

    async def stop(self):
        await self.consumer.stop()

