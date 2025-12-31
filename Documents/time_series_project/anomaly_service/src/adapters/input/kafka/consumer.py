import json
import logging
from aiokafka import AIOKafkaConsumer
from src.adapters.input.kafka.message_handler import PreprocessingCompletedHandler

logger = logging.getLogger(__name__)


class AnomalyKafkaConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str, handler: PreprocessingCompletedHandler):
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
        )
        self.handler = handler

    async def start(self):
        await self.consumer.start()
        logger.info("Kafka consumer started")

        try:
            async for msg in self.consumer:
                await self.handler.handle(msg.value)
        finally:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")
