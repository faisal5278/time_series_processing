import json
import logging
import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from src.adapters.input.kafka.message_handler import PreprocessingCompletedHandler

logger = logging.getLogger(__name__)

class AnomalyKafkaConsumer:
    def __init__(self, bootstrap_servers, topic, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.handler = PreprocessingCompletedHandler()
        self.consumer = None

    async def start(self):
        while True:
            try:
                logger.info("Starting Kafka consumer...")
                self.consumer = AIOKafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",
                )

                await self.consumer.start()
                logger.info("Kafka consumer connected successfully")

                async for msg in self.consumer:
                    await self.handler.handle(msg.value)

            except KafkaConnectionError as e:
                logger.warning("Kafka not ready yet, retrying in 5 seconds...")
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Kafka consumer error: {e}", exc_info=True)
                await asyncio.sleep(5)
