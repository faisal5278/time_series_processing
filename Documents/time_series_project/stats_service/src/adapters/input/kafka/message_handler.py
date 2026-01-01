'''
import logging
from sqlalchemy.orm import Session

from src.adapters.db import SessionLocal
from src.adapters.repository import fetch_series_values
from src.application.stats_use_case import StatsUseCase
from src.adapters.output.kafka.producer import StatsKafkaProducer

logger = logging.getLogger(__name__)


class PreprocessingCompletedStatsHandler:
    def __init__(self, producer: StatsKafkaProducer):
        self.producer = producer
        self.output_topic = "data.stats.completed"

    async def handle(self, message: dict):
        series_id = message.get("series_id")
        if not series_id:
            logger.warning("Stats handler: message missing series_id")
            return

        logger.info(f"Stats: preprocessing completed for series_id={series_id}")

        db: Session = SessionLocal()
        try:
            _, values = fetch_series_values(
                db=db,
                series_id=series_id,
                column="close",   # same column as anomaly
                limit=500,
            )

            if not values:
                logger.warning(f"Stats: no values found for series_id={series_id}")
                return

            use_case = StatsUseCase()
            stats = use_case.execute(values)

            await self.producer.publish(
                self.output_topic,
                {
                    "series_id": series_id,
                    **stats,
                },
            )

            logger.info(f"Stats completed for series_id={series_id}")

        finally:
            db.close()
'''



import logging

from src.application.stats_use_case import StatsUseCase
from src.adapters.output.kafka.producer import StatsKafkaProducer

logger = logging.getLogger(__name__)


class PreprocessingCompletedStatsHandler:
    def __init__(self, producer: StatsKafkaProducer):
        self.producer = producer
        self.output_topic = "data.stats.completed"

    async def handle(self, message: dict):
        """
        TEMPORARY TEST VERSION
        ----------------------
        This version BYPASSES the database completely.
        It always computes stats and always publishes,
        to prove the Kafka pipeline end-to-end.
        """

        series_id = message.get("series_id")
        if not series_id:
            logger.warning("Stats handler: message missing series_id")
            return

        logger.info(
            "Stats: preprocessing completed for series_id=%s (DB bypass enabled)",
            series_id,
        )

        #  TEMPORARY TEST — bypass DB completely
        values = [1, 2, 100, 2, 1]

        # Compute stats
        use_case = StatsUseCase()
        stats = use_case.execute(values)

        # Publish result
        await self.producer.publish(
            self.output_topic,
            {
                "series_id": series_id,
                **stats,
            },
        )

        logger.info(
            "Stats completed and published for series_id=%s (DB bypass)",
            series_id,
        )
