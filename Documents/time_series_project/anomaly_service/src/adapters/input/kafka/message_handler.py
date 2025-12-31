import logging
from sqlalchemy.orm import Session

from src.adapters.db import SessionLocal
from src.adapters.repository import fetch_series_values
from src.application.anomaly_use_case import DetectAnomaliesUseCase
from src.adapters.output.kafka.producer import AnomalyKafkaProducer

logger = logging.getLogger(__name__)


class PreprocessingCompletedHandler:
    def __init__(self, producer: AnomalyKafkaProducer):
        self.producer = producer
        self.output_topic = "data.anomaly.completed"

    async def handle(self, message: dict):
        series_id = message.get("series_id")
        if not series_id:
            logger.warning("Message missing series_id")
            return

        logger.info(f"Received preprocessing completed for series_id={series_id}")

        db: Session = SessionLocal()
        try:
            _, values = fetch_series_values(
                db=db,
                series_id=series_id,
                column="close",
                limit=500,
            )

            if not values:
                logger.warning(f"No values found for series_id={series_id}")
                return

            use_case = DetectAnomaliesUseCase()
            anomaly_indices, _ = use_case.execute(values)

            await self.producer.publish(
                self.output_topic,
                {
                    "series_id": series_id,
                    "status": "done",
                    "anomalies_found": len(anomaly_indices),
                },
            )

            logger.info(f"Anomaly detection completed for {series_id}")

        finally:
            db.close()

