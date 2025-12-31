import logging
from sqlalchemy.orm import Session

from src.adapters.db import SessionLocal
from src.adapters.repository import fetch_series_values
from src.application.anomaly_use_case import DetectAnomaliesUseCase
from src.adapters.output.kafka.producer import AnomalyKafkaProducer

logger = logging.getLogger(__name__)


class PreprocessingCompletedHandler:
    def __init__(self, producer: AnomalyKafkaProducer, output_topic: str):
        self.producer = producer
        self.output_topic = output_topic

    async def handle(self, message: dict):
        """
        Expected message example from preprocessing:
        {
          "series_id": "sensor_123"
        }
        """
        series_id = message.get("series_id")
        if not series_id:
            logger.warning("Message missing series_id")
            return

        logger.info(f"Preprocessing completed for series_id={series_id}. Running anomaly detection...")

        db: Session = SessionLocal()
        try:
            _, values = fetch_series_values(
                db=db,
                series_id=series_id,
                column="close",
                limit=500
            )

            if not values:
                logger.warning(f"No values found for series_id={series_id}")
                return

            use_case = DetectAnomaliesUseCase()
            anomaly_indices, z_scores = use_case.execute(values)

            anomalies_found = len(anomaly_indices)
            logger.info(f"Anomaly detection done for {series_id}. Found {anomalies_found} anomalies")

            # ✅ Publish "done" message
            await self.producer.publish(
                self.output_topic,
                {
                    "series_id": series_id,
                    "status": "done",
                    "anomalies_found": anomalies_found,
                }
            )

        finally:
            db.close()
