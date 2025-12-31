import logging
from sqlalchemy.orm import Session

from src.adapters.db import SessionLocal
from src.adapters.repository import fetch_series_values
from src.application.anomaly_use_case import DetectAnomaliesUseCase

logger = logging.getLogger(__name__)


class PreprocessingCompletedHandler:
    async def handle(self, message: dict):
        """
        Expected Kafka message:
        {
          "series_id": "sensor_123"
        }
        """

        series_id = message.get("series_id")
        if not series_id:
            logger.warning("Kafka message missing series_id")
            return

        logger.info(f"Preprocessing completed for series_id={series_id}")

        db: Session = SessionLocal()

        try:
            _, values = fetch_series_values(
                db=db,
                series_id=series_id,
                column="close",   # ✅ correct column
                limit=500
            )

            if not values:
                logger.warning(f"No values found for series_id={series_id}")
                return

            use_case = DetectAnomaliesUseCase()
            anomaly_indices, z_scores = use_case.execute(values)

            logger.info(
                f"Anomaly detection finished for {series_id}. "
                f"Found {len(anomaly_indices)} anomalies"
            )

        except Exception as e:
            logger.error(f"Error during anomaly detection: {e}", exc_info=True)

        finally:
            db.close()
