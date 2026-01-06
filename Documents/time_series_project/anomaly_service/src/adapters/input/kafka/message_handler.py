import logging
from sqlalchemy.orm import Session

from src.adapters.db import SessionLocal
from src.adapters.repository import fetch_series_values
from src.application.anomaly_use_case import DetectAnomaliesUseCase
from src.adapters.output.kafka.producer import AnomalyKafkaProducer

from src.shared.job_tracker import JobTracker
from shared import SimpleJobTracker

logger = logging.getLogger(__name__)


class PreprocessingCompletedHandler:
    def __init__(self, producer: AnomalyKafkaProducer, tracker: JobTracker):
        self.producer = producer
        self.tracker = tracker
        self.output_topic = "data.anomaly.completed"

    async def handle(self, message: dict):
        """
        Expected message example:
        {"series_id": "sensor_123"}
        """
        series_id = message.get("series_id")
        job_id = message.get("job_id")
        if not series_id:
            logger.warning("Message missing series_id")
            return

        logger.info(f"Received preprocessing completed for series_id={series_id}")


        # Mark forecasting as started
        SimpleJobTracker.update_status(
            job_id = job_id,
            series_id=series_id,
            status='running',
            stage='anomaly_service'
        )

      

        self.tracker.write_status(series_id=series_id, status="started")

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

                # ✅ status: no_data
                self.tracker.write_status(
                    series_id=series_id,
                    status="no_data",
                    details={"reason": "No values found in time_series_preprocessed"},
                )
                return

            use_case = DetectAnomaliesUseCase()
            anomaly_indices, _ = use_case.execute(values)

            # ✅ publish anomaly result message
            await self.producer.publish(
                self.output_topic,
                {
                    "series_id": series_id,
                    "status": "done",
                    "anomalies_found": len(anomaly_indices),
                },
            )

            # ✅ status: done
            self.tracker.write_status(
                series_id=series_id,
                status="done",
                anomalies_found=len(anomaly_indices),
            )
            

            SimpleJobTracker.update_status(
                job_id = job_id,
                series_id=series_id,
                status='completed',
                stage='anomaly_service'
            )

            logger.info(f"Anomaly detection completed for {series_id}")

        except Exception as e:
            logger.error(f"Error processing series_id={series_id}: {e}", exc_info=True)

            # ✅ status: failed
            self.tracker.write_status(
                series_id=series_id,
                status="failed",
                details={"error": str(e)},
            )


            SimpleJobTracker.update_status(
            job_id = job_id,
            series_id=series_id,
            status='failed',
            stage='anomaly_service'
            )

        finally:
            db.close()
