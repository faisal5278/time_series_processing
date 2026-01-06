import logging
from sqlalchemy.orm import Session

from src.adapters.db import SessionLocal
from src.adapters.repository import fetch_series_values
from src.application.stats_use_case import StatsUseCase
from src.adapters.output.kafka.producer import StatsKafkaProducer
from src.shared.job_tracker import JobTracker
from shared import SimpleJobTracker

logger = logging.getLogger(__name__)


class PreprocessingCompletedStatsHandler:
    def __init__(self, producer: StatsKafkaProducer, tracker: JobTracker):
        self.producer = producer
        self.tracker = tracker
        self.output_topic = "data.stats.completed"

    async def handle(self, message: dict):
        series_id = message.get("series_id")
        job_id = message.get("job_id")
        if not series_id:
            logger.warning("Stats handler: message missing series_id")
            return

        logger.info(f"Stats: preprocessing completed for series_id={series_id}")

        # ✅ status: started
        self.tracker.write_status(series_id=series_id, status="started")

        SimpleJobTracker.update_status(
                job_id=job_id,
                series_id=series_id,
                status='running',
                stage='stats_service'
            )

        db: Session = SessionLocal()
        try:
            _, values = fetch_series_values(
                db=db,
                series_id=series_id,
                column="close",
                limit=500,
            )

            if not values:
                logger.warning(f"Stats: no values found for series_id={series_id}")

                # ✅ status: no_data
                self.tracker.write_status(
                    series_id=series_id,
                    status="no_data",
                    details={"reason": "No values found in time_series_preprocessed"},
                )
                return

            use_case = StatsUseCase()
            stats = use_case.execute(values)

            # ✅ publish stats result
            await self.producer.publish(
                self.output_topic,
                {
                    "series_id": series_id,
                    **stats,
                },
            )

            # ✅ status: done
            self.tracker.write_status(
                series_id=series_id,
                status="done",
                details={"stats_keys": list(stats.keys())},
            )

            SimpleJobTracker.update_status(
                job_id=job_id,
                series_id=series_id,
                status='completed',
                stage='stats_service'
            )

            logger.info(f"Stats completed for series_id={series_id}")

        except Exception as e:
            logger.error(f"Stats error for series_id={series_id}: {e}", exc_info=True)

            # ✅ status: failed
            self.tracker.write_status(
                series_id=series_id,
                status="failed",
                details={"error": str(e)},
            )


            SimpleJobTracker.update_status(
                job_id=job_id,
                series_id=series_id,
                status='failed',
                stage='stats_service'
            )




        finally:
            db.close()

