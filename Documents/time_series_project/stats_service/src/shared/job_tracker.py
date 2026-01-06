import os
import json
import time
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON, Text, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

Base = declarative_base()

class JobStatus(Base):
    """Generic job status model - table name set dynamically"""
    __tablename__ = "job_status"  # Will be overridden
    
    id = Column(Integer, primary_key=True)
    series_id = Column(String(255), nullable=False, index=True)
    status = Column(String(50), nullable=False, index=True)
    details = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class JobTracker:
    def __init__(self, status_db_url: str = None, table_name: str = "job_status"):
        """
        Initialize JobTracker with custom status database URL.
        
        Args:
            status_db_url: Database URL for status tracking
            table_name: Name of the status table (e.g., 'anomaly_job_status')
        """
        self.status_db_url = status_db_url or os.getenv(
            "STATUS_DATABASE_URL",
            "postgresql+psycopg2://tsuser:ts_password@localhost:5432/status"
        )
        self.table_name = table_name
        
        # Create engine with connection retry
        self.engine = self._create_engine_with_retry()
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # Override table name
        JobStatus.__tablename__ = table_name

    def _create_engine_with_retry(self, max_retries=5, retry_delay=2):
        """
        Create SQLAlchemy engine with connection retry logic.
        """
        for attempt in range(max_retries):
            try:
                engine = create_engine(self.status_db_url)
                # Test connection - use text() for SQLAlchemy 2.x compatibility
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                print(f"✅ Successfully connected to status database: {self.status_db_url}")
                return engine
            except OperationalError as e:
                if attempt < max_retries - 1:
                    print(f"⚠️  Status DB not ready (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    print(f"❌ Failed to connect to status database after {max_retries} attempts")
                    raise

    def create_tables(self):
        """Create status table if it doesn't exist"""
        try:
            Base.metadata.create_all(bind=self.engine)
            print(f"✅ Status table '{self.table_name}' ready")
        except Exception as e:
            print(f"❌ Error creating status table: {e}")
            raise

    def write_status(
        self,
        series_id: str,
        status: str,
        details: dict = None,
        **kwargs
    ):
        """
        Write job status to the status database.
        
        Args:
            series_id: The series being processed
            status: Job status (started, done, failed, no_data)
            details: Additional JSON details
            **kwargs: Additional fields to store in details
        """
        session = self.SessionLocal()
        try:
            # Merge kwargs into details
            if details is None:
                details = {}
            details.update(kwargs)
            
            # Create status record
            job_status = JobStatus(
                series_id=series_id,
                status=status,
                details=details if details else None,
                updated_at=datetime.utcnow()
            )
            
            session.add(job_status)
            session.commit()
            
        except Exception as e:
            session.rollback()
            print(f"Error writing status: {e}")
        finally:
            session.close()

    def get_latest_status(self, series_id: str):
        """Get the latest status for a series_id"""
        session = self.SessionLocal()
        try:
            result = (
                session.query(JobStatus)
                .filter(JobStatus.series_id == series_id)
                .order_by(JobStatus.created_at.desc())
                .first()
            )
            return result
        finally:
            session.close()
