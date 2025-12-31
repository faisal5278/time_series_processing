from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, DateTime, Float
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()


class TimeSeriesPreprocessed(Base):
    __tablename__ = "time_series_preprocessed"

    series_id = Column(String, primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)

    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)

    features = Column(JSONB)
