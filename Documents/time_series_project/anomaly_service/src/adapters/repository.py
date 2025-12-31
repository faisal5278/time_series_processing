from typing import List, Tuple, Optional

from sqlalchemy.orm import Session

from src.adapters.models import TimeSeriesPreprocessed


ALLOWED_COLUMNS = {"open", "high", "low", "close", "volume"}


def fetch_series_values(
    db: Session,
    series_id: str,
    column: str = "close",
    limit: Optional[int] = None,
) -> Tuple[List[str], List[float]]:
    """
    Fetch time-series values for a given series_id and column from
    time_series_preprocessed table.

    Returns:
        timestamps: list of ISO8601 timestamp strings
        values: list of floats
    """

    if column not in ALLOWED_COLUMNS:
        raise ValueError(f"Unsupported column '{column}'. Must be one of {ALLOWED_COLUMNS}")

    # Dynamically pick the column attribute (open/high/low/close/volume)
    column_attr = getattr(TimeSeriesPreprocessed, column)

    query = (
        db.query(TimeSeriesPreprocessed.timestamp, column_attr)
        .filter(TimeSeriesPreprocessed.series_id == series_id)
        .order_by(TimeSeriesPreprocessed.timestamp.asc())
    )

    if limit is not None:
        query = query.limit(limit)

    rows = query.all()

    timestamps = [row[0].isoformat() for row in rows]
    values = [row[1] for row in rows]

    return timestamps, values
