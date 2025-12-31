from pydantic import BaseModel
from typing import List, Optional


class AnomalyRequest(BaseModel):
    values: List[float]


class AnomalyResponse(BaseModel):
    anomaly_indices: List[int]
    z_scores: List[float]


class AnomalyFromDBRequest(BaseModel):
    series_id: str
    column: str = "close"
    limit: Optional[int] = None
