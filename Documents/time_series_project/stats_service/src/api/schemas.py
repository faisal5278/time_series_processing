from pydantic import BaseModel
from typing import List, Optional


class StatsRequest(BaseModel):
    values: List[float]


class StatsResponse(BaseModel):
    mean: Optional[float]
    min: Optional[float]
    max: Optional[float]
    std: Optional[float]
    trend_slope: Optional[float]



class StatsFromDBRequest(BaseModel):
    series_id: str
    column: str = "close"
    limit: Optional[int] = None
