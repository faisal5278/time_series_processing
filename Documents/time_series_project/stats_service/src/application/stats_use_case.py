from typing import List, Dict
import numpy as np


class StatsUseCase:
    """
    Pure application logic.
    No Kafka, no DB, no FastAPI here.
    """

    def execute(self, values: List[float]) -> Dict[str, float]:
        if not values:
            return {
                "count": 0,
                "mean": 0.0,
                "min": 0.0,
                "max": 0.0,
            }

        arr = np.array(values, dtype=float)

        return {
            "count": int(arr.size),
            "mean": float(arr.mean()),
            "min": float(arr.min()),
            "max": float(arr.max()),
        }
