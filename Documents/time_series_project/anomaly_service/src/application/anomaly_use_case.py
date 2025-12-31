from typing import List, Tuple

from src.domain.anomaly_service import detect_anomalies_zscore


class DetectAnomaliesUseCase:
    """
    Application-level use case for anomaly detection.
    This orchestrates domain logic and hides implementation details
    from the API layer.
    """

    def execute(self, values: List[float]) -> Tuple[List[int], List[float]]:
        """
        Run anomaly detection on provided values.

        Returns:
            anomaly_indices, z_scores
        """
        return detect_anomalies_zscore(values)
