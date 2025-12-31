import numpy as np

def detect_anomalies_zscore(values: list[float], threshold: float = 3.0):
    """
    Simple Z-score anomaly detection.

    values: list of numbers (e.g., close prices)
    threshold: number of standard deviations from the mean

    returns:
        anomaly_indices: list of indices where anomalies occur
        z_scores: list of z-score values for each point
    """

    if not values or len(values) < 3:
        return [], []

    arr = np.array(values)
    mean = np.mean(arr)
    std = np.std(arr)

    if std == 0:
        return [], [0 for _ in values]

    z_scores = (arr - mean) / std
    anomaly_indices = [i for i, z in enumerate(z_scores) if abs(z) > threshold]

    return anomaly_indices, z_scores.tolist()
