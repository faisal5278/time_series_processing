import numpy as np

def compute_basic_stats(values: list[float]):
    """
    Compute simple descriptive statistics.
    """

    if not values:
        return {
            "mean": None,
            "min": None,
            "max": None,
            "std": None,
            "trend_slope": None
        }

    arr = np.array(values)

    # simple trend using linear regression y = a*x + b
    x = np.arange(len(arr))
    slope, _ = np.polyfit(x, arr, 1)

    return {
        "mean": float(np.mean(arr)),
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "std": float(np.std(arr)),
        "trend_slope": float(slope)
    }
