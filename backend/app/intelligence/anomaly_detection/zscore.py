"""Z-Score anomaly detector for univariate time-series data.

Uses a rolling window to compute local mean and standard deviation, then
flags data points whose z-score exceeds a configurable threshold.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Tuple


@dataclass
class AnomalyResult:
    """A single anomalous data point detected by a statistical method."""

    timestamp: datetime
    value: float
    score: float  # absolute z-score
    method: str = "zscore"
    window_size: int = 10
    threshold: float = 2.5


class ZScoreDetector:
    """Detects anomalies in a univariate time series using rolling z-scores.

    Parameters
    ----------
    window_size : int
        Number of preceding points used to compute rolling statistics.
    threshold : float
        Minimum |z-score| to classify a point as anomalous.
    """

    def __init__(self, window_size: int = 10, threshold: float = 2.5) -> None:
        if window_size < 2:
            raise ValueError("window_size must be at least 2")
        if threshold <= 0:
            raise ValueError("threshold must be positive")
        self.window_size = window_size
        self.threshold = threshold

    def detect(
        self, series: List[Tuple[datetime, float]]
    ) -> List[AnomalyResult]:
        """Run z-score detection on a time-ordered series of (timestamp, value).

        Returns a list of AnomalyResult for every point whose |z-score|
        exceeds the configured threshold. Points within the initial warm-up
        window (fewer than *window_size* predecessors) are evaluated against
        all available preceding points.
        """
        if len(series) < 2:
            return []

        anomalies: List[AnomalyResult] = []
        values = [v for _, v in series]

        for i in range(len(values)):
            # Build the rolling window from preceding points
            window_start = max(0, i - self.window_size)
            window = values[window_start:i]

            # Need at least 2 points to compute a meaningful std dev
            if len(window) < 2:
                continue

            mean = sum(window) / len(window)
            variance = sum((x - mean) ** 2 for x in window) / len(window)
            std_dev = math.sqrt(variance)

            # Skip if there is no variance (constant window)
            if std_dev < 1e-12:
                continue

            z_score = abs((values[i] - mean) / std_dev)

            if z_score > self.threshold:
                ts, val = series[i]
                anomalies.append(
                    AnomalyResult(
                        timestamp=ts,
                        value=val,
                        score=round(z_score, 4),
                        method="zscore",
                        window_size=self.window_size,
                        threshold=self.threshold,
                    )
                )

        return anomalies
