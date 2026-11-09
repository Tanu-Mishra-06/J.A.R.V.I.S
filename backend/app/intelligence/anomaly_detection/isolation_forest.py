"""Isolation Forest anomaly detector for multivariate feature data.

Wraps scikit-learn's IsolationForest with sensible defaults and graceful
handling of edge cases (too few samples, constant features, NaN values).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# Minimum number of samples required to fit a meaningful forest
_MIN_SAMPLES = 10


@dataclass
class IsolationForestResult:
    """Per-row anomaly classification produced by IsolationForest."""

    index: int
    is_anomaly: bool
    anomaly_score: float  # higher = more anomalous (rescaled to 0-1)


class IsolationForestDetector:
    """Multivariate anomaly detector backed by scikit-learn IsolationForest.

    Parameters
    ----------
    contamination : float
        Expected proportion of anomalies in the data.
    random_state : int
        Seed for reproducibility.
    n_estimators : int
        Number of trees in the ensemble.
    """

    def __init__(
        self,
        contamination: float = 0.1,
        random_state: int = 42,
        n_estimators: int = 100,
    ) -> None:
        self.contamination = contamination
        self.random_state = random_state
        self.n_estimators = n_estimators

    def detect(self, features: np.ndarray) -> List[IsolationForestResult]:
        """Fit an IsolationForest and return anomaly labels + scores.

        Parameters
        ----------
        features : np.ndarray
            2-D array of shape (n_samples, n_features).

        Returns
        -------
        List of IsolationForestResult, one per input row.
        """
        if features.ndim != 2:
            raise ValueError("features must be a 2-D array")

        n_samples, n_features = features.shape

        if n_samples < _MIN_SAMPLES:
            logger.warning(
                "Too few samples (%d) for IsolationForest; skipping detection",
                n_samples,
            )
            return [
                IsolationForestResult(index=i, is_anomaly=False, anomaly_score=0.0)
                for i in range(n_samples)
            ]

        # Replace NaN / Inf with column medians so the model doesn't choke
        clean_features = self._impute(features)

        # Drop constant columns (zero variance) — they carry no signal
        col_std = np.nanstd(clean_features, axis=0)
        varying_mask = col_std > 1e-12
        if not np.any(varying_mask):
            logger.warning("All features are constant; skipping detection")
            return [
                IsolationForestResult(index=i, is_anomaly=False, anomaly_score=0.0)
                for i in range(n_samples)
            ]
        filtered = clean_features[:, varying_mask]

        # Fit the model
        try:
            from sklearn.ensemble import IsolationForest

            model = IsolationForest(
                contamination=self.contamination,
                random_state=self.random_state,
                n_estimators=self.n_estimators,
            )
            model.fit(filtered)

            labels = model.predict(filtered)       # +1 = inlier, -1 = outlier
            raw_scores = model.decision_function(filtered)  # lower = more anomalous
        except Exception:
            logger.exception("IsolationForest fitting failed")
            return [
                IsolationForestResult(index=i, is_anomaly=False, anomaly_score=0.0)
                for i in range(n_samples)
            ]

        # Rescale raw scores to 0-1 where 1 = most anomalous
        normalized = self._normalize_scores(raw_scores)

        results: List[IsolationForestResult] = []
        for i in range(n_samples):
            results.append(
                IsolationForestResult(
                    index=i,
                    is_anomaly=bool(labels[i] == -1),
                    anomaly_score=round(float(normalized[i]), 4),
                )
            )
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _impute(arr: np.ndarray) -> np.ndarray:
        """Replace NaN and Inf values with per-column medians."""
        result = arr.copy().astype(float)
        result[~np.isfinite(result)] = np.nan
        for col in range(result.shape[1]):
            mask = np.isnan(result[:, col])
            if mask.any():
                median = np.nanmedian(result[:, col])
                if np.isnan(median):
                    median = 0.0
                result[mask, col] = median
        return result

    @staticmethod
    def _normalize_scores(raw_scores: np.ndarray) -> np.ndarray:
        """Map IsolationForest decision_function output to 0-1 range.

        decision_function returns values where *lower* = more anomalous.
        We invert and scale so that 1.0 = most anomalous.
        """
        inverted = -raw_scores
        s_min, s_max = inverted.min(), inverted.max()
        if s_max - s_min < 1e-12:
            return np.zeros_like(inverted)
        return (inverted - s_min) / (s_max - s_min)
