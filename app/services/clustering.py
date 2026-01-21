from typing import Optional
import numpy as np
import polars as pl
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from ..core.constants import (
    SML_CLUSTER_DESCRIPTIONS,
    SML_CLUSTER_LABELS,
    SML_N_CLUSTERS,
    SML_RANDOM_STATE,
)

class MaturityClassifier:
    EMERGING = 0
    MATURE = 1
    HIGH_CHURN = 2
    
    def __init__(self, n_clusters: int = SML_N_CLUSTERS, random_state: int = SML_RANDOM_STATE):
        self.n_clusters = n_clusters
        self.random_state = random_state
        self.model: Optional[KMeans] = None
        self.scaler: Optional[StandardScaler] = None
        self._cluster_mapping: dict[int, int] = {}
        self._used_fallback = False
        self._method_used = "none"
    
    def _log_transform(self, features: np.ndarray) -> np.ndarray:
        return np.log1p(np.clip(features, 0, None))
    
    def _check_balance(self, labels: np.ndarray, threshold: float = 0.10) -> bool:
        total = len(labels)
        if total == 0:
            return False
        for cluster_id in range(self.n_clusters):
            count = np.sum(labels == cluster_id)
            if count / total < threshold:
                return False
        return True
    
    def _assign_labels_from_centroids(self, centroids: np.ndarray) -> dict[int, int]:
        enrol_scores = centroids[:, 0]
        demo_scores = centroids[:, 2]
        
        emerging_raw = int(np.argmax(enrol_scores))
        high_churn_raw = int(np.argmax(demo_scores))
        
        if emerging_raw == high_churn_raw:
            if enrol_scores[emerging_raw] >= demo_scores[high_churn_raw]:
                sorted_demo = np.argsort(demo_scores)[::-1]
                high_churn_raw = int(sorted_demo[1]) if len(sorted_demo) > 1 else (emerging_raw + 1) % 3
            else:
                sorted_enrol = np.argsort(enrol_scores)[::-1]
                emerging_raw = int(sorted_enrol[1]) if len(sorted_enrol) > 1 else (high_churn_raw + 1) % 3
        
        all_clusters = set(range(self.n_clusters))
        used = {emerging_raw, high_churn_raw}
        remaining = list(all_clusters - used)
        mature_raw = remaining[0] if remaining else 1
        
        return {
            emerging_raw: self.EMERGING,
            mature_raw: self.MATURE,
            high_churn_raw: self.HIGH_CHURN
        }
    
    def _quantile_binning(self, features: np.ndarray) -> np.ndarray:
        n = len(features)
        labels = np.full(n, self.MATURE)
        
        total_activity = features[:, 0] + features[:, 1] + features[:, 2] + 1
        enrol_ratio = features[:, 0] / total_activity
        demo_ratio = features[:, 2] / total_activity
        
        enrol_p67 = np.percentile(enrol_ratio, 67)
        demo_p67 = np.percentile(demo_ratio, 67)
        
        high_enrol = enrol_ratio >= enrol_p67
        high_demo = demo_ratio >= demo_p67
        
        for i in range(n):
            if high_enrol[i] and not high_demo[i]:
                labels[i] = self.EMERGING
            elif high_demo[i] and not high_enrol[i]:
                labels[i] = self.HIGH_CHURN
            elif high_enrol[i] and high_demo[i]:
                if enrol_ratio[i] >= demo_ratio[i]:
                    labels[i] = self.EMERGING
                else:
                    labels[i] = self.HIGH_CHURN
        
        return labels
    
    def fit(self, features: np.ndarray) -> "MaturityClassifier":
        if features.shape[0] < self.n_clusters:
            raise ValueError(f"Need at least {self.n_clusters} samples")
        
        log_features = self._log_transform(features)
        self.scaler = StandardScaler()
        scaled_features = self.scaler.fit_transform(log_features)
        
        self.model = KMeans(
            n_clusters=self.n_clusters,
            random_state=self.random_state,
            n_init=10,
            max_iter=300
        )
        raw_labels = self.model.fit_predict(scaled_features)
        
        if self._check_balance(raw_labels, threshold=0.10):
            self._cluster_mapping = self._assign_labels_from_centroids(self.model.cluster_centers_)
            self._used_fallback = False
            self._method_used = "kmeans"
        else:
            self._used_fallback = True
            self._method_used = "quantile"
            self._cluster_mapping = {0: 0, 1: 1, 2: 2}
        
        return self
    
    def predict(self, features: np.ndarray) -> np.ndarray:
        if self.model is None or self.scaler is None:
            raise RuntimeError("Model not fitted. Call fit() first.")
        
        log_features = self._log_transform(features)
        
        if self._used_fallback:
            return self._quantile_binning(log_features)
        
        scaled_features = self.scaler.transform(log_features)
        raw_labels = self.model.predict(scaled_features)
        
        return np.array([self._cluster_mapping.get(raw, raw) for raw in raw_labels])
    
    def fit_predict(self, features: np.ndarray) -> np.ndarray:
        self.fit(features)
        return self.predict(features)
    
    def classify_districts(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df.with_columns([
                pl.lit(None).alias("sml_cluster"),
                pl.lit(None).alias("sml_label"),
                pl.lit(None).alias("sml_description"),
            ])
        
        for col in ["total_enrolment", "total_biometric", "total_demographic"]:
            if col not in df.columns:
                df = df.with_columns(pl.lit(0).alias(col))
        
        features = df.select([
            pl.col("total_enrolment").fill_null(0),
            pl.col("total_biometric").fill_null(0),
            pl.col("total_demographic").fill_null(0),
        ]).to_numpy().astype(float)
        
        if len(features) < self.n_clusters:
            return df.with_columns([
                pl.lit(0).alias("sml_cluster"),
                pl.lit(SML_CLUSTER_LABELS[0]).alias("sml_label"),
                pl.lit(SML_CLUSTER_DESCRIPTIONS[0]).alias("sml_description"),
            ])
        
        cluster_ids = self.fit_predict(features)
        labels = [SML_CLUSTER_LABELS.get(c, "Unknown") for c in cluster_ids]
        descriptions = [SML_CLUSTER_DESCRIPTIONS.get(c, "") for c in cluster_ids]
        
        return df.with_columns([
            pl.Series("sml_cluster", cluster_ids.tolist()),
            pl.Series("sml_label", labels),
            pl.Series("sml_description", descriptions),
        ])
    
    def get_method_used(self) -> str:
        return self._method_used

_classifier: Optional[MaturityClassifier] = None

def get_maturity_classifier() -> MaturityClassifier:
    global _classifier
    if _classifier is None:
        _classifier = MaturityClassifier()
    return _classifier

def reset_classifier() -> None:
    global _classifier
    _classifier = None
