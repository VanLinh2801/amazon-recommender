"""
Score Normalization Module
==========================

Module để normalize scores trước khi đưa vào ranking model.
Mục tiêu: Giảm dominance của Popularity/CF, cân bằng các signals.

Features:
1. Min-Max normalization cho MF scores
2. Min-Max normalization cho popularity scores
3. Z-score normalization (optional)
4. Feature balancing weights
"""

import logging
import numpy as np
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class NormalizationStats:
    """Statistics để normalize scores."""
    mf_min: float = 0.0
    mf_max: float = 1.0
    mf_mean: float = 0.0
    mf_std: float = 1.0
    popularity_min: float = 0.0
    popularity_max: float = 1.0
    popularity_mean: float = 0.0
    popularity_std: float = 1.0


class ScoreNormalizer:
    """
    Service để normalize scores trước ranking.
    
    Mục tiêu:
    - Giảm dominance của Popularity/CF
    - Cân bằng các signals
    - Đảm bảo features có cùng scale
    """
    
    def __init__(
        self,
        normalization_method: str = "min_max",  # "min_max" or "z_score"
        feature_weights: Optional[Dict[str, float]] = None,
        mf_weight: float = 1.0,
        popularity_weight: float = 0.8,  # Giảm weight của popularity
        rating_weight: float = 1.0,
        content_weight: float = 1.0
    ):
        """
        Khởi tạo ScoreNormalizer.
        
        Args:
            normalization_method: "min_max" hoặc "z_score"
            feature_weights: Custom weights cho từng feature
            mf_weight: Weight cho MF score (default: 1.0)
            popularity_weight: Weight cho popularity score (default: 0.8, giảm dominance)
            rating_weight: Weight cho rating score
            content_weight: Weight cho content score
        """
        self.normalization_method = normalization_method
        self.stats: Optional[NormalizationStats] = None
        
        # Feature weights để balance dominance
        if feature_weights:
            self.feature_weights = feature_weights
        else:
            self.feature_weights = {
                "mf_score": mf_weight,
                "popularity_score": popularity_weight,  # Giảm weight
                "rating_score": rating_weight,
                "content_score": content_weight
            }
        
        logger.info(
            f"ScoreNormalizer initialized: method={normalization_method}, "
            f"weights={self.feature_weights}"
        )
    
    def compute_stats(
        self,
        mf_scores: List[float],
        popularity_scores: List[float]
    ) -> NormalizationStats:
        """
        Tính statistics từ scores để normalize.
        
        Args:
            mf_scores: List of MF scores
            popularity_scores: List of popularity scores
            
        Returns:
            NormalizationStats
        """
        mf_array = np.array(mf_scores)
        pop_array = np.array(popularity_scores)
        
        stats = NormalizationStats(
            mf_min=float(np.min(mf_array)) if len(mf_array) > 0 else 0.0,
            mf_max=float(np.max(mf_array)) if len(mf_array) > 0 else 1.0,
            mf_mean=float(np.mean(mf_array)) if len(mf_array) > 0 else 0.0,
            mf_std=float(np.std(mf_array)) if len(mf_array) > 0 and np.std(mf_array) > 0 else 1.0,
            popularity_min=float(np.min(pop_array)) if len(pop_array) > 0 else 0.0,
            popularity_max=float(np.max(pop_array)) if len(pop_array) > 0 else 1.0,
            popularity_mean=float(np.mean(pop_array)) if len(pop_array) > 0 else 0.0,
            popularity_std=float(np.std(pop_array)) if len(pop_array) > 0 and np.std(pop_array) > 0 else 1.0
        )
        
        return stats
    
    def normalize_mf_score(self, mf_score: float) -> float:
        """
        Normalize MF score.
        
        Args:
            mf_score: Raw MF score
            
        Returns:
            Normalized MF score [0, 1]
        """
        if self.stats is None:
            # Fallback: clip to [0, 1] nếu chưa có stats
            return float(np.clip(mf_score, 0.0, 1.0))
        
        if self.normalization_method == "min_max":
            # Min-Max normalization
            if self.stats.mf_max > self.stats.mf_min:
                normalized = (mf_score - self.stats.mf_min) / (self.stats.mf_max - self.stats.mf_min)
            else:
                normalized = 0.5  # Fallback
            return float(np.clip(normalized, 0.0, 1.0))
        
        elif self.normalization_method == "z_score":
            # Z-score normalization (sau đó clip về [0, 1])
            if self.stats.mf_std > 0:
                z_score = (mf_score - self.stats.mf_mean) / self.stats.mf_std
                # Convert z-score to [0, 1] using sigmoid
                normalized = 1.0 / (1.0 + np.exp(-z_score))
            else:
                normalized = 0.5
            return float(np.clip(normalized, 0.0, 1.0))
        
        else:
            return float(np.clip(mf_score, 0.0, 1.0))
    
    def normalize_popularity_score(self, popularity_score: float) -> float:
        """
        Normalize popularity score.
        
        Args:
            popularity_score: Raw popularity score
            
        Returns:
            Normalized popularity score [0, 1]
        """
        if self.stats is None:
            return float(np.clip(popularity_score, 0.0, 1.0))
        
        if self.normalization_method == "min_max":
            if self.stats.popularity_max > self.stats.popularity_min:
                normalized = (popularity_score - self.stats.popularity_min) / (
                    self.stats.popularity_max - self.stats.popularity_min
                )
            else:
                normalized = 0.5
            return float(np.clip(normalized, 0.0, 1.0))
        
        elif self.normalization_method == "z_score":
            if self.stats.popularity_std > 0:
                z_score = (popularity_score - self.stats.popularity_mean) / self.stats.popularity_std
                normalized = 1.0 / (1.0 + np.exp(-z_score))
            else:
                normalized = 0.5
            return float(np.clip(normalized, 0.0, 1.0))
        
        else:
            return float(np.clip(popularity_score, 0.0, 1.0))
    
    def normalize_feature_vector(
        self,
        features: List[float],
        apply_weights: bool = True
    ) -> List[float]:
        """
        Normalize feature vector và apply weights.
        
        Args:
            features: [mf_score, popularity_score, rating_score, content_score]
            apply_weights: Có apply weights không
            
        Returns:
            Normalized và weighted feature vector
        """
        if len(features) < 4:
            # Pad với 0 nếu thiếu
            features = list(features) + [0.0] * (4 - len(features))
        
        mf_score, popularity_score, rating_score, content_score = features[:4]
        
        # Normalize
        normalized_mf = self.normalize_mf_score(mf_score)
        normalized_pop = self.normalize_popularity_score(popularity_score)
        
        # Rating và content đã được normalize (0-1)
        normalized_rating = float(np.clip(rating_score, 0.0, 1.0))
        normalized_content = float(np.clip(content_score, 0.0, 1.0))
        
        # Apply weights nếu cần
        if apply_weights:
            normalized_mf *= self.feature_weights.get("mf_score", 1.0)
            normalized_pop *= self.feature_weights.get("popularity_score", 0.8)
            normalized_rating *= self.feature_weights.get("rating_score", 1.0)
            normalized_content *= self.feature_weights.get("content_score", 1.0)
        
        return [
            normalized_mf,
            normalized_pop,
            normalized_rating,
            normalized_content
        ]
    
    def normalize_batch(
        self,
        feature_vectors: List[List[float]]
    ) -> Tuple[List[List[float]], NormalizationStats]:
        """
        Normalize batch of feature vectors.
        
        Args:
            feature_vectors: List of feature vectors
            
        Returns:
            Tuple of (normalized_vectors, stats)
        """
        if not feature_vectors:
            return [], NormalizationStats()
        
        # Extract scores để tính stats
        mf_scores = [fv[0] if len(fv) > 0 else 0.0 for fv in feature_vectors]
        popularity_scores = [fv[1] if len(fv) > 1 else 0.0 for fv in feature_vectors]
        
        # Compute stats
        self.stats = self.compute_stats(mf_scores, popularity_scores)
        
        # Normalize từng vector
        normalized_vectors = [
            self.normalize_feature_vector(fv, apply_weights=True)
            for fv in feature_vectors
        ]
        
        return normalized_vectors, self.stats


# Singleton instance
_normalizer_instance: Optional[ScoreNormalizer] = None


def get_score_normalizer(
    normalization_method: str = "min_max",
    popularity_weight: float = 0.8
) -> ScoreNormalizer:
    """
    Get singleton instance của ScoreNormalizer.
    
    Args:
        normalization_method: "min_max" hoặc "z_score"
        popularity_weight: Weight cho popularity (giảm dominance)
        
    Returns:
        ScoreNormalizer instance
    """
    global _normalizer_instance
    
    if _normalizer_instance is None:
        _normalizer_instance = ScoreNormalizer(
            normalization_method=normalization_method,
            popularity_weight=popularity_weight
        )
    
    return _normalizer_instance



