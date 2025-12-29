"""
Ranking Service
===============

Service để rank candidate items sử dụng trained Logistic Regression model.

Input: user_id, candidates (List[ItemCandidate])
Output: List[RankedItem] (sorted by rank_score DESC)

Feature order (PHẢI đúng với training):
1. mf_score
2. popularity_score
3. rating_score
4. content_score
"""

import json
import logging
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict, Any
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

from app.recommender.score_normalizer import ScoreNormalizer, get_score_normalizer

# Setup logging
logger = logging.getLogger(__name__)

# Debug flag
DEBUG_RANKING = False


@dataclass
class ItemCandidate:
    """
    Candidate item từ recall layer.
    
    Attributes:
        item_id: Item ID (string)
        mf_score: Optional MF score (nếu có từ MF recall)
        content_score: Optional content similarity score (nếu có từ content recall)
        raw_signals: Optional dict chứa các raw signals khác
    """
    item_id: str
    mf_score: Optional[float] = None
    content_score: Optional[float] = None
    raw_signals: Optional[Dict[str, Any]] = None


@dataclass
class RankedItem:
    """
    Item đã được rank.
    
    Attributes:
        item_id: Item ID
        rank_score: Ranking score từ model (probability hoặc raw score)
        rank_position: Vị trí trong ranking (1-based)
        features: Feature vector đã dùng để predict (for debugging)
        category: Item category (optional, for re-ranking)
        rating_number: Number of ratings (optional, for re-ranking)
        raw_signals: Raw signals dict (optional, for re-ranking)
    """
    item_id: str
    rank_score: float
    rank_position: int
    features: Optional[List[float]] = None
    category: Optional[str] = None
    rating_number: Optional[int] = None
    raw_signals: Optional[Dict[str, Any]] = None


class RankingService:
    """
    Service để rank candidate items.
    
    Load model một lần và cache để tái sử dụng.
    """
    
    def __init__(
        self,
        model_path: Optional[Path] = None,
        metadata_path: Optional[Path] = None,
        artifacts_dir: Optional[Path] = None,
        top_n: int = 50,
        use_normalization: bool = True,
        popularity_weight: float = 0.8
    ):
        """
        Khởi tạo RankingService.
        
        Args:
            model_path: Đường dẫn đến ranking_model.pkl
            metadata_path: Đường dẫn đến model_metadata.json (optional)
            artifacts_dir: Thư mục artifacts (nếu None, dùng default)
            top_n: Số lượng items top-N cần trả về
        """
        if artifacts_dir is None:
            base_dir = Path(__file__).resolve().parent.parent.parent
            artifacts_dir = base_dir / "artifacts"
        
        self.artifacts_dir = artifacts_dir
        self.top_n = top_n
        
        # Model paths
        if model_path is None:
            model_path = artifacts_dir / "ranking" / "ranking_model.pkl"
        if metadata_path is None:
            metadata_path = artifacts_dir / "ranking" / "model_metadata.json"
        
        self.model_path = model_path
        self.metadata_path = metadata_path
        
        # Model và metadata sẽ được load lazy
        self._model: Optional[LogisticRegression] = None
        self._metadata: Optional[Dict[str, Any]] = None
        
        # Feature order (PHẢI đúng với training)
        self.feature_order = ['mf_score', 'popularity_score', 'rating_score', 'content_score']
        
        # Lookup tables (sẽ được load từ artifacts)
        self._popularity_lookup: Optional[Dict[str, float]] = None
        self._rating_lookup: Optional[Dict[str, Dict[str, float]]] = None
        
        # Score normalizer (để giảm dominance)
        self.use_normalization = use_normalization
        self.normalizer: Optional[ScoreNormalizer] = None
        if use_normalization:
            try:
                self.normalizer = get_score_normalizer(
                    normalization_method="min_max",
                    popularity_weight=popularity_weight  # Giảm weight của popularity
                )
            except Exception as e:
                logger.warning(f"Failed to initialize score normalizer: {e}")
                self.use_normalization = False
        
        logger.info(
            f"RankingService initialized with top_n={top_n}, "
            f"use_normalization={self.use_normalization}"
        )
    
    def _load_model(self):
        """Load ranking model nếu chưa load."""
        if self._model is not None:
            return  # Đã load rồi
        
        if not self.model_path.exists():
            raise FileNotFoundError(
                f"Không tìm thấy ranking model: {self.model_path}\n"
                f"Vui lòng train và lưu model trước khi sử dụng."
            )
        
        logger.debug(f"Loading ranking model from {self.model_path}")
        with open(self.model_path, 'rb') as f:
            self._model = pickle.load(f)
        
        logger.info(f"Ranking model loaded: {type(self._model).__name__}")
    
    def _load_metadata(self):
        """Load model metadata nếu có."""
        if self._metadata is not None:
            return  # Đã load rồi
        
        if self.metadata_path.exists():
            logger.debug(f"Loading metadata from {self.metadata_path}")
            with open(self.metadata_path, 'r', encoding='utf-8') as f:
                self._metadata = json.load(f)
            logger.debug(f"Metadata loaded: {self._metadata}")
        else:
            logger.debug("Metadata file not found, using defaults")
            self._metadata = {}
    
    def _load_popularity_lookup(self):
        """Load popularity lookup table (bao gồm cả rating_score nếu có)."""
        if self._popularity_lookup is not None:
            return  # Đã load rồi
        
        popularity_path = self.artifacts_dir / "popularity" / "item_popularity_normalized.parquet"
        if not popularity_path.exists():
            logger.warning(f"Popularity file not found: {popularity_path}")
            self._popularity_lookup = {}
            self._rating_lookup = {}
            return
        
        logger.debug(f"Loading popularity lookup from {popularity_path}")
        df = pd.read_parquet(popularity_path)
        
        # Tạo lookup: item_id -> popularity_score
        self._popularity_lookup = dict(
            zip(df['item_id'].astype(str), df['popularity_score'])
        )
        
        # Tạo rating lookup nếu có rating_score column
        if 'rating_score' in df.columns:
            self._rating_lookup = dict(
                zip(df['item_id'].astype(str), df['rating_score'])
            )
        else:
            self._rating_lookup = {}
        
        logger.debug(f"Popularity lookup loaded: {len(self._popularity_lookup)} items")
        if self._rating_lookup:
            logger.debug(f"Rating lookup loaded: {len(self._rating_lookup)} items")
    
    
    def _build_feature_vector(
        self,
        user_id: str,
        candidate: ItemCandidate,
        user_vector: Optional[np.ndarray] = None,
        item_vector: Optional[np.ndarray] = None,
        user2idx: Optional[Dict[str, int]] = None,
        idx2item: Optional[Dict[int, str]] = None
    ) -> List[float]:
        """
        Xây dựng feature vector cho (user_id, item_id).
        
        Feature order (PHẢI đúng với training):
        1. mf_score: dot(user_vector, item_vector)
        2. popularity_score: normalized popularity
        3. rating_score: average_rating hoặc log(rating_number)
        4. content_score: cosine similarity (nếu có)
        
        Args:
            user_id: User ID
            candidate: ItemCandidate object
            user_vector: User latent vector (optional, nếu có sẵn)
            item_vector: Item latent vector (optional, nếu có sẵn)
            user2idx: User to index mapping (optional)
            idx2item: Index to item mapping (optional)
            
        Returns:
            Feature vector: [mf_score, popularity_score, rating_score, content_score]
        """
        features = []
        
        # 1. mf_score
        mf_score = 0.0
        if candidate.mf_score is not None:
            # Nếu đã có sẵn từ recall
            mf_score = float(candidate.mf_score)
        elif user_vector is not None and item_vector is not None:
            # Tính dot-product
            mf_score = float(np.dot(item_vector, user_vector))
        elif user2idx is not None and idx2item is not None:
            # Cần load MF artifacts và tính
            # Tạm thời dùng 0 nếu không có
            logger.warning(f"MF score not available for item {candidate.item_id}, using 0.0")
            mf_score = 0.0
        else:
            logger.warning(f"MF score not available for item {candidate.item_id}, using 0.0")
            mf_score = 0.0
        
        features.append(mf_score)
        
        # 2. popularity_score
        self._load_popularity_lookup()
        popularity_score = self._popularity_lookup.get(candidate.item_id, 0.0)
        features.append(float(popularity_score))
        
        # 3. rating_score
        # Ưu tiên lấy từ rating_lookup (nếu có trong popularity data)
        # Nếu không, tính từ raw_signals.avg_rating
        rating_score = 0.0
        
        # Load rating lookup nếu chưa load
        if self._rating_lookup is None:
            self._load_popularity_lookup()
        
        # Thử lấy từ rating_lookup trước
        if self._rating_lookup and candidate.item_id in self._rating_lookup:
            rating_score = float(self._rating_lookup[candidate.item_id])
        elif candidate.raw_signals and 'avg_rating' in candidate.raw_signals:
            # Fallback: tính từ raw_signals
            # Normalize rating từ [1, 5] về [0, 1]: (avg_rating - 1) / 4
            avg_rating = candidate.raw_signals['avg_rating']
            if avg_rating is not None:
                rating_score = (float(avg_rating) - 1.0) / 4.0
        else:
            # Fallback: dùng 0.0
            rating_score = 0.0
        
        features.append(float(rating_score))
        
        # 4. content_score
        content_score = 0.0
        if candidate.content_score is not None:
            content_score = float(candidate.content_score)
        else:
            # Fallback: 0.0
            content_score = 0.0
        
        features.append(float(content_score))
        
        return features
    
    def _predict_scores(self, feature_vectors: np.ndarray) -> np.ndarray:
        """
        Chạy inference ranking model.
        
        Args:
            feature_vectors: Array shape (n_samples, n_features)
            
        Returns:
            Array shape (n_samples,) chứa rank scores
        """
        self._load_model()
        
        # Dùng predict_proba nếu có (lấy probability của positive class)
        if hasattr(self._model, 'predict_proba'):
            probabilities = self._model.predict_proba(feature_vectors)
            # Lấy probability của class 1 (positive)
            if probabilities.shape[1] == 2:
                scores = probabilities[:, 1]
            else:
                # Nếu chỉ có 1 class, dùng class 0
                scores = probabilities[:, 0]
        else:
            # Fallback: dùng predict (binary)
            scores = self._model.predict(feature_vectors).astype(float)
        
        return scores
    
    def rank_candidates(
        self,
        user_id: str,
        candidates: List[ItemCandidate],
        user_vector: Optional[np.ndarray] = None,
        item_vectors: Optional[Dict[str, np.ndarray]] = None,
        user2idx: Optional[Dict[str, int]] = None,
        idx2item: Optional[Dict[int, str]] = None,
        item2idx: Optional[Dict[str, int]] = None,
        content_scores: Optional[Dict[str, float]] = None,
        content_score_boost: float = 1.0
    ) -> List[RankedItem]:
        """
        Rank candidate items.
        
        Args:
            user_id: User ID
            candidates: List of ItemCandidate
            user_vector: User latent vector (optional)
            item_vectors: Dict mapping item_id -> item_vector (optional)
            user2idx: User to index mapping (optional)
            idx2item: Index to item mapping (optional)
            item2idx: Item to index mapping (optional)
            content_scores: Dict mapping item_id -> content_score (optional)
            content_score_boost: Multiplier để boost content_score (default: 1.0)
            
        Returns:
            List of RankedItem, sorted by rank_score DESC
        """
        if not candidates:
            logger.warning(f"No candidates to rank for user {user_id}")
            return []
        
        logger.info(f"Ranking {len(candidates)} candidates for user_id: {user_id}")
        
        # Build feature vectors cho tất cả candidates
        feature_vectors = []
        candidate_items = []
        
        for candidate in candidates:
            # Lấy item_vector nếu có
            item_vector = None
            if item_vectors and candidate.item_id in item_vectors:
                item_vector = item_vectors[candidate.item_id]
            elif item2idx and idx2item:
                # Có thể load từ MF artifacts
                # Tạm thời để None, sẽ tính từ mf_score nếu có
                pass
            
            # Override content_score nếu có content_scores dict
            if content_scores and candidate.item_id in content_scores:
                # Boost content_score nếu cần
                boosted_score = content_scores[candidate.item_id] * content_score_boost
                # Update candidate's content_score
                candidate.content_score = min(1.0, boosted_score)
            
            # Build feature vector
            features = self._build_feature_vector(
                user_id=user_id,
                candidate=candidate,
                user_vector=user_vector,
                item_vector=item_vector,
                user2idx=user2idx,
                idx2item=idx2item
            )
            
            feature_vectors.append(features)
            candidate_items.append(candidate.item_id)
        
        # Normalize feature vectors nếu cần (để giảm dominance)
        if self.use_normalization and self.normalizer:
            # Extract scores để tính stats
            mf_scores = [fv[0] if len(fv) > 0 else 0.0 for fv in feature_vectors]
            popularity_scores = [fv[1] if len(fv) > 1 else 0.0 for fv in feature_vectors]
            
            # Compute stats và normalize
            normalized_vectors, stats = self.normalizer.normalize_batch(feature_vectors)
            feature_vectors = normalized_vectors
            
            if DEBUG_RANKING:
                logger.info(
                    f"Normalization stats: "
                    f"MF=[{stats.mf_min:.4f}, {stats.mf_max:.4f}], "
                    f"Pop=[{stats.popularity_min:.4f}, {stats.popularity_max:.4f}]"
                )
        
        # Convert to numpy array
        X = np.array(feature_vectors)
        
        if DEBUG_RANKING:
            logger.info(f"Feature vectors shape: {X.shape}")
            logger.info(f"Feature order: {self.feature_order}")
            # Log first 3-5 items
            num_samples = min(5, len(candidates))
            for i in range(num_samples):
                logger.info(
                    f"  Item {candidate_items[i]}: "
                    f"features={[f'{f:.4f}' for f in feature_vectors[i]]}"
                )
        
        # Predict scores
        rank_scores = self._predict_scores(X)
        
        if DEBUG_RANKING:
            logger.info("Rank scores (first 5):")
            for i in range(min(5, len(rank_scores))):
                logger.info(f"  Item {candidate_items[i]}: score={rank_scores[i]:.6f}")
        
        # Create RankedItem objects
        ranked_items = []
        for i, (item_id, score) in enumerate(zip(candidate_items, rank_scores)):
            ranked_items.append(
                RankedItem(
                    item_id=item_id,
                    rank_score=float(score),
                    rank_position=i + 1,  # Will be updated after sorting
                    features=feature_vectors[i] if DEBUG_RANKING else None
                )
            )
        
        # Sort by rank_score DESC
        ranked_items.sort(key=lambda x: x.rank_score, reverse=True)
        
        # Update rank positions
        for i, item in enumerate(ranked_items):
            item.rank_position = i + 1
        
        # Return top-N
        top_ranked = ranked_items[:self.top_n]
        
        logger.info(
            f"Ranking completed: {len(top_ranked)} items (top-{self.top_n}) "
            f"from {len(candidates)} candidates"
        )
        
        if DEBUG_RANKING:
            logger.info("Top 5 ranked items:")
            for item in top_ranked[:5]:
                logger.info(
                    f"  Rank {item.rank_position}: {item.item_id} "
                    f"(score={item.rank_score:.6f})"
                )
        
        return top_ranked


# Singleton instance
_ranking_service_instance: Optional[RankingService] = None


def get_ranking_service(
    model_path: Optional[Path] = None,
    metadata_path: Optional[Path] = None,
    artifacts_dir: Optional[Path] = None,
    top_n: int = 50
) -> RankingService:
    """
    Get singleton instance của RankingService.
    
    Args:
        model_path: Đường dẫn đến ranking_model.pkl
        metadata_path: Đường dẫn đến model_metadata.json
        artifacts_dir: Thư mục artifacts
        top_n: Số lượng items top-N
        
    Returns:
        RankingService instance
    """
    global _ranking_service_instance
    
    if _ranking_service_instance is None:
        _ranking_service_instance = RankingService(
            model_path=model_path,
            metadata_path=metadata_path,
            artifacts_dir=artifacts_dir,
            top_n=top_n
        )
    
    return _ranking_service_instance


# Convenience function
def rank_candidates(
    user_id: str,
    candidates: List[ItemCandidate],
    model_path: Optional[Path] = None,
    artifacts_dir: Optional[Path] = None,
    top_n: int = 50,
    **kwargs
) -> List[RankedItem]:
    """
    Convenience function để rank candidates.
    
    Args:
        user_id: User ID
        candidates: List of ItemCandidate
        model_path: Đường dẫn đến ranking_model.pkl
        artifacts_dir: Thư mục artifacts
        top_n: Số lượng items top-N
        **kwargs: Additional arguments cho rank_candidates
        
    Returns:
        List of RankedItem
    """
    service = get_ranking_service(
        model_path=model_path,
        artifacts_dir=artifacts_dir,
        top_n=top_n
    )
    return service.rank_candidates(user_id, candidates, **kwargs)

