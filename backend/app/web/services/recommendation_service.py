"""
Recommendation Service
======================

Service để generate recommendations sử dụng full pipeline:
Recall -> Ranking -> Re-ranking
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
import numpy as np

from app.recommender.recall_service import RecallService
from app.recommender.ranking_service import RankingService, ItemCandidate, RankedItem
from app.recommender.reranking_service import ReRankingService, ReRankedItem
from app.recommender.content_recall_service import ContentBasedRecallService
from app.web.schemas.item import RecommendedItemResponse

logger = logging.getLogger(__name__)


class RecommendationService:
    """
    Service để generate recommendations.
    
    Sử dụng full pipeline: Recall -> Ranking -> Re-ranking
    """
    
    def __init__(
        self,
        artifacts_dir: Optional[Path] = None,
        k_mf: int = 100,
        k_pop: int = 50,
        top_n: int = 20
    ):
        """
        Khởi tạo RecommendationService.
        
        Args:
            artifacts_dir: Thư mục chứa artifacts. Nếu None, dùng default.
            k_mf: Số lượng items từ MF recall
            k_pop: Số lượng items từ popularity recall
            top_n: Số lượng recommendations cuối cùng
        """
        if artifacts_dir is None:
            artifacts_dir = Path(__file__).resolve().parent.parent.parent.parent / "artifacts"
        
        self.artifacts_dir = artifacts_dir
        self.k_mf = k_mf
        self.k_pop = k_pop
        self.top_n = top_n
        
        # Initialize services
        self.recall_service = RecallService(
            artifacts_dir=artifacts_dir,
            k_mf=k_mf,
            k_pop=k_pop
        )
        self.ranking_service = RankingService(
            artifacts_dir=artifacts_dir,
            top_n=top_n * 2,  # Rank nhiều hơn để re-ranking có đủ items
            use_normalization=True,  # Enable normalization để giảm dominance
            popularity_weight=0.8  # Giảm weight của popularity
        )
        self.reranking_service = ReRankingService(
            top_n=top_n,
            diversity_threshold=0.25,  # Giảm threshold để tăng diversity (25% thay vì 30%)
            diversity_penalty=0.7,  # Penalty mạnh hơn (0.7 thay vì 0.75)
            max_same_category=4  # Giảm từ 5 xuống 4 items cùng category
        )
        
        # Initialize content-based recall service
        self.content_recall_service = ContentBasedRecallService()
        
        logger.info(
            f"RecommendationService initialized: "
            f"artifacts_dir={artifacts_dir}, k_mf={k_mf}, k_pop={k_pop}, top_n={top_n}"
        )
    
    def generate_recommendations(
        self,
        user_id: str,
        top_n: Optional[int] = None,
        reference_item_id: Optional[str] = None,
        user_reference_items: Optional[List[str]] = None,
        content_score_boost: float = 1.0,
        use_only_content_recall: bool = False
    ) -> tuple[List[ReRankedItem], int, int]:
        """
        Generate recommendations cho user.
        
        Args:
            user_id: User ID (string)
            top_n: Số lượng recommendations (override default nếu có)
            reference_item_id: Item ID để tính content similarity (ví dụ: item hiện tại ở product detail page)
            user_reference_items: List of item IDs từ user history (cart, purchases, views)
            content_score_boost: Multiplier để boost content_score (default: 1.0, tăng lên 2.0-3.0 cho product detail)
            use_only_content_recall: Nếu True, chỉ dùng Content-based recall (cho product detail page, không dùng MF/Popularity)
            
        Returns:
            Tuple of (recommendations, recall_count, ranking_count)
        """
        if top_n is None:
            top_n = self.top_n
        
        logger.info(f"Generating recommendations for user_id: {user_id}")
        
        # Step 1: Recall (với content-based recall nếu có reference items)
        # Lấy user history từ Redis và PostgreSQL để cải thiện recommendations
        exclude_recent_items = None
        user_reference_items_from_history = None
        
        try:
            # Import services
            from app.recommender.reranking_service import ReRankingService
            from app.web.services.user_interaction_service import get_user_interaction_service
            
            # Lấy recent items từ Redis để exclude
            temp_reranking = ReRankingService()
            exclude_recent_items = temp_reranking._load_recent_items(user_id)
            if exclude_recent_items:
                logger.info(f"Excluding {len(exclude_recent_items)} recent items from Popularity recall")
            
            # Lấy user history để dùng làm reference items cho content-based recall
            # (chỉ lấy nếu chưa có user_reference_items từ parameter)
            if not user_reference_items:
                try:
                    # Import db session (cần async context)
                    from app.web.utils.database import get_db
                    # Note: Cần async context, sẽ lấy trong route handler
                    # Tạm thời chỉ dùng Redis data
                    interaction_service = get_user_interaction_service()
                    recent_items = interaction_service.get_recent_items_from_redis(int(user_id), limit=20)
                    if recent_items:
                        user_reference_items_from_history = recent_items
                        logger.info(f"Using {len(recent_items)} recent items from Redis as reference for content recall")
                except Exception as e:
                    logger.debug(f"Could not load user history: {e}")
                    
        except Exception as e:
            logger.debug(f"Could not load recent items from Redis: {e}")
            exclude_recent_items = None
        
        # Sử dụng user_reference_items từ history nếu chưa có
        final_user_reference_items = user_reference_items or user_reference_items_from_history
        
        candidate_item_ids = self.recall_service.recall_candidates(
            user_id=user_id,
            reference_item_id=reference_item_id,
            user_reference_items=final_user_reference_items,  # Dùng history nếu có
            exclude_recent_items=exclude_recent_items,  # Exclude items đã xem
            use_only_content_recall=use_only_content_recall  # Chỉ dùng Content recall cho product detail
        )
        recall_count = len(candidate_item_ids)
        
        logger.info(f"Recall: {recall_count} candidates")
        
        if not candidate_item_ids:
            logger.warning(f"No candidates found for user {user_id}")
            return [], 0, 0
        
        # Step 2: Convert to ItemCandidate và Rank
        # Load MF artifacts
        self.recall_service._load_mf_artifacts()
        self.recall_service._load_popularity_data()
        
        # Tạo ItemCandidate objects
        candidates = []
        mf_scores = {}
        user_vector = None
        item_vectors = {}
        
        # Tính MF scores nếu user có trong MF
        if user_id in self.recall_service._user2idx:
            user_idx = self.recall_service._user2idx[user_id]
            user_vector = self.recall_service._user_factors[user_idx]
            
            for item_id in candidate_item_ids:
                # Tìm item index
                item_idx = None
                for idx, mapped_item_id in self.recall_service._idx2item.items():
                    if mapped_item_id == item_id:
                        item_idx = idx
                        break
                
                if item_idx is not None:
                    item_vector = self.recall_service._item_factors[item_idx]
                    mf_score = float(np.dot(item_vector, user_vector))
                    mf_scores[item_id] = mf_score
                    item_vectors[item_id] = item_vector
        
        # Lấy popularity và rating từ data
        popularity_df = self.recall_service._popularity_df
        
        for item_id in candidate_item_ids:
            # Lấy popularity và rating
            item_data = popularity_df[popularity_df['item_id'].astype(str) == item_id]
            
            raw_signals = {}
            if not item_data.empty:
                if 'popularity_score' in item_data.columns:
                    raw_signals['popularity_score'] = float(item_data['popularity_score'].iloc[0])
                if 'rating_score' in item_data.columns:
                    raw_signals['rating_score'] = float(item_data['rating_score'].iloc[0])
                if 'avg_rating' in item_data.columns:
                    raw_signals['avg_rating'] = float(item_data['avg_rating'].iloc[0])
                if 'rating_number' in item_data.columns:
                    raw_signals['rating_number'] = int(item_data['rating_number'].iloc[0])
            
            candidate = ItemCandidate(
                item_id=item_id,
                mf_score=mf_scores.get(item_id),
                content_score=None,
                raw_signals=raw_signals if raw_signals else None
            )
            candidates.append(candidate)
        
        # Tính content scores nếu có reference items
        content_scores = {}
        if reference_item_id or user_reference_items:
            try:
                if reference_item_id:
                    # Product detail page: tính similarity với item hiện tại
                    content_scores = self.content_recall_service.compute_content_scores(
                        candidate_item_ids=candidate_item_ids,
                        reference_item_id=reference_item_id
                    )
                    logger.info(f"Computed content scores for {len(content_scores)} candidates based on reference item {reference_item_id}")
                elif user_reference_items:
                    # Homepage: tính similarity với user history
                    content_scores = self.content_recall_service.compute_content_scores_batch(
                        candidate_item_ids=candidate_item_ids,
                        reference_item_ids=user_reference_items
                    )
                    logger.info(f"Computed content scores for {len(content_scores)} candidates based on {len(user_reference_items)} reference items")
            except Exception as e:
                logger.warning(f"Error computing content scores: {e}, continuing without content scores")
                content_scores = {}
        
        # Rank candidates
        try:
            ranked_items = self.ranking_service.rank_candidates(
                user_id=user_id,
                candidates=candidates,
                user_vector=user_vector,
                item_vectors=item_vectors if item_vectors else None,
                user2idx=self.recall_service._user2idx,
                idx2item=self.recall_service._idx2item,
                content_scores=content_scores if content_scores else None,
                content_score_boost=content_score_boost
            )
            ranking_count = len(ranked_items)
            logger.info(f"Ranking: {ranking_count} ranked items")
            
        except FileNotFoundError as e:
            logger.warning(f"Ranking model not found: {e}, using fallback")
            # Fallback: tạo mock ranked items từ candidates
            ranked_items = []
            for i, candidate in enumerate(candidates[:top_n * 2]):
                # Lấy category từ raw_signals nếu có
                category = None
                if candidate.raw_signals:
                    category = candidate.raw_signals.get('category') or candidate.raw_signals.get('main_category')
                
                ranked_items.append(
                    RankedItem(
                        item_id=candidate.item_id,
                        rank_score=0.9 - i * 0.01,
                        rank_position=i + 1,
                        category=category,
                        rating_number=candidate.raw_signals.get('rating_number') if candidate.raw_signals else None,
                        raw_signals=candidate.raw_signals
                    )
                )
            ranking_count = len(ranked_items)
            logger.info(f"Fallback ranking: {ranking_count} items")
        except Exception as e:
            logger.error(f"Error in ranking: {e}", exc_info=True)
            # Fallback: tạo mock ranked items
            ranked_items = []
            for i, candidate in enumerate(candidates[:top_n * 2]):
                category = None
                if candidate.raw_signals:
                    category = candidate.raw_signals.get('category') or candidate.raw_signals.get('main_category')
                
                ranked_items.append(
                    RankedItem(
                        item_id=candidate.item_id,
                        rank_score=0.9 - i * 0.01,
                        rank_position=i + 1,
                        category=category,
                        rating_number=candidate.raw_signals.get('rating_number') if candidate.raw_signals else None,
                        raw_signals=candidate.raw_signals
                    )
                )
            ranking_count = len(ranked_items)
            logger.info(f"Error fallback ranking: {ranking_count} items")
        
        # Step 3: Re-ranking
        try:
            reranked_items = self.reranking_service.rerank_items(
                user_id=str(user_id),
                ranked_items=ranked_items
            )
            
            logger.info(f"Re-ranking: {len(reranked_items)} final recommendations")
            
            return reranked_items, recall_count, ranking_count
            
        except Exception as e:
            logger.error(f"Error in re-ranking: {e}", exc_info=True)
            # Fallback: return ranked items (convert to ReRankedItem)
            fallback_items = []
            for i, item in enumerate(ranked_items[:top_n]):
                # Lấy category từ item
                category = item.category
                if not category and item.raw_signals:
                    category = item.raw_signals.get('category') or item.raw_signals.get('main_category')
                
                # Lấy rating_number từ item
                rating_number = item.rating_number
                if rating_number is None and item.raw_signals:
                    rating_number = item.raw_signals.get('rating_number')
                
                fallback_items.append(
                    ReRankedItem(
                        item_id=item.item_id,
                        rank_score=item.rank_score,
                        adjusted_score=item.rank_score,
                        rank_position=i + 1,
                        applied_rules=[],
                        category=category,
                        rating_number=rating_number
                    )
                )
            return fallback_items, recall_count, ranking_count


# Singleton instance
_recommendation_service_instance: Optional[RecommendationService] = None


def get_recommendation_service(
    artifacts_dir: Optional[Path] = None,
    k_mf: int = 100,
    k_pop: int = 50,
    top_n: int = 20
) -> RecommendationService:
    """
    Get singleton instance của RecommendationService.
    
    Args:
        artifacts_dir: Thư mục chứa artifacts
        k_mf: Số lượng items từ MF recall
        k_pop: Số lượng items từ popularity recall
        top_n: Số lượng recommendations
        
    Returns:
        RecommendationService instance
    """
    global _recommendation_service_instance
    
    if _recommendation_service_instance is None:
        _recommendation_service_instance = RecommendationService(
            artifacts_dir=artifacts_dir,
            k_mf=k_mf,
            k_pop=k_pop,
            top_n=top_n
        )
    
    return _recommendation_service_instance

