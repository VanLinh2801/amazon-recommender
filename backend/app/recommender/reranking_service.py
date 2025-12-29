"""
Re-ranking Service
==================

Service để re-rank items đã được rank bằng rule-based logic sử dụng Redis short-term memory.

Input: user_id, ranked_items (List[RankedItem])
Output: List[RankedItem] đã được re-rank (sort lại theo adjusted_score)

Rules:
1. Short-term intent boost (category)
2. Penalize recent items
3. Diversity penalty
4. Popularity floor (optional)
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import redis
from app.recommender.ranking_service import RankedItem

# Setup logging
logger = logging.getLogger(__name__)

# Debug flag
DEBUG_RERANKING = False


@dataclass
class ReRankedItem:
    """
    Item đã được re-rank với adjusted score và explanation.
    
    Attributes:
        item_id: Item ID
        rank_score: Original ranking score
        adjusted_score: Score sau khi áp dụng rules
        rank_position: Vị trí trong ranking (1-based)
        applied_rules: List of rule names đã áp dụng
        category: Item category (optional)
        rating_number: Number of ratings (optional)
    """
    item_id: str
    rank_score: float
    adjusted_score: float
    rank_position: int
    applied_rules: List[str] = field(default_factory=list)
    category: Optional[str] = None
    rating_number: Optional[int] = None


class ReRankingService:
    """
    Service để re-rank items sử dụng Redis short-term memory.
    
    Áp dụng rule-based logic để điều chỉnh scores.
    """
    
    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        top_n: int = 20,
        min_rating_threshold: int = 5,
        diversity_threshold: float = 0.25,  # Giảm xuống 25% để tăng diversity
        diversity_penalty: float = 0.7,  # Penalty mạnh hơn
        max_same_category: int = 4  # Giảm từ 5 xuống 4 items cùng category trong top-N
    ):
        """
        Khởi tạo ReRankingService.
        
        Args:
            redis_host: Redis host
            redis_port: Redis port
            redis_db: Redis database number
            top_n: Số lượng items top-N cần trả về
            min_rating_threshold: Threshold cho popularity floor rule
            diversity_threshold: Threshold cho diversity rule (0.0 - 1.0)
            diversity_penalty: Penalty factor cho diversity (giảm để penalty mạnh hơn)
            max_same_category: Tối đa số items cùng category trong top-N
        """
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True  # Decode responses to strings
        )
        self.top_n = top_n
        self.min_rating_threshold = min_rating_threshold
        self.diversity_threshold = diversity_threshold  # Giảm xuống 25% để tăng diversity
        self.diversity_penalty = diversity_penalty
        self.max_same_category = max_same_category  # Giảm từ 5 xuống 4 để tăng diversity
        
        logger.info(
            f"ReRankingService initialized: "
            f"redis={redis_host}:{redis_port}, top_n={top_n}, "
            f"diversity_threshold={diversity_threshold}, diversity_penalty={self.diversity_penalty}"
        )
    
    def _load_recent_items(self, user_id: str) -> List[str]:
        """
        Load recent items từ Redis.
        
        Args:
            user_id: User ID
            
        Returns:
            List of item_id (strings), mới nhất ở đầu
        """
        try:
            key = f"user:{user_id}:recent_items"
            items = self.redis_client.lrange(key, 0, -1)  # Lấy tất cả items
            logger.debug(f"Loaded {len(items)} recent items for user {user_id}")
            return items
        except redis.RedisError as e:
            logger.warning(f"Failed to load recent items from Redis: {e}")
            return []
        except Exception as e:
            logger.warning(f"Unexpected error loading recent items: {e}")
            return []
    
    def _load_recent_categories(self, user_id: str) -> Dict[str, int]:
        """
        Load recent categories từ Redis.
        
        Args:
            user_id: User ID
            
        Returns:
            Dict mapping category -> interaction_count
        """
        try:
            key = f"user:{user_id}:recent_categories"
            categories = self.redis_client.hgetall(key)
            # Convert values từ string sang int
            result = {cat: int(count) for cat, count in categories.items()}
            logger.debug(f"Loaded {len(result)} recent categories for user {user_id}")
            return result
        except redis.RedisError as e:
            logger.warning(f"Failed to load recent categories from Redis: {e}")
            return {}
        except Exception as e:
            logger.warning(f"Unexpected error loading recent categories: {e}")
            return {}
    
    def _apply_rule_intent_boost(
        self,
        item: RankedItem,
        recent_categories: Dict[str, int],
        adjusted_score: float,
        applied_rules: List[str]
    ) -> float:
        """
        Rule 1: Short-term intent boost (TĂNG CƯỜNG).
        
        Nếu item.category nằm trong recent_categories:
        - Boost mạnh hơn: adjusted_score *= (1 + min(0.4, 0.08 * interaction_count))
        - Tăng boost factor để items cùng category được ưu tiên hơn
        
        Args:
            item: RankedItem
            recent_categories: Dict of category -> count
            adjusted_score: Current adjusted score
            applied_rules: List to append rule name
            
        Returns:
            Adjusted score
        """
        # Lấy category từ item (có thể từ raw_signals hoặc metadata)
        category = None
        if hasattr(item, 'category') and item.category:
            category = item.category
        elif hasattr(item, 'raw_signals') and item.raw_signals:
            category = item.raw_signals.get('category') or item.raw_signals.get('main_category')
        
        if category and category in recent_categories:
            interaction_count = recent_categories[category]
            # Tăng boost: max 40% thay vì 20%, rate 0.08 thay vì 0.05
            boost_factor = min(0.4, 0.08 * interaction_count)
            adjusted_score *= (1.0 + boost_factor)
            applied_rules.append(f"intent_boost({category}:+{boost_factor:.2%})")
            logger.debug(
                f"  Intent boost for {item.item_id}: "
                f"category={category}, count={interaction_count}, "
                f"boost={boost_factor:.2%}"
            )
        
        return adjusted_score
    
    def _apply_rule_penalize_recent(
        self,
        item: RankedItem,
        recent_items: List[str],
        adjusted_score: float,
        applied_rules: List[str]
    ) -> float:
        """
        Rule 2: Penalize items vừa xem (TĂNG CƯỜNG).
        
        Penalty theo thứ tự recency:
        - Top 5 items gần nhất: penalty 0.2 (giảm 80%)
        - Top 10 items: penalty 0.4 (giảm 60%)
        - Còn lại: penalty 0.6 (giảm 40%)
        
        Args:
            item: RankedItem
            recent_items: List of recent item_ids (mới nhất ở đầu)
            adjusted_score: Current adjusted score
            applied_rules: List to append rule name
            
        Returns:
            Adjusted score
        """
        if item.item_id in recent_items:
            # Tìm vị trí trong recent_items (0 = mới nhất)
            position = recent_items.index(item.item_id)
            
            if position < 5:
                # Top 5 items gần nhất: penalty mạnh nhất
                penalty = 0.2
                applied_rules.append(f"recent_penalty_top5(-80%)")
            elif position < 10:
                # Top 10 items: penalty vừa
                penalty = 0.4
                applied_rules.append(f"recent_penalty_top10(-60%)")
            else:
                # Còn lại: penalty nhẹ
                penalty = 0.6
                applied_rules.append(f"recent_penalty(-40%)")
            
            adjusted_score *= penalty
            logger.debug(
                f"  Recent penalty for {item.item_id}: "
                f"position={position}, penalty={penalty:.1%}"
            )
        
        return adjusted_score
    
    def _apply_rule_diversity(
        self,
        item_category: Optional[str],
        top_items_categories: List[Optional[str]],
        adjusted_score: float,
        applied_rules: List[str]
    ) -> float:
        """
        Rule 3: Diversity penalty.
        
        Nếu top-N có quá nhiều items cùng category:
        Penalize các item trùng category: adjusted_score *= 0.85
        
        Args:
            item_category: Category của item hiện tại
            top_items_categories: List of categories của top items
            adjusted_score: Current adjusted score
            applied_rules: List to append rule name
            
        Returns:
            Adjusted score
        """
        if not item_category:
            return adjusted_score
        
        # Đếm số items cùng category trong top items
        category_count = sum(1 for cat in top_items_categories if cat == item_category)
        
        # Nếu category chiếm > threshold thì penalty
        if len(top_items_categories) > 0:
            category_ratio = category_count / len(top_items_categories)
            if category_ratio > self.diversity_threshold:
                adjusted_score *= self.diversity_penalty
                applied_rules.append(f"diversity_penalty({category_ratio:.1%})")
                logger.debug(
                    f"  Diversity penalty: category={item_category}, ratio={category_ratio:.1%}, "
                    f"penalty={self.diversity_penalty}"
                )
        
        # Thêm rule: Nếu đã có quá nhiều items cùng category trong top-N, penalty mạnh hơn
        if category_count >= self.max_same_category:
            adjusted_score *= 0.6  # Penalty mạnh hơn
            applied_rules.append(f"category_limit_exceeded({category_count})")
            logger.debug(
                f"  Category limit penalty: category={item_category}, count={category_count}"
            )
        
        return adjusted_score
    
    def _apply_rule_popularity_floor(
        self,
        item: RankedItem,
        adjusted_score: float,
        applied_rules: List[str]
    ) -> float:
        """
        Rule 4: Popularity floor (optional).
        
        Nếu item.rating_number < threshold:
        adjusted_score *= 0.9
        
        Args:
            item: RankedItem
            adjusted_score: Current adjusted score
            applied_rules: List to append rule name
            
        Returns:
            Adjusted score
        """
        # Lấy rating_number từ item
        rating_number = item.rating_number
        if rating_number is None and hasattr(item, 'raw_signals') and item.raw_signals:
            rating_number = item.raw_signals.get('rating_number')
        
        if rating_number is not None and rating_number < self.min_rating_threshold:
            adjusted_score *= 0.9
            applied_rules.append(f"popularity_floor(rating={rating_number})")
            logger.debug(
                f"  Popularity floor penalty for {item.item_id}: "
                f"rating_number={rating_number}"
            )
        
        return adjusted_score
    
    def rerank_items(
        self,
        user_id: str,
        ranked_items: List[RankedItem]
    ) -> List[ReRankedItem]:
        """
        Re-rank items sử dụng rule-based logic.
        
        Args:
            user_id: User ID
            ranked_items: List of RankedItem đã được rank
            
        Returns:
            List of ReRankedItem đã được re-rank và sort theo adjusted_score DESC
        """
        if not ranked_items:
            logger.warning(f"No items to re-rank for user {user_id}")
            return []
        
        logger.info(f"Re-ranking {len(ranked_items)} items for user_id: {user_id}")
        
        # Load Redis context
        recent_items = self._load_recent_items(user_id)
        recent_categories = self._load_recent_categories(user_id)
        
        logger.debug(
            f"Redis context: {len(recent_items)} recent items, "
            f"{len(recent_categories)} recent categories"
        )
        
        # Apply rules cho từng item
        reranked_items = []
        
        for item in ranked_items:
            # Khởi tạo adjusted_score = rank_score
            adjusted_score = item.rank_score
            applied_rules = []
            
            # Lấy metadata từ item
            category = item.category
            if not category and hasattr(item, 'raw_signals') and item.raw_signals:
                category = item.raw_signals.get('category') or item.raw_signals.get('main_category')
            
            rating_number = item.rating_number
            if rating_number is None and hasattr(item, 'raw_signals') and item.raw_signals:
                rating_number = item.raw_signals.get('rating_number')
            
            # Rule 1: Intent boost
            adjusted_score = self._apply_rule_intent_boost(
                item, recent_categories, adjusted_score, applied_rules
            )
            
            # Rule 2: Penalize recent items
            adjusted_score = self._apply_rule_penalize_recent(
                item, recent_items, adjusted_score, applied_rules
            )
            
            # Rule 3: Diversity (cần top items để tính)
            # Tạm thời skip, sẽ áp dụng sau khi có top items
            # adjusted_score = self._apply_rule_diversity(
            #     item, ranked_items[:self.top_n], adjusted_score, applied_rules
            # )
            
            # Rule 4: Popularity floor
            adjusted_score = self._apply_rule_popularity_floor(
                item, adjusted_score, applied_rules
            )
            
            # Tạo ReRankedItem
            reranked_item = ReRankedItem(
                item_id=item.item_id,
                rank_score=item.rank_score,
                adjusted_score=adjusted_score,
                rank_position=0,  # Will be updated after sorting
                applied_rules=applied_rules,
                category=category,
                rating_number=rating_number
            )
            
            reranked_items.append(reranked_item)
        
        # Sort theo adjusted_score
        reranked_items_sorted = sorted(
            reranked_items,
            key=lambda x: x.adjusted_score,
            reverse=True
        )
        
        # Apply diversity rule cho top items (iterative để đảm bảo diversity tốt)
        # Lấy top items để tính diversity (lấy nhiều hơn để có buffer)
        top_items_for_diversity = reranked_items_sorted[:min(self.top_n * 3, len(reranked_items_sorted))]
        
        # Tính category distribution trong top items
        category_counts = {}
        for item in top_items_for_diversity:
            if item.category:
                category_counts[item.category] = category_counts.get(item.category, 0) + 1
        
        # Áp dụng diversity penalty cho top items (iterative để đảm bảo diversity tốt)
        max_iterations = 3
        for iteration in range(max_iterations):
            # Sort lại sau mỗi iteration
            top_items_for_diversity = sorted(
                top_items_for_diversity,
                key=lambda x: x.adjusted_score,
                reverse=True
            )
            
            # Recalculate category counts
            category_counts = {}
            for item in top_items_for_diversity[:self.top_n * 2]:
                if item.category:
                    category_counts[item.category] = category_counts.get(item.category, 0) + 1
            
            # Apply penalties
            penalty_applied = False
            for reranked_item in top_items_for_diversity[:self.top_n * 2]:
                if reranked_item.category:
                    category_count = category_counts.get(reranked_item.category, 0)
                    total_top = len([x for x in top_items_for_diversity[:self.top_n * 2] if x.category])
                    category_ratio = category_count / total_top if total_top > 0 else 0
                    
                    # Penalty nếu category chiếm quá nhiều (giảm threshold)
                    if category_ratio > self.diversity_threshold:
                        penalty = self.diversity_penalty
                        reranked_item.adjusted_score *= penalty
                        penalty_applied = True
                        if f"diversity_penalty({category_ratio:.1%})" not in reranked_item.applied_rules:
                            reranked_item.applied_rules.append(
                                f"diversity_penalty({category_ratio:.1%})"
                            )
                    
                    # Penalty nếu vượt quá max_same_category (giảm threshold)
                    if category_count > self.max_same_category:
                        penalty = 0.5  # Penalty mạnh hơn
                        reranked_item.adjusted_score *= penalty
                        penalty_applied = True
                        if f"category_limit_exceeded({category_count})" not in reranked_item.applied_rules:
                            reranked_item.applied_rules.append(
                                f"category_limit_exceeded({category_count})"
                            )
            
            # Nếu không có penalty nào được áp dụng, dừng iteration
            if not penalty_applied:
                break
        
        # Sort lại sau khi áp dụng diversity
        reranked_items_sorted = sorted(
            reranked_items,
            key=lambda x: x.adjusted_score,
            reverse=True
        )
        
        # Deduplication: Loại bỏ items trùng lặp
        # 1. Deduplicate theo item_id (ASIN)
        seen_item_ids = set()
        # 2. Deduplicate theo parent_asin (để tránh recommend nhiều variants cùng sản phẩm)
        seen_parent_asins = set()
        deduplicated_items = []
        
        for item in reranked_items_sorted:
            # Lấy parent_asin từ raw_signals hoặc từ item_id (nếu item_id là parent_asin)
            parent_asin = None
            if hasattr(item, 'raw_signals') and item.raw_signals:
                parent_asin = item.raw_signals.get('parent_asin')
            
            # Nếu không có parent_asin trong raw_signals, giả định item_id có thể là parent_asin
            # (trong recall, item_id thường là parent_asin)
            if not parent_asin:
                # Tạm thời dùng item_id làm parent_asin nếu không có thông tin khác
                # Sẽ được xử lý đúng ở API layer khi fetch từ database
                parent_asin = item.item_id
            
            # Check duplicate theo cả item_id và parent_asin
            is_duplicate = False
            
            # Check duplicate theo item_id
            if item.item_id in seen_item_ids:
                is_duplicate = True
                logger.debug(f"  Duplicate item_id: {item.item_id}")
            
            # Check duplicate theo parent_asin (quan trọng hơn)
            if parent_asin in seen_parent_asins:
                is_duplicate = True
                logger.debug(f"  Duplicate parent_asin: {parent_asin} (item_id: {item.item_id})")
            
            if not is_duplicate:
                seen_item_ids.add(item.item_id)
                seen_parent_asins.add(parent_asin)
                deduplicated_items.append(item)
        
        logger.info(
            f"Deduplication: {len(reranked_items_sorted)} → {len(deduplicated_items)} items "
            f"(removed {len(reranked_items_sorted) - len(deduplicated_items)} duplicates)"
        )
        
        # Lấy top-N sau deduplication
        final_items = deduplicated_items[:self.top_n]
        
        # Update rank_position
        for idx, item in enumerate(final_items):
            item.rank_position = idx + 1
        
        logger.info(
            f"Re-ranking complete: {len(final_items)} items "
            f"(from {len(reranked_items)} candidates, "
            f"deduplicated from {len(reranked_items_sorted)})"
        )
        
        # Log top 5 items với explainability
        if DEBUG_RERANKING or logger.isEnabledFor(logging.INFO):
            logger.info("Top 5 re-ranked items:")
            for item in final_items[:5]:
                logger.info(
                    f"  Rank {item.rank_position}: {item.item_id} | "
                    f"rank_score={item.rank_score:.6f} → "
                    f"adjusted_score={item.adjusted_score:.6f} | "
                    f"rules={item.applied_rules if item.applied_rules else ['none']}"
                )
        
        return final_items


# Singleton instance
_reranking_service_instance: Optional[ReRankingService] = None


def get_reranking_service(
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_db: int = 0,
    top_n: int = 20
) -> ReRankingService:
    """
    Get singleton instance của ReRankingService.
    
    Args:
        redis_host: Redis host
        redis_port: Redis port
        redis_db: Redis database number
        top_n: Số lượng items top-N
        
    Returns:
        ReRankingService instance
    """
    global _reranking_service_instance
    
    if _reranking_service_instance is None:
        _reranking_service_instance = ReRankingService(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db,
            top_n=top_n
        )
    
    return _reranking_service_instance


# Convenience function
def rerank_items(
    user_id: str,
    ranked_items: List[RankedItem],
    redis_host: str = "localhost",
    redis_port: int = 6379,
    top_n: int = 20
) -> List[ReRankedItem]:
    """
    Convenience function để re-rank items.
    
    Args:
        user_id: User ID
        ranked_items: List of RankedItem
        redis_host: Redis host
        redis_port: Redis port
        top_n: Số lượng items top-N
        
    Returns:
        List of ReRankedItem
    """
    service = get_reranking_service(
        redis_host=redis_host,
        redis_port=redis_port,
        top_n=top_n
    )
    return service.rerank_items(user_id, ranked_items)

