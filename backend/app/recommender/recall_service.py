"""
Candidate Recall Service
========================

Service để tạo danh sách candidate items cho recommendation.

Logic:
1. MF Recall: Dùng user_factors và item_factors để tính dot-product
2. Popularity Recall: Lấy top items theo popularity_score
3. Merge: Union và remove duplicates

Chưa làm ranking/re-ranking ở đây.
"""

import json
import logging
from pathlib import Path
from typing import List, Optional
import numpy as np
import pandas as pd
from functools import lru_cache

# Setup logging
logger = logging.getLogger(__name__)

# Default parameters
DEFAULT_K_MF = 100  # Số lượng items từ MF recall
DEFAULT_K_POP = 50  # Số lượng items từ popularity recall


class RecallService:
    """
    Service để thực hiện candidate recall.
    
    Load artifacts một lần và cache để tái sử dụng.
    """
    
    def __init__(
        self,
        artifacts_dir: Optional[Path] = None,
        k_mf: int = DEFAULT_K_MF,
        k_pop: int = DEFAULT_K_POP,
        k_content: int = 50  # Số lượng items từ content-based recall
    ):
        """
        Khởi tạo RecallService.
        
        Args:
            artifacts_dir: Thư mục chứa artifacts. Nếu None, dùng default.
            k_mf: Số lượng items từ MF recall
            k_pop: Số lượng items từ popularity recall
            k_content: Số lượng items từ content-based recall
        """
        if artifacts_dir is None:
            # Tìm project root
            base_dir = Path(__file__).resolve().parent.parent.parent
            artifacts_dir = base_dir / "artifacts"
        
        self.artifacts_dir = artifacts_dir
        self.k_mf = k_mf
        self.k_pop = k_pop
        self.k_content = k_content
        
        # Initialize content-based recall service (lazy)
        self._content_recall_service = None
        
        # Artifacts sẽ được load lazy
        self._user_factors: Optional[np.ndarray] = None
        self._item_factors: Optional[np.ndarray] = None
        self._user2idx: Optional[dict] = None
        self._idx2item: Optional[dict] = None
        self._popularity_df: Optional[pd.DataFrame] = None
        
        logger.info(f"RecallService initialized with k_mf={k_mf}, k_pop={k_pop}")
    
    def _load_mf_artifacts(self):
        """Load MF artifacts nếu chưa load."""
        if self._user_factors is not None:
            return  # Đã load rồi
        
        mf_dir = self.artifacts_dir / "mf"
        
        # Load user_factors
        user_factors_path = mf_dir / "user_factors.npy"
        if not user_factors_path.exists():
            raise FileNotFoundError(f"Không tìm thấy: {user_factors_path}")
        
        logger.debug(f"Loading {user_factors_path}")
        self._user_factors = np.load(str(user_factors_path))
        logger.debug(f"user_factors shape: {self._user_factors.shape}")
        
        # Load item_factors
        item_factors_path = mf_dir / "item_factors.npy"
        if not item_factors_path.exists():
            raise FileNotFoundError(f"Không tìm thấy: {item_factors_path}")
        
        logger.debug(f"Loading {item_factors_path}")
        self._item_factors = np.load(str(item_factors_path))
        logger.debug(f"item_factors shape: {self._item_factors.shape}")
        
        # Load user2idx
        user2idx_path = mf_dir / "user2idx.json"
        if not user2idx_path.exists():
            raise FileNotFoundError(f"Không tìm thấy: {user2idx_path}")
        
        logger.debug(f"Loading {user2idx_path}")
        with open(user2idx_path, 'r', encoding='utf-8') as f:
            self._user2idx = json.load(f)
        logger.debug(f"user2idx size: {len(self._user2idx)}")
        
        # Load idx2item
        idx2item_path = mf_dir / "idx2item.json"
        if not idx2item_path.exists():
            raise FileNotFoundError(f"Không tìm thấy: {idx2item_path}")
        
        logger.debug(f"Loading {idx2item_path}")
        with open(idx2item_path, 'r', encoding='utf-8') as f:
            idx2item_raw = json.load(f)
        # Convert keys từ string sang int
        self._idx2item = {int(k): v for k, v in idx2item_raw.items()}
        logger.debug(f"idx2item size: {len(self._idx2item)}")
        
        # Validate consistency
        if self._user_factors.shape[0] != len(self._user2idx):
            raise ValueError(
                f"Số users không khớp: {self._user_factors.shape[0]} vs {len(self._user2idx)}"
            )
        if self._item_factors.shape[0] != len(self._idx2item):
            raise ValueError(
                f"Số items không khớp: {self._item_factors.shape[0]} vs {len(self._idx2item)}"
            )
        
        logger.info("MF artifacts loaded successfully")
    
    def _load_popularity_data(self):
        """Load popularity data nếu chưa load."""
        if self._popularity_df is not None:
            return  # Đã load rồi
        
        # Tìm file ở nhiều nơi (fallback)
        possible_paths = [
            self.artifacts_dir / "popularity" / "item_popularity_normalized.parquet",
            self.artifacts_dir / "popularity" / "item_popularity.parquet",
            # Tìm trong data/processed (từ project root)
            Path(__file__).resolve().parent.parent.parent.parent / "data" / "processed" / "item_popularity.parquet",
            Path(__file__).resolve().parent.parent.parent.parent / "data" / "processed" / "item_popularity_normalized.parquet",
        ]
        
        popularity_path = None
        for path in possible_paths:
            if path.exists():
                popularity_path = path
                logger.info(f"Found popularity data at: {popularity_path}")
                break
        
        if popularity_path is None:
            logger.warning(
                f"Không tìm thấy popularity data ở các vị trí: {possible_paths}. "
                f"Popularity recall sẽ bị skip."
            )
            # Tạo empty DataFrame với đúng structure để tránh lỗi
            self._popularity_df = pd.DataFrame(columns=['item_id', 'popularity_score'])
            logger.warning("Using empty popularity DataFrame - popularity recall will return no items")
            return
        
        logger.debug(f"Loading {popularity_path}")
        self._popularity_df = pd.read_parquet(popularity_path)
        logger.debug(f"popularity_df shape: {self._popularity_df.shape}")
        logger.debug(f"popularity_df columns: {list(self._popularity_df.columns)}")
        
        # Validate và tính popularity_score nếu chưa có
        if 'item_id' not in self._popularity_df.columns:
            logger.error("Thiếu column 'item_id' trong popularity data")
            self._popularity_df = pd.DataFrame(columns=['item_id', 'popularity_score'])
            return
        
        # Nếu chưa có popularity_score, tính từ interaction_count và mean_rating
        if 'popularity_score' not in self._popularity_df.columns:
            logger.info("Column 'popularity_score' không có, sẽ tính từ interaction_count và mean_rating")
            
            # Normalize interaction_count (0-1 scale)
            if 'interaction_count' in self._popularity_df.columns:
                max_interactions = self._popularity_df['interaction_count'].max()
                if max_interactions > 0:
                    normalized_interactions = self._popularity_df['interaction_count'] / max_interactions
                else:
                    normalized_interactions = pd.Series([0.0] * len(self._popularity_df))
            else:
                normalized_interactions = pd.Series([0.5] * len(self._popularity_df))
            
            # Normalize mean_rating (0-1 scale, giả sử rating từ 1-5)
            if 'mean_rating' in self._popularity_df.columns:
                normalized_ratings = (self._popularity_df['mean_rating'] - 1.0) / 4.0  # Scale từ 1-5 -> 0-1
                normalized_ratings = normalized_ratings.fillna(0.5)  # Default 0.5 nếu NaN
            else:
                normalized_ratings = pd.Series([0.5] * len(self._popularity_df))
            
            # Tính popularity_score = 0.7 * normalized_interactions + 0.3 * normalized_ratings
            # Ưu tiên interaction_count hơn rating
            self._popularity_df['popularity_score'] = (
                0.7 * normalized_interactions + 0.3 * normalized_ratings
            )
            logger.info("Đã tính popularity_score từ interaction_count và mean_rating")
        
        logger.info("Popularity data loaded successfully")
    
    def _mf_recall(self, user_id: str) -> List[str]:
        """
        MF Recall: Tính dot-product và lấy top K_mf items.
        
        Args:
            user_id: User ID (string)
            
        Returns:
            List of item_id (strings)
        """
        # Load artifacts nếu chưa load
        self._load_mf_artifacts()
        
        # Kiểm tra user_id có trong user2idx không
        if user_id not in self._user2idx:
            logger.debug(f"User '{user_id}' không tồn tại trong user2idx, bỏ qua MF recall")
            return []
        
        # Lấy user index
        user_idx = self._user2idx[user_id]
        
        # Lấy user vector
        user_vector = self._user_factors[user_idx]
        
        # Tính dot-product với toàn bộ item_factors
        # item_factors shape: (num_items, latent_dim)
        # user_vector shape: (latent_dim,)
        # scores shape: (num_items,)
        scores = np.dot(self._item_factors, user_vector)
        
        # Lấy top K_mf items
        top_k_indices = np.argsort(scores)[::-1][:self.k_mf]
        
        # Convert indices sang item_id
        candidate_items = [self._idx2item[idx] for idx in top_k_indices]
        
        logger.debug(
            f"MF recall cho user '{user_id}': "
            f"top {len(candidate_items)} items (k_mf={self.k_mf})"
        )
        
        return candidate_items
    
    def _popularity_recall(
        self,
        exclude_items: Optional[List[str]] = None,
        shuffle: bool = True
    ) -> List[str]:
        """
        Popularity Recall: Lấy top K_pop items theo popularity_score.
        
        Args:
            exclude_items: List of items cần loại bỏ (ví dụ: items đã xem)
            shuffle: Có shuffle để tăng diversity không (default: True)
        
        Returns:
            List of item_id (strings)
        """
        # Load popularity data nếu chưa load
        self._load_popularity_data()
        
        # Kiểm tra nếu DataFrame rỗng
        if self._popularity_df is None or len(self._popularity_df) == 0:
            logger.warning("Popularity DataFrame is empty, returning empty list")
            return []
        
        # Sort theo popularity_score descending
        sorted_df = self._popularity_df.sort_values('popularity_score', ascending=False)
        
        # Filter excluded items nếu có
        if exclude_items:
            exclude_set = set(exclude_items)
            sorted_df = sorted_df[~sorted_df['item_id'].astype(str).isin(exclude_set)]
            logger.debug(f"Filtered out {len(exclude_set)} excluded items from popularity recall")
        
        # Lấy top K_pop * 2 để có buffer cho shuffle
        top_items_df = sorted_df.head(self.k_pop * 2)
        
        candidate_items = top_items_df['item_id'].astype(str).tolist()
        
        # Shuffle để tăng diversity (giữ top items nhưng randomize thứ tự)
        if shuffle and len(candidate_items) > self.k_pop:
            import random
            # Giữ top 20% items ở đầu, shuffle phần còn lại
            top_20_percent = int(len(candidate_items) * 0.2)
            top_items = candidate_items[:top_20_percent]
            rest_items = candidate_items[top_20_percent:]
            random.shuffle(rest_items)
            candidate_items = top_items + rest_items
        
        # Lấy top K_pop sau shuffle
        candidate_items = candidate_items[:self.k_pop]
        
        logger.debug(
            f"Popularity recall: top {len(candidate_items)} items (k_pop={self.k_pop}, "
            f"excluded={len(exclude_items) if exclude_items else 0}, shuffled={shuffle})"
        )
        
        return candidate_items
    
    def _get_content_recall_service(self):
        """Lazy initialization của content recall service."""
        if self._content_recall_service is None:
            try:
                from app.recommender.content_recall_service import ContentBasedRecallService
                self._content_recall_service = ContentBasedRecallService()
            except Exception as e:
                logger.warning(f"Could not initialize ContentBasedRecallService: {e}")
                self._content_recall_service = None
        return self._content_recall_service
    
    def _content_recall(
        self,
        reference_item_id: Optional[str] = None,
        user_reference_items: Optional[List[str]] = None,
        exclude_items: Optional[List[str]] = None
    ) -> List[str]:
        """
        Content-based Recall: Tìm items tương tự với reference items.
        
        Args:
            reference_item_id: Item ID để tìm similar items (ví dụ: item hiện tại ở product detail page)
            user_reference_items: List of item IDs từ user history (cart, purchases, views)
            exclude_items: List of items cần loại bỏ (ví dụ: items đã xem)
            
        Returns:
            List of item_id (strings)
        """
        content_service = self._get_content_recall_service()
        
        if not content_service:
            logger.debug("Content-based recall service not available, skipping")
            return []
        
        content_candidates = []
        
        # Case 1: Single reference item (product detail page)
        if reference_item_id:
            try:
                similar_items = content_service.find_similar_items(
                    item_id=reference_item_id,
                    top_k=self.k_content,
                    exclude_items=exclude_items or []
                )
                content_candidates = [item['item_id'] for item in similar_items]
                logger.debug(
                    f"Content recall (single reference): {len(content_candidates)} items "
                    f"for reference_item_id={reference_item_id}"
                )
            except Exception as e:
                logger.warning(f"Error in content recall for reference_item_id {reference_item_id}: {e}")
                return []
        
        # Case 2: Multiple reference items (user history)
        elif user_reference_items:
            try:
                # Tìm similar items cho từng reference item và merge
                all_similar_items = []
                for ref_item_id in user_reference_items[:10]:  # Giới hạn số reference items
                    similar_items = content_service.find_similar_items(
                        item_id=ref_item_id,
                        top_k=self.k_content // len(user_reference_items[:10]) + 5,  # Chia đều
                        exclude_items=exclude_items or []
                    )
                    all_similar_items.extend(similar_items)
                
                # Deduplicate và sort theo score
                seen_item_ids = set()
                unique_items = []
                for item in all_similar_items:
                    item_id = item.get('item_id')
                    if item_id and item_id not in seen_item_ids:
                        seen_item_ids.add(item_id)
                        unique_items.append(item)
                
                # Sort theo score và lấy top-K
                unique_items.sort(key=lambda x: x.get('score', 0.0), reverse=True)
                content_candidates = [item['item_id'] for item in unique_items[:self.k_content]]
                
                logger.debug(
                    f"Content recall (multiple references): {len(content_candidates)} items "
                    f"from {len(user_reference_items)} reference items"
                )
            except Exception as e:
                logger.warning(f"Error in content recall for user_reference_items: {e}")
                return []
        
        else:
            # Không có reference items, skip content recall
            logger.debug("No reference items provided, skipping content recall")
            return []
        
        return content_candidates
    
    def recall_candidates(
        self,
        user_id: str,
        reference_item_id: Optional[str] = None,
        user_reference_items: Optional[List[str]] = None,
        exclude_recent_items: Optional[List[str]] = None,
        use_only_content_recall: bool = False
    ) -> List[str]:
        """
        Main function: Recall candidates cho user.
        
        Logic:
        1. MF Recall (nếu user_id tồn tại, skip nếu use_only_content_recall=True)
        2. Popularity Recall (luôn chạy, skip nếu use_only_content_recall=True)
        3. Content-based Recall (nếu có reference items)
        4. Merge và remove duplicates
        
        Args:
            user_id: User ID (string)
            reference_item_id: Item ID để tìm similar items (optional, cho product detail page)
            user_reference_items: List of item IDs từ user history (optional, cho homepage)
            exclude_recent_items: List of items đã xem (từ Redis) để exclude khỏi Popularity recall
            use_only_content_recall: Nếu True, chỉ dùng Content-based recall (cho product detail page)
            
        Returns:
            List of item_id (strings), đã remove duplicates
        """
        logger.info(f"Starting recall for user_id: {user_id}, use_only_content_recall={use_only_content_recall}")
        
        # A. MF Recall (skip nếu chỉ dùng Content recall)
        mf_candidates = []
        num_mf = 0
        if not use_only_content_recall:
            mf_candidates = self._mf_recall(user_id)
            num_mf = len(mf_candidates)
        
        # B. Popularity Recall (skip nếu chỉ dùng Content recall)
        pop_candidates = []
        num_pop = 0
        if not use_only_content_recall:
            # Exclude items đã có trong MF và items đã xem
            exclude_for_popularity = list(set(mf_candidates))
            if exclude_recent_items:
                exclude_for_popularity.extend(exclude_recent_items)
                exclude_for_popularity = list(set(exclude_for_popularity))  # Remove duplicates
            pop_candidates = self._popularity_recall(
                exclude_items=exclude_for_popularity,
                shuffle=True  # Shuffle để tăng diversity
            )
            num_pop = len(pop_candidates)
        
        # C. Content-based Recall (nếu có reference items)
        content_candidates = []
        num_content = 0
        if reference_item_id or user_reference_items:
            # Exclude items đã có trong MF và Popularity recall (nếu có)
            exclude_items = list(set(mf_candidates + pop_candidates))
            content_candidates = self._content_recall(
                reference_item_id=reference_item_id,
                user_reference_items=user_reference_items,
                exclude_items=exclude_items
            )
            num_content = len(content_candidates)
        
        # Nếu chỉ dùng Content recall và không có candidates, log warning
        if use_only_content_recall and not content_candidates:
            logger.warning(
                f"Content-only recall returned no candidates for "
                f"reference_item_id={reference_item_id}, user_reference_items={user_reference_items}"
            )
        
        # D. Merge candidates
        # Union và remove duplicates (giữ thứ tự: MF > Content > Popularity)
        all_candidates = mf_candidates + content_candidates + pop_candidates
        # Remove duplicates nhưng giữ thứ tự
        seen = set()
        unique_candidates = []
        for item_id in all_candidates:
            if item_id not in seen:
                seen.add(item_id)
                unique_candidates.append(item_id)
        
        num_total = len(unique_candidates)
        
        # Log debug info
        logger.info(f"Recall results for user_id '{user_id}':")
        logger.info(f"  - MF recall: {num_mf} items")
        logger.info(f"  - Content recall: {num_content} items")
        logger.info(f"  - Popularity recall: {num_pop} items")
        logger.info(f"  - Total candidates (after merge): {num_total} items")
        
        # Log sample items
        sample_size = min(5, num_total)
        if sample_size > 0:
            sample_items = unique_candidates[:sample_size]
            logger.info(f"  - Sample item_ids: {sample_items}")
        
        return unique_candidates


# Singleton instance (optional, có thể tạo mới mỗi lần)
_recall_service_instance: Optional[RecallService] = None


def get_recall_service(
    artifacts_dir: Optional[Path] = None,
    k_mf: int = DEFAULT_K_MF,
    k_pop: int = DEFAULT_K_POP
) -> RecallService:
    """
    Get singleton instance của RecallService.
    
    Args:
        artifacts_dir: Thư mục artifacts (nếu None, dùng default)
        k_mf: Số lượng items từ MF recall
        k_pop: Số lượng items từ popularity recall
        
    Returns:
        RecallService instance
    """
    global _recall_service_instance
    
    if _recall_service_instance is None:
        _recall_service_instance = RecallService(
            artifacts_dir=artifacts_dir,
            k_mf=k_mf,
            k_pop=k_pop
        )
    
    return _recall_service_instance


# Convenience function
def recall_candidates(
    user_id: str,
    artifacts_dir: Optional[Path] = None,
    k_mf: int = DEFAULT_K_MF,
    k_pop: int = DEFAULT_K_POP
) -> List[str]:
    """
    Convenience function để recall candidates.
    
    Args:
        user_id: User ID (string)
        artifacts_dir: Thư mục artifacts (nếu None, dùng default)
        k_mf: Số lượng items từ MF recall
        k_pop: Số lượng items từ popularity recall
        
    Returns:
        List of item_id (strings)
    """
    service = get_recall_service(artifacts_dir=artifacts_dir, k_mf=k_mf, k_pop=k_pop)
    return service.recall_candidates(user_id)

