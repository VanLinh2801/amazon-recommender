"""
Content-Based Recall Service
============================

Service để tính content similarity scores sử dụng Qdrant embeddings.
"""

import logging
from typing import List, Dict, Optional
import numpy as np

logger = logging.getLogger(__name__)


class ContentBasedRecallService:
    """
    Service để tính content similarity scores.
    
    Sử dụng Qdrant để tìm items tương tự dựa trên embeddings.
    """
    
    def __init__(self, qdrant_url: str = "http://localhost:6333"):
        """
        Khởi tạo ContentBasedRecallService.
        
        Args:
            qdrant_url: URL của Qdrant server
        """
        self.qdrant_url = qdrant_url
        self._qdrant_manager = None
        self._initialized = False
        
    def _init_qdrant(self):
        """Lazy initialization của Qdrant manager."""
        if self._initialized:
            return
            
        try:
            from vector_db.qdrant.qdrant_manager import QdrantManager
            self._qdrant_manager = QdrantManager(url=self.qdrant_url)
            
            if self._qdrant_manager.connect():
                self._initialized = True
                logger.info("ContentBasedRecallService initialized with Qdrant")
            else:
                logger.warning("Could not connect to Qdrant, content-based recall will be disabled")
                self._qdrant_manager = None
        except Exception as e:
            logger.warning(f"Could not initialize Qdrant: {e}, content-based recall will be disabled")
            self._qdrant_manager = None
    
    def get_item_vector(self, item_id: str) -> Optional[np.ndarray]:
        """
        Lấy vector embedding của một item.
        
        Args:
            item_id: Item ID
            
        Returns:
            Vector embedding hoặc None nếu không tìm thấy
        """
        self._init_qdrant()
        
        if not self._qdrant_manager:
            return None
            
        try:
            return self._qdrant_manager.get_item_vector(item_id)
        except Exception as e:
            # Chỉ log warning nếu là lỗi thực sự (không phải "not found")
            if "not found" not in str(e).lower():
                logger.debug(f"Could not get vector for item {item_id}: {e}")
            return None
    
    def compute_content_scores(
        self,
        candidate_item_ids: List[str],
        reference_item_id: Optional[str] = None,
        reference_vector: Optional[np.ndarray] = None
    ) -> Dict[str, float]:
        """
        Tính content similarity scores cho candidates.
        
        Args:
            candidate_item_ids: Danh sách item IDs cần tính score
            reference_item_id: Item ID để so sánh (nếu có)
            reference_vector: Vector embedding để so sánh (nếu có, ưu tiên hơn reference_item_id)
            
        Returns:
            Dict mapping item_id -> content_score (0.0 - 1.0)
        """
        self._init_qdrant()
        
        if not self._qdrant_manager:
            # Nếu không có Qdrant, trả về scores = 0.0
            return {item_id: 0.0 for item_id in candidate_item_ids}
        
        # Lấy reference vector
        if reference_vector is None and reference_item_id:
            reference_vector = self.get_item_vector(reference_item_id)
        
        if reference_vector is None:
            # Nếu không có reference, trả về scores = 0.0
            logger.debug("No reference vector/item_id provided, returning zero scores")
            return {item_id: 0.0 for item_id in candidate_item_ids}
        
        # Normalize reference vector
        ref_norm = np.linalg.norm(reference_vector)
        if ref_norm > 0:
            reference_vector = reference_vector / ref_norm
        else:
            logger.warning("Reference vector has zero norm")
            return {item_id: 0.0 for item_id in candidate_item_ids}
        
        # Tính similarity với từng candidate
        content_scores = {}
        
        for item_id in candidate_item_ids:
            try:
                # Lấy vector của candidate
                candidate_vector = self.get_item_vector(item_id)
                
                if candidate_vector is None:
                    content_scores[item_id] = 0.0
                    continue
                
                # Normalize candidate vector
                cand_norm = np.linalg.norm(candidate_vector)
                if cand_norm > 0:
                    candidate_vector = candidate_vector / cand_norm
                else:
                    content_scores[item_id] = 0.0
                    continue
                
                # Cosine similarity (dot product vì đã normalize)
                similarity = float(np.dot(reference_vector, candidate_vector))
                
                # Clip về [0, 1] (cosine similarity có thể âm nếu vectors ngược hướng)
                content_scores[item_id] = max(0.0, min(1.0, similarity))
                
            except Exception as e:
                logger.warning(f"Error computing content score for {item_id}: {e}")
                content_scores[item_id] = 0.0
        
        return content_scores
    
    def compute_content_scores_batch(
        self,
        candidate_item_ids: List[str],
        reference_item_ids: List[str],
        weights: Optional[List[float]] = None
    ) -> Dict[str, float]:
        """
        Tính content scores dựa trên nhiều reference items (ví dụ: user history).
        
        Args:
            candidate_item_ids: Danh sách item IDs cần tính score
            reference_item_ids: Danh sách reference item IDs (ví dụ: items trong cart)
            weights: Weights cho từng reference item (nếu None, dùng equal weights)
            
        Returns:
            Dict mapping item_id -> content_score (0.0 - 1.0)
        """
        if not reference_item_ids:
            return {item_id: 0.0 for item_id in candidate_item_ids}
        
        # Equal weights nếu không có weights
        if weights is None:
            weights = [1.0 / len(reference_item_ids)] * len(reference_item_ids)
        else:
            # Normalize weights
            total_weight = sum(weights)
            if total_weight > 0:
                weights = [w / total_weight for w in weights]
            else:
                weights = [1.0 / len(reference_item_ids)] * len(reference_item_ids)
        
        # Tính scores cho từng reference item
        all_scores = []
        for ref_item_id, weight in zip(reference_item_ids, weights):
            scores = self.compute_content_scores(
                candidate_item_ids=candidate_item_ids,
                reference_item_id=ref_item_id
            )
            # Weighted scores
            weighted_scores = {item_id: score * weight for item_id, score in scores.items()}
            all_scores.append(weighted_scores)
        
        # Aggregate scores (sum of weighted similarities)
        final_scores = {}
        for item_id in candidate_item_ids:
            aggregated_score = sum(scores.get(item_id, 0.0) for scores in all_scores)
            # Normalize về [0, 1] (có thể > 1 nếu nhiều reference items)
            final_scores[item_id] = min(1.0, aggregated_score)
        
        return final_scores
    
    def find_similar_items(
        self,
        item_id: str,
        top_k: int = 10,
        exclude_items: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Tìm items tương tự với item_id sử dụng Qdrant search.
        
        Args:
            item_id: Item ID để tìm similar items
            top_k: Số lượng items tương tự
            exclude_items: Danh sách items cần loại bỏ (ví dụ: item hiện tại)
            
        Returns:
            List of dicts với keys: item_id, score
        """
        self._init_qdrant()
        
        if not self._qdrant_manager:
            return []
        
        # Lấy vector của item
        query_vector = self.get_item_vector(item_id)
        if query_vector is None:
            return []
        
        # Search similar items
        try:
            results = self._qdrant_manager.search_similar_items(
                query_vector=query_vector,
                top_k=top_k + (len(exclude_items) if exclude_items else 0)
            )
            
            # Filter excluded items
            if exclude_items:
                exclude_set = set(exclude_items)
                results = [r for r in results if r.get('item_id') not in exclude_set]
            
            # Limit to top_k
            results = results[:top_k]
            
            return results
            
        except Exception as e:
            logger.warning(f"Error finding similar items for {item_id}: {e}")
            return []


