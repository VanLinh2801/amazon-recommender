"""
Recommendation Pipeline Example
===============================

Ví dụ sử dụng full pipeline: Recall -> Ranking -> Re-ranking

Usage:
    python -m app.recommender.recommendation_pipeline
"""

import sys
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Thêm root directory vào path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

import numpy as np
from app.recommender.recall_service import RecallService
from app.recommender.ranking_service import RankingService, ItemCandidate, RankedItem
from app.recommender.reranking_service import ReRankingService


def full_pipeline_example(user_id: str):
    """
    Ví dụ full pipeline: Recall -> Ranking -> Re-ranking.
    
    Args:
        user_id: User ID
    """
    print("\n" + "=" * 80)
    print("FULL RECOMMENDATION PIPELINE")
    print("=" * 80)
    print(f"\nUser ID: {user_id}")
    
    artifacts_dir = BASE_DIR / "artifacts"
    
    # Step 1: Recall
    print("\n" + "-" * 80)
    print("STEP 1: RECALL")
    print("-" * 80)
    
    recall_service = RecallService(artifacts_dir=artifacts_dir, k_mf=100, k_pop=50)
    candidate_item_ids = recall_service.recall_candidates(user_id)
    
    print(f"✅ Recall: {len(candidate_item_ids)} candidates")
    print(f"   Sample: {candidate_item_ids[:5]}")
    
    # Step 2: Convert to ItemCandidate và Rank
    print("\n" + "-" * 80)
    print("STEP 2: RANKING")
    print("-" * 80)
    
    # Load MF artifacts để tính MF scores
    recall_service._load_mf_artifacts()
    recall_service._load_popularity_data()
    
    # Tạo ItemCandidate objects
    candidates = []
    mf_scores = {}
    
    # Tính MF scores nếu user có trong MF
    if user_id in recall_service._user2idx:
        user_idx = recall_service._user2idx[user_id]
        user_vector = recall_service._user_factors[user_idx]
        
        for item_id in candidate_item_ids:
            # Tìm item index
            item_idx = None
            for idx, mapped_item_id in recall_service._idx2item.items():
                if mapped_item_id == item_id:
                    item_idx = idx
                    break
            
            if item_idx is not None:
                item_vector = recall_service._item_factors[item_idx]
                mf_score = float(np.dot(item_vector, user_vector))
                mf_scores[item_id] = mf_score
    
    # Lấy popularity và rating từ data
    popularity_df = recall_service._popularity_df
    
    for item_id in candidate_item_ids:
        # Lấy popularity và rating
        item_data = popularity_df[popularity_df['item_id'].astype(str) == item_id]
        
        raw_signals = {}
        if not item_data.empty:
            if 'popularity_score' in item_data.columns:
                raw_signals['popularity_score'] = float(item_data['popularity_score'].iloc[0])
            if 'rating_score' in item_data.columns:
                raw_signals['rating_score'] = float(item_data['rating_score'].iloc[0])
        
        candidate = ItemCandidate(
            item_id=item_id,
            mf_score=mf_scores.get(item_id),
            content_score=None,
            raw_signals=raw_signals if raw_signals else None
        )
        candidates.append(candidate)
    
    # Rank candidates
    ranking_service = RankingService(artifacts_dir=artifacts_dir, top_n=50)
    
    try:
        # Get user and item vectors
        user_vector = None
        item_vectors = {}
        
        if user_id in recall_service._user2idx:
            user_idx = recall_service._user2idx[user_id]
            user_vector = recall_service._user_factors[user_idx]
            
            for candidate in candidates:
                for idx, mapped_item_id in recall_service._idx2item.items():
                    if mapped_item_id == candidate.item_id:
                        item_vectors[candidate.item_id] = recall_service._item_factors[idx]
                        break
        
        ranked_items = ranking_service.rank_candidates(
            user_id=user_id,
            candidates=candidates,
            user_vector=user_vector,
            item_vectors=item_vectors if item_vectors else None,
            user2idx=recall_service._user2idx,
            idx2item=recall_service._idx2item
        )
        
        print(f"✅ Ranking: {len(ranked_items)} ranked items")
        print(f"   Top 3: {[item.item_id for item in ranked_items[:3]]}")
        
    except FileNotFoundError:
        print("⚠️  Ranking model not found, skipping ranking step")
        print("   Creating mock ranked items...")
        
        # Tạo mock ranked items
        ranked_items = []
        for i, item_id in enumerate(candidate_item_ids[:20]):
            ranked_items.append(
                RankedItem(
                    item_id=item_id,
                    rank_score=0.9 - i * 0.01,
                    rank_position=i + 1,
                    category="Electronics" if i % 2 == 0 else "Fashion",
                    rating_number=100 * (i + 1)
                )
            )
        print(f"✅ Mock ranking: {len(ranked_items)} items")
    
    # Step 3: Re-ranking
    print("\n" + "-" * 80)
    print("STEP 3: RE-RANKING")
    print("-" * 80)
    
    reranking_service = ReRankingService(top_n=20)
    
    try:
        reranked_items = reranking_service.rerank_items(user_id, ranked_items)
        
        print(f"✅ Re-ranking: {len(reranked_items)} final recommendations")
        
        print(f"\n{'Rank':<8} {'Item ID':<20} {'Original':<12} {'Adjusted':<12} {'Rules'}")
        print("-" * 80)
        
        for item in reranked_items[:10]:
            rules_str = ", ".join(item.applied_rules) if item.applied_rules else "none"
            print(
                f"{item.rank_position:<8} {item.item_id:<20} "
                f"{item.rank_score:<12.6f} {item.adjusted_score:<12.6f} {rules_str}"
            )
        
    except Exception as e:
        print(f"⚠️  Re-ranking error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("✅ PIPELINE HOÀN TẤT!")
    print("=" * 80)


if __name__ == "__main__":
    # Test với user có trong MF
    artifacts_dir = BASE_DIR / "artifacts"
    recall_service = RecallService(artifacts_dir=artifacts_dir)
    recall_service._load_mf_artifacts()
    test_user_id = list(recall_service._user2idx.keys())[0]
    
    full_pipeline_example(test_user_id)

