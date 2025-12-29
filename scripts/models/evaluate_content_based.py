"""
Evaluate Content-Based Recommendations
======================================
Đánh giá content-based recommendations với các metrics:
- Precision@K, Recall@K
- Coverage
- Diversity (category diversity)
- Similarity scores distribution
- Hit rate

Chạy: python scripts/models/evaluate_content_based.py
"""

import sys
import io
from pathlib import Path
import json
import numpy as np
import polars as pl
from typing import Dict, List, Tuple, Set
from collections import Counter

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm root vào path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(BASE_DIR / "backend"))

from backend.app.recommender.content_recall_service import ContentBasedRecallService
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def load_test_data(test_path: Path) -> pl.DataFrame:
    """Load test data."""
    print("\n[1] Đang load test data...")
    test_df = pl.read_parquet(str(test_path))
    print(f"[OK] Test data: {len(test_df):,} samples")
    print(f"  Users: {test_df['user_id'].n_unique():,}")
    print(f"  Items: {test_df['item_id'].n_unique():,}")
    return test_df


def load_metadata(metadata_path: Path) -> pl.DataFrame:
    """Load metadata để lấy category."""
    print("\n[2] Đang load metadata...")
    metadata_df = pl.read_parquet(str(metadata_path))
    print(f"[OK] Metadata: {len(metadata_df):,} items")
    
    # Tạo mapping item_id -> category
    if 'main_category' in metadata_df.columns:
        item_category_map = dict(
            zip(metadata_df['parent_asin'].to_list(), metadata_df['main_category'].to_list())
        )
    else:
        item_category_map = {}
    
    return item_category_map


def evaluate_content_based_recall(
    content_service: ContentBasedRecallService,
    test_df: pl.DataFrame,
    item_category_map: Dict[str, str],
    k: int = 10,
    threshold: float = 4.0
) -> Dict:
    """
    Đánh giá content-based recall.
    
    Logic:
    1. Với mỗi user trong test set, lấy items họ đã rate >= threshold
    2. Với mỗi positive item, tìm top-K similar items
    3. Kiểm tra xem các similar items có nằm trong test set của user không
    4. Tính Precision@K, Recall@K
    """
    print(f"\n[3] Đang đánh giá Content-Based Recall (K={k}, threshold={threshold})...")
    
    # Group test data theo user
    test_by_user = test_df.group_by("user_id").agg([
        pl.col("item_id").alias("test_items"),
        pl.col("rating").alias("test_ratings")
    ])
    
    precisions = []
    recalls = []
    hit_rates = []
    similarity_scores = []
    category_diversities = []
    valid_users = 0
    total_recommendations = 0
    unique_recommended_items = set()
    
    for row in test_by_user.iter_rows(named=True):
        user_id = row["user_id"]
        test_items = row["test_items"]
        test_ratings = row["test_ratings"]
        
        # Lấy positive items (rating >= threshold)
        positive_items = [
            item for item, rating in zip(test_items, test_ratings)
            if rating >= threshold
        ]
        
        if len(positive_items) == 0:
            continue
        
        valid_users += 1
        
        # Với mỗi positive item, tìm similar items
        user_recommendations = set()
        user_similarities = []
        user_categories = []
        
        for positive_item in positive_items[:5]:  # Giới hạn 5 items để tránh quá nhiều
            try:
                # Tìm similar items
                similar_items = content_service.find_similar_items(
                    item_id=positive_item,
                    top_k=k * 2,  # Lấy nhiều hơn để có đủ sau khi filter
                    exclude_items=None
                )
                
                if similar_items:
                    # similar_items là list of dicts với keys: item_id, score
                    for item_result in similar_items[:k]:
                        item_id = item_result.get('item_id') or item_result.get('id')
                        similarity = item_result.get('score') or item_result.get('similarity', 0.0)
                        
                        if item_id:
                            user_recommendations.add(item_id)
                            user_similarities.append(float(similarity))
                            
                            # Lấy category (có thể là parent_asin)
                            category = item_category_map.get(item_id, "Unknown")
                            if category == "Unknown":
                                # Thử tìm với parent_asin
                                category = item_category_map.get(positive_item, "Unknown")
                            user_categories.append(category)
                            
                            unique_recommended_items.add(item_id)
                
            except Exception as e:
                # Item không có embedding, skip
                logger.debug(f"Could not find similar items for {positive_item}: {e}")
                continue
        
        if len(user_recommendations) == 0:
            continue
        
        total_recommendations += len(user_recommendations)
        
        # Tính precision và recall
        # Ground truth: positive items trong test set
        positive_set = set(positive_items)
        
        # Recommended items có nằm trong positive set không?
        hits = user_recommendations & positive_set
        hit_count = len(hits)
        
        precision = hit_count / len(user_recommendations) if len(user_recommendations) > 0 else 0.0
        recall = hit_count / len(positive_set) if len(positive_set) > 0 else 0.0
        hit_rate = 1.0 if hit_count > 0 else 0.0
        
        precisions.append(precision)
        recalls.append(recall)
        hit_rates.append(hit_rate)
        
        # Similarity scores
        if user_similarities:
            similarity_scores.extend(user_similarities)
        
        # Category diversity (số lượng unique categories / tổng số recommendations)
        if user_categories:
            unique_cats = len(set(user_categories))
            total_cats = len(user_categories)
            diversity = unique_cats / total_cats if total_cats > 0 else 0.0
            category_diversities.append(diversity)
    
    # Tính metrics
    avg_precision = np.mean(precisions) if precisions else 0.0
    avg_recall = np.mean(recalls) if recalls else 0.0
    avg_hit_rate = np.mean(hit_rates) if hit_rates else 0.0
    avg_similarity = np.mean(similarity_scores) if similarity_scores else 0.0
    avg_diversity = np.mean(category_diversities) if category_diversities else 0.0
    
    # Coverage: số lượng unique items được recommend / tổng số items
    total_items = test_df['item_id'].n_unique()
    coverage = len(unique_recommended_items) / total_items if total_items > 0 else 0.0
    
    print(f"  Valid users: {valid_users:,}")
    print(f"  Total recommendations: {total_recommendations:,}")
    print(f"  Unique recommended items: {len(unique_recommended_items):,}")
    print(f"  Precision@{k}: {avg_precision:.4f} ({avg_precision*100:.2f}%)")
    print(f"  Recall@{k}: {avg_recall:.4f} ({avg_recall*100:.2f}%)")
    print(f"  Hit Rate: {avg_hit_rate:.4f} ({avg_hit_rate*100:.2f}%)")
    print(f"  Coverage: {coverage:.4f} ({coverage*100:.2f}%)")
    print(f"  Avg Similarity: {avg_similarity:.4f}")
    print(f"  Category Diversity: {avg_diversity:.4f} ({avg_diversity*100:.2f}%)")
    
    return {
        'precision': avg_precision,
        'recall': avg_recall,
        'hit_rate': avg_hit_rate,
        'coverage': coverage,
        'avg_similarity': avg_similarity,
        'category_diversity': avg_diversity,
        'valid_users': valid_users,
        'total_recommendations': total_recommendations,
        'unique_recommended_items': len(unique_recommended_items)
    }


def evaluate_similarity_quality(
    content_service: ContentBasedRecallService,
    test_df: pl.DataFrame,
    k: int = 10
) -> Dict:
    """
    Đánh giá chất lượng similarity scores.
    """
    print(f"\n[4] Đang đánh giá Similarity Quality...")
    
    # Lấy một số items từ test set
    sample_items = test_df['item_id'].unique().to_list()[:50]
    
    similarity_scores = []
    recommendations_per_item = []
    
    for item_id in sample_items:
        try:
            similar_items = content_service.find_similar_items(
                reference_item_id=item_id,
                top_k=k,
                min_similarity=0.0
            )
            
            if similar_items:
                recommendations_per_item.append(len(similar_items))
                for _, similarity in similar_items:
                    similarity_scores.append(similarity)
        except Exception as e:
            continue
    
    if not similarity_scores:
        return {
            'avg_similarity': 0.0,
            'min_similarity': 0.0,
            'max_similarity': 0.0,
            'median_similarity': 0.0,
            'avg_recommendations_per_item': 0.0
        }
    
    print(f"  Items evaluated: {len(sample_items)}")
    print(f"  Avg similarity: {np.mean(similarity_scores):.4f}")
    print(f"  Min similarity: {np.min(similarity_scores):.4f}")
    print(f"  Max similarity: {np.max(similarity_scores):.4f}")
    print(f"  Median similarity: {np.median(similarity_scores):.4f}")
    print(f"  Avg recommendations per item: {np.mean(recommendations_per_item):.2f}")
    
    return {
        'avg_similarity': float(np.mean(similarity_scores)),
        'min_similarity': float(np.min(similarity_scores)),
        'max_similarity': float(np.max(similarity_scores)),
        'median_similarity': float(np.median(similarity_scores)),
        'avg_recommendations_per_item': float(np.mean(recommendations_per_item)) if recommendations_per_item else 0.0
    }


def main():
    """Hàm chính để evaluate content-based recommendations."""
    print("=" * 80)
    print("EVALUATE CONTENT-BASED RECOMMENDATIONS")
    print("=" * 80)
    
    # Đường dẫn
    test_path = BASE_DIR / "data" / "processed" / "interactions_5core_test.parquet"
    metadata_path = BASE_DIR / "data" / "processed" / "metadata_clean.parquet"
    
    if not test_path.exists():
        raise FileNotFoundError(f"Không tìm thấy test data: {test_path}")
    
    # Load data
    test_df = load_test_data(test_path)
    item_category_map = load_metadata(metadata_path) if metadata_path.exists() else {}
    
    # Initialize content service
    print("\n[3] Đang khởi tạo ContentBasedRecallService...")
    content_service = ContentBasedRecallService()
    print("[OK] Content service initialized")
    
    # Evaluate với các K khác nhau
    all_metrics = {}
    
    for k in [5, 10, 20]:
        print(f"\n{'='*80}")
        print(f"EVALUATING WITH K={k}")
        print(f"{'='*80}")
        
        metrics = evaluate_content_based_recall(
            content_service=content_service,
            test_df=test_df,
            item_category_map=item_category_map,
            k=k,
            threshold=4.0
        )
        
        all_metrics[f'k={k}'] = metrics
    
    # Evaluate similarity quality
    print(f"\n{'='*80}")
    print("SIMILARITY QUALITY")
    print(f"{'='*80}")
    similarity_metrics = evaluate_similarity_quality(
        content_service=content_service,
        test_df=test_df,
        k=10
    )
    all_metrics['similarity_quality'] = similarity_metrics
    
    # In kết quả tổng hợp
    print("\n" + "=" * 80)
    print("KẾT QUẢ TỔNG HỢP")
    print("=" * 80)
    
    for k, metrics in all_metrics.items():
        if k.startswith('k='):
            print(f"\n[{k}]")
            print(f"  Precision@{k.split('=')[1]}: {metrics['precision']:.4f} ({metrics['precision']*100:.2f}%)")
            print(f"  Recall@{k.split('=')[1]}: {metrics['recall']:.4f} ({metrics['recall']*100:.2f}%)")
            print(f"  Hit Rate: {metrics['hit_rate']:.4f} ({metrics['hit_rate']*100:.2f}%)")
            print(f"  Coverage: {metrics['coverage']:.4f} ({metrics['coverage']*100:.2f}%)")
            print(f"  Category Diversity: {metrics['category_diversity']:.4f} ({metrics['category_diversity']*100:.2f}%)")
    
    print(f"\n[Similarity Quality]")
    print(f"  Avg Similarity: {similarity_metrics['avg_similarity']:.4f}")
    print(f"  Min Similarity: {similarity_metrics['min_similarity']:.4f}")
    print(f"  Max Similarity: {similarity_metrics['max_similarity']:.4f}")
    print(f"  Median Similarity: {similarity_metrics['median_similarity']:.4f}")
    
    # Lưu metrics
    output_dir = BASE_DIR / "backend" / "artifacts" / "metrics"
    output_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = output_dir / "content_based_metrics.json"
    
    with open(metrics_path, 'w', encoding='utf-8') as f:
        json.dump(all_metrics, f, indent=2)
    print(f"\n[OK] Đã lưu metrics vào: {metrics_path}")
    
    return all_metrics


if __name__ == "__main__":
    main()
