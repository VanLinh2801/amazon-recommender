"""
Evaluate Recommendation Pipeline
=================================
Đánh giá toàn bộ recommendation pipeline với các metrics:
- RMSE, MAE (cho rating prediction)
- Precision@K, Recall@K (cho recommendation quality)

Chạy: python scripts/models/evaluate_recommendation_pipeline.py
"""

import sys
import io
from pathlib import Path
import json
import numpy as np
import polars as pl
from typing import Dict, List, Tuple

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm root vào path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(BASE_DIR / "backend"))

from scripts.models.matrix_factorization import MatrixFactorization


def load_mf_model(artifacts_dir: Path) -> MatrixFactorization:
    """Load MF model từ artifacts."""
    import json
    
    user_factors_path = artifacts_dir / "user_factors.npy"
    item_factors_path = artifacts_dir / "item_factors.npy"
    user2idx_path = artifacts_dir / "user2idx.json"
    idx2item_path = artifacts_dir / "idx2item.json"
    
    if not all(p.exists() for p in [user_factors_path, item_factors_path, user2idx_path, idx2item_path]):
        raise FileNotFoundError(f"Không tìm thấy MF artifacts trong {artifacts_dir}")
    
    user_factors = np.load(user_factors_path)
    item_factors = np.load(item_factors_path)
    
    with open(user2idx_path, 'r', encoding='utf-8') as f:
        user2idx = json.load(f)
    
    with open(idx2item_path, 'r', encoding='utf-8') as f:
        idx2item = json.load(f)
    
    # Tạo reverse mapping
    item2idx = {item: idx for idx, item in idx2item.items()}
    
    model = MatrixFactorization(n_factors=user_factors.shape[1])
    model.user_factors = user_factors
    model.item_factors = item_factors
    model.user_to_idx = {k: int(v) for k, v in user2idx.items()}
    model.item_to_idx = {k: int(v) for k, v in item2idx.items()}
    model.idx_to_user = {int(v): k for k, v in user2idx.items()}
    model.idx_to_item = {int(k): v for k, v in idx2item.items()}
    
    # Set global mean và biases (nếu không có thì dùng default)
    train_path = BASE_DIR / "data" / "processed" / "interactions_5core_train.parquet"
    if train_path.exists():
        train_df = pl.read_parquet(str(train_path))
        model.global_mean = float(train_df['rating'].mean())
    else:
        model.global_mean = 3.0
    
    # Initialize biases nếu chưa có
    if not hasattr(model, 'user_bias') or model.user_bias is None:
        model.user_bias = np.zeros(len(model.user_to_idx))
    if not hasattr(model, 'item_bias') or model.item_bias is None:
        model.item_bias = np.zeros(len(model.item_to_idx))
    
    return model


def calculate_rmse_mae(model: MatrixFactorization, test_df: pl.DataFrame) -> Tuple[float, float]:
    """Tính RMSE và MAE trên test set."""
    print("\n[1] Đang tính RMSE và MAE...")
    
    user_ids = test_df['user_id'].to_numpy()
    item_ids = test_df['item_id'].to_numpy()
    ratings = test_df['rating'].to_numpy().astype(np.float32)
    
    # Chỉ tính cho các user/item có trong model
    valid_mask = np.array([
        uid in model.user_to_idx and iid in model.item_to_idx
        for uid, iid in zip(user_ids, item_ids)
    ])
    
    if valid_mask.sum() == 0:
        return float('inf'), float('inf')
    
    valid_user_ids = user_ids[valid_mask]
    valid_item_ids = item_ids[valid_mask]
    valid_ratings = ratings[valid_mask]
    
    # Predict
    predictions = model.predict(valid_user_ids, valid_item_ids)
    
    # RMSE
    mse = np.mean((valid_ratings - predictions) ** 2)
    rmse = np.sqrt(mse)
    
    # MAE
    mae = np.mean(np.abs(valid_ratings - predictions))
    
    print(f"  Valid samples: {valid_mask.sum():,} / {len(test_df):,}")
    print(f"  RMSE: {rmse:.4f}")
    print(f"  MAE: {mae:.4f}")
    
    return float(rmse), float(mae)


def calculate_precision_recall_at_k(
    model: MatrixFactorization,
    test_df: pl.DataFrame,
    k: int = 10,
    threshold: float = 4.0
) -> Tuple[float, float]:
    """
    Tính Precision@K và Recall@K.
    
    Args:
        model: MF model
        test_df: Test DataFrame
        k: Top K items để recommend
        threshold: Rating threshold để coi là positive (>= threshold)
        
    Returns:
        Tuple (precision@k, recall@k)
    """
    print(f"\n[2] Đang tính Precision@{k} và Recall@{k}...")
    print(f"  Rating threshold: {threshold}")
    
    # Group test data theo user
    test_by_user = test_df.group_by("user_id").agg([
        pl.col("item_id").alias("test_items"),
        pl.col("rating").alias("test_ratings")
    ])
    
    precisions = []
    recalls = []
    valid_users = 0
    
    for row in test_by_user.iter_rows(named=True):
        user_id = row["user_id"]
        test_items = row["test_items"]
        test_ratings = row["test_ratings"]
        
        # Chỉ tính cho users có trong model
        if user_id not in model.user_to_idx:
            continue
        
        # Lấy test items với rating >= threshold
        positive_test_items = set()
        for item, rating in zip(test_items, test_ratings):
            if rating >= threshold and item in model.item_to_idx:
                positive_test_items.add(item)
        
        if len(positive_test_items) == 0:
            continue  # Bỏ qua users không có positive items
        
        valid_users += 1
        
        # Dự đoán scores cho tất cả items
        user_idx = model.user_to_idx[user_id]
        user_vector = model.user_factors[user_idx]
        all_scores = np.dot(model.item_factors, user_vector)
        
        # Lấy top K items (loại bỏ items đã có trong test set để tránh data leakage)
        # Tạo mask để loại bỏ test items
        item_indices = np.arange(len(model.idx_to_item))
        test_item_indices = [
            model.item_to_idx[item] for item in positive_test_items
            if item in model.item_to_idx
        ]
        
        # Sort scores và lấy top K (có thể bao gồm test items, đó là OK cho evaluation)
        top_k_indices = np.argsort(all_scores)[::-1][:k]
        top_k_items = {model.idx_to_item[idx] for idx in top_k_indices}
        
        # Tính precision và recall
        relevant_recommended = len(top_k_items & positive_test_items)
        precision = relevant_recommended / k if k > 0 else 0.0
        recall = relevant_recommended / len(positive_test_items) if len(positive_test_items) > 0 else 0.0
        
        precisions.append(precision)
        recalls.append(recall)
    
    avg_precision = np.mean(precisions) if precisions else 0.0
    avg_recall = np.mean(recalls) if recalls else 0.0
    
    print(f"  Valid users: {valid_users:,}")
    print(f"  Precision@{k}: {avg_precision:.4f}")
    print(f"  Recall@{k}: {avg_recall:.4f}")
    
    return float(avg_precision), float(avg_recall)


def main():
    """Hàm chính để evaluate recommendation pipeline."""
    print("=" * 80)
    print("EVALUATE RECOMMENDATION PIPELINE")
    print("=" * 80)
    
    # Đường dẫn
    artifacts_dir = BASE_DIR / "backend" / "artifacts" / "mf"
    if not artifacts_dir.exists():
        artifacts_dir = BASE_DIR / "artifacts" / "mf"
    
    test_path = BASE_DIR / "data" / "processed" / "interactions_5core_test.parquet"
    
    if not test_path.exists():
        raise FileNotFoundError(f"Không tìm thấy test data: {test_path}")
    
    # Load test data
    print("\nĐang load test data...")
    test_df = pl.read_parquet(str(test_path))
    print(f"[OK] Test data: {len(test_df):,} samples")
    print(f"  Users: {test_df['user_id'].n_unique():,}")
    print(f"  Items: {test_df['item_id'].n_unique():,}")
    
    # Load model
    print("\nĐang load MF model...")
    model = load_mf_model(artifacts_dir)
    print(f"[OK] Model loaded")
    print(f"  Users: {len(model.user_to_idx):,}")
    print(f"  Items: {len(model.item_to_idx):,}")
    
    # Tính metrics
    metrics = {}
    
    # RMSE và MAE
    rmse, mae = calculate_rmse_mae(model, test_df)
    metrics['rmse'] = rmse
    metrics['mae'] = mae
    
    # Precision@K và Recall@K cho các K khác nhau
    for k in [5, 10, 20]:
        precision, recall = calculate_precision_recall_at_k(model, test_df, k=k)
        metrics[f'precision@{k}'] = precision
        metrics[f'recall@{k}'] = recall
    
    # In kết quả
    print("\n" + "=" * 80)
    print("KẾT QUẢ METRICS")
    print("=" * 80)
    print(f"RMSE: {metrics['rmse']:.4f}")
    print(f"MAE: {metrics['mae']:.4f}")
    print(f"Precision@5: {metrics['precision@5']:.4f}")
    print(f"Recall@5: {metrics['recall@5']:.4f}")
    print(f"Precision@10: {metrics['precision@10']:.4f}")
    print(f"Recall@10: {metrics['recall@10']:.4f}")
    print(f"Precision@20: {metrics['precision@20']:.4f}")
    print(f"Recall@20: {metrics['recall@20']:.4f}")
    
    # Lưu metrics vào file JSON
    output_dir = BASE_DIR / "backend" / "artifacts" / "metrics"
    output_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = output_dir / "recommendation_metrics.json"
    
    with open(metrics_path, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2)
    print(f"\n[OK] Đã lưu metrics vào: {metrics_path}")
    
    return metrics


if __name__ == "__main__":
    main()

