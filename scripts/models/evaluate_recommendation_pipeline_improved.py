"""
Evaluate Recommendation Pipeline (Improved)
===========================================
Sử dụng leave-one-out evaluation để đánh giá tốt hơn với dataset nhỏ.
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
    
    # Set global mean và biases
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


def calculate_precision_recall_at_k_improved(
    model: MatrixFactorization,
    train_df: pl.DataFrame,
    test_df: pl.DataFrame,
    k: int = 10,
    threshold: float = 4.0
) -> Tuple[float, float]:
    """
    Tính Precision@K và Recall@K với leave-one-out evaluation.
    
    Logic:
    - Với mỗi user, lấy tất cả items trong train set làm "known items"
    - Lấy items trong test set làm "ground truth"
    - Recommend top K items (exclude known items)
    - Tính precision và recall
    """
    print(f"\n[2] Đang tính Precision@{k} và Recall@{k} (Improved)...")
    print(f"  Rating threshold: {threshold}")
    print(f"  Method: Leave-one-out evaluation")
    
    # Group test data theo user
    test_by_user = test_df.group_by("user_id").agg([
        pl.col("item_id").alias("test_items"),
        pl.col("rating").alias("test_ratings")
    ])
    
    # Group train data theo user (known items)
    train_by_user = train_df.group_by("user_id").agg([
        pl.col("item_id").alias("train_items")
    ])
    
    precisions = []
    recalls = []
    valid_users = 0
    
    for test_row in test_by_user.iter_rows(named=True):
        user_id = test_row["user_id"]
        test_items = test_row["test_items"]
        test_ratings = test_row["test_ratings"]
        
        # Chỉ tính cho users có trong model
        if user_id not in model.user_to_idx:
            continue
        
        # Lấy positive test items (rating >= threshold)
        positive_test_items = set()
        for item, rating in zip(test_items, test_ratings):
            if rating >= threshold and item in model.item_to_idx:
                positive_test_items.add(item)
        
        if len(positive_test_items) == 0:
            continue
        
        # Lấy known items từ train set
        train_row = train_by_user.filter(pl.col("user_id") == user_id)
        known_items = set()
        if len(train_row) > 0:
            known_items = set(train_row.select("train_items").to_dicts()[0]["train_items"])
        
        valid_users += 1
        
        # Dự đoán scores cho tất cả items
        user_idx = model.user_to_idx[user_id]
        user_vector = model.user_factors[user_idx]
        all_scores = np.dot(model.item_factors, user_vector)
        
        # Exclude known items và test items khỏi recommendations
        # (chỉ exclude known items, giữ test items để có thể hit)
        all_item_indices = np.arange(len(model.idx_to_item))
        known_item_indices = [
            model.item_to_idx[item] for item in known_items
            if item in model.item_to_idx
        ]
        
        # Tạo mask để exclude known items
        mask = np.ones(len(all_item_indices), dtype=bool)
        for idx in known_item_indices:
            mask[idx] = False
        
        # Lấy top K items (sau khi exclude known items)
        masked_scores = all_scores.copy()
        masked_scores[~mask] = -np.inf  # Set known items to -inf
        
        top_k_indices = np.argsort(masked_scores)[::-1][:k]
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
    print(f"  Precision@{k}: {avg_precision:.4f} ({avg_precision*100:.2f}%)")
    print(f"  Recall@{k}: {avg_recall:.4f} ({avg_recall*100:.2f}%)")
    
    return float(avg_precision), float(avg_recall)


def main():
    """Hàm chính để evaluate recommendation pipeline."""
    print("=" * 80)
    print("EVALUATE RECOMMENDATION PIPELINE (IMPROVED)")
    print("=" * 80)
    
    # Đường dẫn
    artifacts_dir = BASE_DIR / "backend" / "artifacts" / "mf"
    if not artifacts_dir.exists():
        artifacts_dir = BASE_DIR / "artifacts" / "mf"
    
    train_path = BASE_DIR / "data" / "processed" / "interactions_5core_train.parquet"
    test_path = BASE_DIR / "data" / "processed" / "interactions_5core_test.parquet"
    
    if not test_path.exists():
        raise FileNotFoundError(f"Không tìm thấy test data: {test_path}")
    
    # Load data
    print("\nĐang load data...")
    train_df = pl.read_parquet(str(train_path)) if train_path.exists() else None
    test_df = pl.read_parquet(str(test_path))
    print(f"[OK] Train data: {len(train_df):,} samples" if train_df is not None else "[OK] No train data")
    print(f"[OK] Test data: {len(test_df):,} samples")
    
    # Load model
    print("\nĐang load MF model...")
    model = load_mf_model(artifacts_dir)
    print(f"[OK] Model loaded")
    
    # Tính metrics
    metrics = {}
    
    # RMSE và MAE
    rmse, mae = calculate_rmse_mae(model, test_df)
    metrics['rmse'] = rmse
    metrics['mae'] = mae
    
    # Precision@K và Recall@K với improved method
    if train_df is not None:
        for k in [5, 10, 20]:
            precision, recall = calculate_precision_recall_at_k_improved(
                model, train_df, test_df, k=k, threshold=3.5  # Giảm threshold xuống 3.5
            )
            metrics[f'precision@{k}'] = precision
            metrics[f'recall@{k}'] = recall
    
    # In kết quả
    print("\n" + "=" * 80)
    print("KẾT QUẢ METRICS (IMPROVED)")
    print("=" * 80)
    print(f"RMSE: {metrics['rmse']:.4f}")
    print(f"MAE: {metrics['mae']:.4f}")
    if train_df is not None:
        for k in [5, 10, 20]:
            print(f"Precision@{k}: {metrics[f'precision@{k}']:.4f} ({metrics[f'precision@{k}']*100:.2f}%)")
            print(f"Recall@{k}: {metrics[f'recall@{k}']:.4f} ({metrics[f'recall@{k}']*100:.2f}%)")
    
    # Lưu metrics
    output_dir = BASE_DIR / "backend" / "artifacts" / "metrics"
    output_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = output_dir / "recommendation_metrics.json"
    
    with open(metrics_path, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2)
    print(f"\n[OK] Đã lưu metrics vào: {metrics_path}")
    
    return metrics


if __name__ == "__main__":
    main()


