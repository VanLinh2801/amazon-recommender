"""
Test Matrix Factorization Model Metrics
========================================

Tính toán các metrics: RMSE, MAE, Precision@K, Recall@K cho model MF.

Usage:
    python -m scripts.models.test_mf_metrics
"""

import sys
from pathlib import Path
import polars as pl
import numpy as np
import json
import io

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm root directory vào path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from scripts.models.matrix_factorization import MatrixFactorization


def load_mf_model(artifacts_dir: Path) -> MatrixFactorization:
    """
    Load MF model từ artifacts.
    
    Args:
        artifacts_dir: Thư mục chứa artifacts
        
    Returns:
        MatrixFactorization model đã được load
    """
    print("\n" + "=" * 80)
    print("LOADING MF MODEL FROM ARTIFACTS")
    print("=" * 80)
    
    mf_dir = artifacts_dir / "mf"
    
    # Load user_factors
    user_factors_path = mf_dir / "user_factors.npy"
    if not user_factors_path.exists():
        raise FileNotFoundError(f"Không tìm thấy: {user_factors_path}")
    user_factors = np.load(str(user_factors_path))
    print(f"[OK] Loaded user_factors: {user_factors.shape}")
    
    # Load item_factors
    item_factors_path = mf_dir / "item_factors.npy"
    if not item_factors_path.exists():
        raise FileNotFoundError(f"Không tìm thấy: {item_factors_path}")
    item_factors = np.load(str(item_factors_path))
    print(f"[OK] Loaded item_factors: {item_factors.shape}")
    
    # Load user2idx
    user2idx_path = mf_dir / "user2idx.json"
    if not user2idx_path.exists():
        raise FileNotFoundError(f"Không tìm thấy: {user2idx_path}")
    with open(user2idx_path, 'r', encoding='utf-8') as f:
        user2idx = json.load(f)
    print(f"[OK] Loaded user2idx: {len(user2idx)} users")
    
    # Load idx2item
    idx2item_path = mf_dir / "idx2item.json"
    if not idx2item_path.exists():
        raise FileNotFoundError(f"Không tìm thấy: {idx2item_path}")
    with open(idx2item_path, 'r', encoding='utf-8') as f:
        idx2item_raw = json.load(f)
    idx2item = {int(k): v for k, v in idx2item_raw.items()}
    print(f"[OK] Loaded idx2item: {len(idx2item)} items")
    
    # Tạo item2idx từ idx2item
    item2idx = {v: k for k, v in idx2item.items()}
    
    # Tạo model và load parameters
    model = MatrixFactorization(n_factors=user_factors.shape[1])
    model.user_factors = user_factors
    model.item_factors = item_factors
    model.user_to_idx = user2idx
    model.item_to_idx = item2idx
    model.idx_to_user = {v: k for k, v in user2idx.items()}
    model.idx_to_item = idx2item
    model.n_users = len(user2idx)
    model.n_items = len(item2idx)
    
    # Tính global mean từ training data (cần load train data)
    train_path = BASE_DIR / "data" / "processed" / "interactions_5core_train.parquet"
    if train_path.exists():
        train_df = pl.read_parquet(str(train_path))
        model.global_mean = float(train_df['rating'].mean())
        print(f"[OK] Global mean: {model.global_mean:.3f}")
    else:
        model.global_mean = 3.0  # Default
        print(f"[WARNING] Không tìm thấy train data, dùng global_mean mặc định: {model.global_mean}")
    
    print("[OK] Model loaded successfully!")
    return model


def calculate_rmse(model: MatrixFactorization, test_df: pl.DataFrame) -> float:
    """Tính RMSE trên test set."""
    print("\nĐang tính RMSE...")
    user_ids = test_df['user_id'].to_numpy()
    item_ids = test_df['item_id'].to_numpy()
    ratings = test_df['rating'].to_numpy().astype(np.float32)
    
    # Chỉ tính cho các user/item có trong model
    valid_mask = np.array([
        uid in model.user_to_idx and iid in model.item_to_idx
        for uid, iid in zip(user_ids, item_ids)
    ])
    
    if valid_mask.sum() == 0:
        return float('inf')
    
    valid_user_ids = user_ids[valid_mask]
    valid_item_ids = item_ids[valid_mask]
    valid_ratings = ratings[valid_mask]
    
    predictions = model.predict(valid_user_ids, valid_item_ids)
    mse = np.mean((valid_ratings - predictions) ** 2)
    rmse = np.sqrt(mse)
    
    print(f"  Valid samples: {valid_mask.sum():,} / {len(test_df):,}")
    print(f"  RMSE: {rmse:.4f}")
    return float(rmse)


def calculate_mae(model: MatrixFactorization, test_df: pl.DataFrame) -> float:
    """Tính MAE trên test set."""
    print("\nĐang tính MAE...")
    user_ids = test_df['user_id'].to_numpy()
    item_ids = test_df['item_id'].to_numpy()
    ratings = test_df['rating'].to_numpy().astype(np.float32)
    
    # Chỉ tính cho các user/item có trong model
    valid_mask = np.array([
        uid in model.user_to_idx and iid in model.item_to_idx
        for uid, iid in zip(user_ids, item_ids)
    ])
    
    if valid_mask.sum() == 0:
        return float('inf')
    
    valid_user_ids = user_ids[valid_mask]
    valid_item_ids = item_ids[valid_mask]
    valid_ratings = ratings[valid_mask]
    
    predictions = model.predict(valid_user_ids, valid_item_ids)
    mae = np.mean(np.abs(valid_ratings - predictions))
    
    print(f"  Valid samples: {valid_mask.sum():,} / {len(test_df):,}")
    print(f"  MAE: {mae:.4f}")
    return float(mae)


def calculate_precision_recall_at_k(
    model: MatrixFactorization,
    test_df: pl.DataFrame,
    k: int = 10,
    threshold: float = 4.0
) -> tuple[float, float]:
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
    print(f"\nĐang tính Precision@{k} và Recall@{k}...")
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
        test_items = set(row["test_items"])
        test_ratings = row["test_ratings"]
        
        # Chỉ tính cho users có trong model
        if user_id not in model.user_to_idx:
            continue
        
        # Lấy test items với rating >= threshold
        positive_test_items = {
            item for item, rating in zip(test_items, test_ratings)
            if rating >= threshold and item in model.item_to_idx
        }
        
        if len(positive_test_items) == 0:
            continue  # Bỏ qua users không có positive items
        
        valid_users += 1
        
        # Dự đoán scores cho tất cả items
        user_idx = model.user_to_idx[user_id]
        user_vector = model.user_factors[user_idx]
        all_scores = np.dot(model.item_factors, user_vector)
        
        # Lấy top K items
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
    """Hàm chính để tính metrics."""
    print("\n" + "=" * 80)
    print("TESTING MF MODEL METRICS")
    print("=" * 80)
    
    # Đường dẫn
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
    model = load_mf_model(artifacts_dir)
    
    # Tính metrics
    metrics = {}
    
    # RMSE
    metrics['rmse'] = calculate_rmse(model, test_df)
    
    # MAE
    metrics['mae'] = calculate_mae(model, test_df)
    
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
    metrics_path = artifacts_dir / "metrics.json"
    with open(metrics_path, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2)
    print(f"\n[OK] Đã lưu metrics vào: {metrics_path}")
    
    return metrics


if __name__ == "__main__":
    main()


