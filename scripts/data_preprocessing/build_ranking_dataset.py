"""
Build Ranking Dataset
=====================
Tạo supervised dataset để huấn luyện ranking model từ interaction data,
MF artifacts, và popularity signals.

Input:
- data/processed/interactions_5core_train.parquet
- data/processed/interactions_5core_test.parquet
- artifacts/mf/user_factors.npy
- artifacts/mf/item_factors.npy
- artifacts/mf/user2idx.json
- artifacts/mf/idx2item.json
- artifacts/popularity/item_popularity_normalized.parquet

Output:
- artifacts/ranking/ranking_dataset.parquet

Usage:
    python -m app.data_preprocessing.build_ranking_dataset
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


def load_interaction_data(train_path: Path, test_path: Path) -> pl.DataFrame:
    """
    Đọc và gộp interaction data từ train + test.
    
    Args:
        train_path: Đường dẫn đến file train parquet
        test_path: Đường dẫn đến file test parquet
        
    Returns:
        Polars DataFrame với toàn bộ interactions
    """
    print("\n" + "=" * 80)
    print("BƯỚC 1: ĐỌC VÀ GỘP INTERACTION DATA")
    print("=" * 80)
    
    # Kiểm tra files tồn tại
    if not train_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file train: {train_path}")
    if not test_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file test: {test_path}")
    
    # Đọc train data
    print(f"\nĐang đọc file train: {train_path}")
    df_train = pl.read_parquet(str(train_path))
    print(f"[OK] Đã đọc {len(df_train):,} interactions từ train")
    print(f"  Columns: {df_train.columns}")
    
    # Đọc test data
    print(f"\nĐang đọc file test: {test_path}")
    df_test = pl.read_parquet(str(test_path))
    print(f"[OK] Đã đọc {len(df_test):,} interactions từ test")
    print(f"  Columns: {df_test.columns}")
    
    # Gộp train + test
    print(f"\nĐang gộp train + test...")
    df_all = pl.concat([df_train, df_test])
    print(f"[OK] Tổng số interactions: {len(df_all):,}")
    
    # Kiểm tra schema
    required_cols = ['user_id', 'item_id', 'rating']
    missing_cols = [col for col in required_cols if col not in df_all.columns]
    if missing_cols:
        raise ValueError(f"Thiếu các cột: {missing_cols}")
    
    # Thống kê
    print(f"\nThống kê interactions:")
    print(f"  Số users unique: {df_all['user_id'].n_unique():,}")
    print(f"  Số items unique: {df_all['item_id'].n_unique():,}")
    print(f"  Rating trung bình: {df_all['rating'].mean():.2f}")
    print(f"  Rating min: {df_all['rating'].min()}")
    print(f"  Rating max: {df_all['rating'].max()}")
    
    return df_all


def load_mf_artifacts(artifacts_dir: Path) -> tuple:
    """
    Load MF artifacts: user_factors, item_factors, user2idx, idx2item.
    
    Args:
        artifacts_dir: Thư mục chứa MF artifacts
        
    Returns:
        Tuple (user_factors, item_factors, user2idx, idx2item)
    """
    print("\n" + "=" * 80)
    print("BƯỚC 2: LOAD MF ARTIFACTS")
    print("=" * 80)
    
    # Load user_factors
    user_factors_path = artifacts_dir / "user_factors.npy"
    if not user_factors_path.exists():
        raise FileNotFoundError(f"Không tìm thấy: {user_factors_path}")
    
    print(f"\nĐang load user_factors.npy...")
    user_factors = np.load(str(user_factors_path))
    print(f"[OK] user_factors shape: {user_factors.shape}")
    
    # Load item_factors
    item_factors_path = artifacts_dir / "item_factors.npy"
    if not item_factors_path.exists():
        raise FileNotFoundError(f"Không tìm thấy: {item_factors_path}")
    
    print(f"\nĐang load item_factors.npy...")
    item_factors = np.load(str(item_factors_path))
    print(f"[OK] item_factors shape: {item_factors.shape}")
    
    # Load user2idx
    user2idx_path = artifacts_dir / "user2idx.json"
    if not user2idx_path.exists():
        raise FileNotFoundError(f"Không tìm thấy: {user2idx_path}")
    
    print(f"\nĐang load user2idx.json...")
    with open(user2idx_path, 'r', encoding='utf-8') as f:
        user2idx = json.load(f)
    print(f"[OK] Số users trong user2idx: {len(user2idx):,}")
    
    # Load idx2item
    idx2item_path = artifacts_dir / "idx2item.json"
    if not idx2item_path.exists():
        raise FileNotFoundError(f"Không tìm thấy: {idx2item_path}")
    
    print(f"\nĐang load idx2item.json...")
    with open(idx2item_path, 'r', encoding='utf-8') as f:
        idx2item_raw = json.load(f)
    # idx2item có keys là string, convert về int
    idx2item = {int(k): v for k, v in idx2item_raw.items()}
    print(f"[OK] Số items trong idx2item: {len(idx2item):,}")
    
    # Kiểm tra tính nhất quán
    print(f"\nKiểm tra tính nhất quán:")
    print(f"  user_factors.shape[0] = {user_factors.shape[0]}, len(user2idx) = {len(user2idx)}")
    print(f"  item_factors.shape[0] = {item_factors.shape[0]}, len(idx2item) = {len(idx2item)}")
    
    if user_factors.shape[0] != len(user2idx):
        raise ValueError(f"Số users không khớp: {user_factors.shape[0]} vs {len(user2idx)}")
    if item_factors.shape[0] != len(idx2item):
        raise ValueError(f"Số items không khớp: {item_factors.shape[0]} vs {len(idx2item)}")
    
    return user_factors, item_factors, user2idx, idx2item


def load_popularity_data(popularity_path: Path) -> pl.DataFrame:
    """
    Load popularity normalized data.
    
    Args:
        popularity_path: Đường dẫn đến file popularity parquet
        
    Returns:
        Polars DataFrame với popularity data
    """
    print("\n" + "=" * 80)
    print("BƯỚC 3: LOAD POPULARITY DATA")
    print("=" * 80)
    
    if not popularity_path.exists():
        raise FileNotFoundError(f"Không tìm thấy: {popularity_path}")
    
    print(f"\nĐang đọc file popularity: {popularity_path}")
    df_pop = pl.read_parquet(str(popularity_path))
    print(f"[OK] Đã đọc {len(df_pop):,} items")
    print(f"  Columns: {df_pop.columns}")
    
    # Kiểm tra schema
    required_cols = ['item_id', 'popularity_score', 'rating_score']
    missing_cols = [col for col in required_cols if col not in df_pop.columns]
    if missing_cols:
        raise ValueError(f"Thiếu các cột: {missing_cols}")
    
    # Thống kê
    print(f"\nThống kê popularity:")
    print(f"  popularity_score - min: {df_pop['popularity_score'].min():.4f}, max: {df_pop['popularity_score'].max():.4f}, mean: {df_pop['popularity_score'].mean():.4f}")
    print(f"  rating_score - min: {df_pop['rating_score'].min():.4f}, max: {df_pop['rating_score'].max():.4f}, mean: {df_pop['rating_score'].mean():.4f}")
    
    return df_pop


def compute_mf_scores(
    df: pl.DataFrame,
    user_factors: np.ndarray,
    item_factors: np.ndarray,
    user2idx: dict,
    idx2item: dict
) -> pl.DataFrame:
    """
    Tính mf_score = dot(user_vector, item_vector) cho mỗi (user_id, item_id).
    
    Args:
        df: DataFrame chứa user_id, item_id
        user_factors: User latent factors (num_users, latent_dim)
        item_factors: Item latent factors (num_items, latent_dim)
        user2idx: Mapping user_id -> index
        idx2item: Mapping index -> item_id
        
    Returns:
        DataFrame với thêm cột mf_score
    """
    print("\n" + "=" * 80)
    print("BƯỚC 4: TÍNH MF SCORES")
    print("=" * 80)
    
    # Tạo reverse mapping: item_id -> index
    item2idx = {v: k for k, v in idx2item.items()}
    
    print(f"\nĐang tính mf_score cho {len(df):,} interactions...")
    
    # Lấy danh sách user_id và item_id
    user_ids = df['user_id'].to_list()
    item_ids = df['item_id'].to_list()
    
    # Tính mf_score cho từng interaction
    mf_scores = []
    missing_user_count = 0
    missing_item_count = 0
    
    for user_id, item_id in zip(user_ids, item_ids):
        # Lấy user index
        if user_id not in user2idx:
            missing_user_count += 1
            mf_scores.append(0.0)  # Default score nếu không tìm thấy
            continue
        
        user_idx = user2idx[user_id]
        user_vector = user_factors[user_idx]
        
        # Lấy item index
        if item_id not in item2idx:
            missing_item_count += 1
            mf_scores.append(0.0)  # Default score nếu không tìm thấy
            continue
        
        item_idx = item2idx[item_id]
        item_vector = item_factors[item_idx]
        
        # Tính dot product
        mf_score = np.dot(user_vector, item_vector)
        mf_scores.append(float(mf_score))
    
    if missing_user_count > 0:
        print(f"[WARNING] {missing_user_count:,} interactions có user_id không tìm thấy trong user2idx")
    if missing_item_count > 0:
        print(f"[WARNING] {missing_item_count:,} interactions có item_id không tìm thấy trong item2idx")
    
    # Thêm cột mf_score vào DataFrame
    df_with_mf = df.with_columns([
        pl.Series("mf_score", mf_scores)
    ])
    
    print(f"[OK] Đã tính xong mf_score")
    print(f"\nThống kê mf_score:")
    print(f"  Min: {df_with_mf['mf_score'].min():.4f}")
    print(f"  Max: {df_with_mf['mf_score'].max():.4f}")
    print(f"  Mean: {df_with_mf['mf_score'].mean():.4f}")
    print(f"  Median: {df_with_mf['mf_score'].median():.4f}")
    
    return df_with_mf


def build_ranking_dataset(
    df_interactions: pl.DataFrame,
    df_popularity: pl.DataFrame
) -> pl.DataFrame:
    """
    Xây dựng ranking dataset với đầy đủ features và label.
    
    Args:
        df_interactions: DataFrame với user_id, item_id, rating, mf_score
        df_popularity: DataFrame với item_id, popularity_score, rating_score
        
    Returns:
        DataFrame với schema: user_id, item_id, mf_score, content_score, popularity_score, rating_score, label
    """
    print("\n" + "=" * 80)
    print("BƯỚC 5: XÂY DỰNG RANKING DATASET")
    print("=" * 80)
    
    # Join với popularity data
    print(f"\nĐang join với popularity data...")
    df_ranking = df_interactions.join(
        df_popularity,
        on='item_id',
        how='left'
    )
    print(f"[OK] Đã join, số dòng: {len(df_ranking):,}")
    
    # Kiểm tra số dòng có popularity data
    has_pop = df_ranking.filter(pl.col('popularity_score').is_not_null())
    print(f"  Interactions có popularity_score: {len(has_pop):,} ({len(has_pop)/len(df_ranking)*100:.1f}%)")
    
    # Fill null values cho popularity_score và rating_score (nếu không có trong popularity data)
    df_ranking = df_ranking.with_columns([
        pl.col('popularity_score').fill_null(0.0),
        pl.col('rating_score').fill_null(0.0)
    ])
    
    # Thêm content_score = 0 (placeholder)
    df_ranking = df_ranking.with_columns([
        pl.lit(0.0).alias('content_score')
    ])
    
    # Tạo label: 1 nếu rating >= 4, 0 nếu rating < 4
    df_ranking = df_ranking.with_columns([
        (pl.col('rating') >= 4).cast(pl.Int64).alias('label')
    ])
    
    # Chọn và sắp xếp các cột theo thứ tự yêu cầu
    df_ranking = df_ranking.select([
        'user_id',
        'item_id',
        'mf_score',
        'content_score',
        'popularity_score',
        'rating_score',
        'label'
    ])
    
    print(f"[OK] Đã xây dựng ranking dataset")
    print(f"  Số dòng: {len(df_ranking):,}")
    print(f"  Columns: {df_ranking.columns}")
    
    return df_ranking


def main():
    """
    Hàm chính để build ranking dataset.
    """
    print("=" * 80)
    print("BUILD RANKING DATASET")
    print("=" * 80)
    
    # Đường dẫn các file input
    project_root = BASE_DIR
    train_path = project_root / "data" / "processed" / "interactions_5core_train.parquet"
    test_path = project_root / "data" / "processed" / "interactions_5core_test.parquet"
    mf_artifacts_dir = project_root / "artifacts" / "mf"
    popularity_path = project_root / "artifacts" / "popularity" / "item_popularity_normalized.parquet"
    
    # Đường dẫn output
    output_dir = project_root / "artifacts" / "ranking"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "ranking_dataset.parquet"
    
    try:
        # Bước 1: Load và gộp interaction data
        df_interactions = load_interaction_data(train_path, test_path)
        
        # Bước 2: Load MF artifacts
        user_factors, item_factors, user2idx, idx2item = load_mf_artifacts(mf_artifacts_dir)
        
        # Bước 3: Load popularity data
        df_popularity = load_popularity_data(popularity_path)
        
        # Bước 4: Tính mf_score
        df_interactions = compute_mf_scores(
            df_interactions,
            user_factors,
            item_factors,
            user2idx,
            idx2item
        )
        
        # Bước 5: Xây dựng ranking dataset
        df_ranking = build_ranking_dataset(df_interactions, df_popularity)
        
        # Thống kê dataset
        print("\n" + "=" * 80)
        print("THỐNG KÊ RANKING DATASET")
        print("=" * 80)
        
        print(f"\nSố dòng dataset: {len(df_ranking):,}")
        
        # Thống kê label
        label_counts = df_ranking.group_by('label').agg(pl.len().alias('count')).sort('label')
        print(f"\nTỷ lệ label:")
        for row in label_counts.to_dicts():
            label = row['label']
            count = row['count']
            pct = count / len(df_ranking) * 100
            print(f"  Label {label}: {count:,} ({pct:.2f}%)")
        
        # Thống kê các features
        print(f"\nThống kê features:")
        for col in ['mf_score', 'content_score', 'popularity_score', 'rating_score']:
            stats = df_ranking[col]
            print(f"  {col}:")
            print(f"    Min: {stats.min():.4f}")
            print(f"    Max: {stats.max():.4f}")
            print(f"    Mean: {stats.mean():.4f}")
            print(f"    Median: {stats.median():.4f}")
        
        # In một vài dòng sample
        print("\n" + "=" * 80)
        print("MẪU DỮ LIỆU (10 dòng đầu tiên):")
        print("=" * 80)
        print(df_ranking.head(10))
        
        # Lưu dataset
        print("\n" + "=" * 80)
        print("LƯU DATASET")
        print("=" * 80)
        
        print(f"\nĐang lưu dataset: {output_path}")
        df_ranking.write_parquet(str(output_path))
        print(f"[OK] Đã lưu {len(df_ranking):,} dòng vào {output_path}")
        
        # Kiểm tra file đã lưu
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"  File size: {file_size_mb:.2f} MB")
        
        print("\n" + "=" * 80)
        print("[OK] HOÀN TẤT: Ranking dataset đã được tạo thành công!")
        print("=" * 80)
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("[ERROR] LỖI KHI BUILD RANKING DATASET")
        print("=" * 80)
        print(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()

