"""
Build Item Popularity Signal
============================

Tính toán popularity statistics cho items từ interactions data.
Tạo raw popularity signal để dùng cho ranking model.

Usage:
    python -m app.data_preprocessing.build_item_popularity
"""

import sys
from pathlib import Path
import polars as pl
import numpy as np
import io

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm root directory vào path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))


def load_and_merge_data(train_path: str, test_path: str):
    """
    Đọc và gộp dữ liệu train + test thành một tập duy nhất.
    
    Args:
        train_path: Đường dẫn đến file train parquet
        test_path: Đường dẫn đến file test parquet
        
    Returns:
        Polars DataFrame với toàn bộ dữ liệu (chỉ item_id và rating)
    """
    print("\n" + "=" * 80)
    print("BƯỚC 1: ĐỌC VÀ GỘP DỮ LIỆU")
    print("=" * 80)
    
    # Kiểm tra files tồn tại
    train_file = Path(train_path)
    test_file = Path(test_path)
    
    if not train_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file train: {train_path}")
    if not test_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file test: {test_path}")
    
    # Đọc dữ liệu
    print(f"Đang đọc train data từ: {train_path}")
    train_df = pl.read_parquet(train_path)
    print(f"[OK] Train data: {len(train_df):,} samples")
    
    print(f"Đang đọc test data từ: {test_path}")
    test_df = pl.read_parquet(test_path)
    print(f"[OK] Test data: {len(test_df):,} samples")
    
    # Kiểm tra schema
    required_cols = ['item_id', 'rating']
    for col in required_cols:
        if col not in train_df.columns:
            raise ValueError(f"Thiếu cột '{col}' trong train data")
        if col not in test_df.columns:
            raise ValueError(f"Thiếu cột '{col}' trong test data")
    
    # Lấy các cột cần thiết: item_id và rating
    train_df = train_df.select(['item_id', 'rating'])
    test_df = test_df.select(['item_id', 'rating'])
    
    # Gộp train + test
    print(f"\nĐang gộp train + test...")
    full_df = pl.concat([train_df, test_df])
    print(f"[OK] Tổng số samples sau khi gộp: {len(full_df):,}")
    
    # Thống kê
    ratings = full_df['rating'].to_numpy()
    print(f"\nThống kê dữ liệu đã gộp:")
    print(f"  - Rating min: {ratings.min():.2f}")
    print(f"  - Rating max: {ratings.max():.2f}")
    print(f"  - Rating mean: {ratings.mean():.2f}")
    print(f"  - Unique items: {full_df['item_id'].n_unique():,}")
    
    return full_df


def compute_popularity_stats(df: pl.DataFrame):
    """
    Tính toán popularity statistics cho mỗi item.
    
    Args:
        df: Polars DataFrame với columns: item_id, rating
        
    Returns:
        Polars DataFrame với columns: item_id, interaction_count, mean_rating
    """
    print("\n" + "=" * 80)
    print("BƯỚC 2: TÍNH TOÁN POPULARITY STATISTICS")
    print("=" * 80)
    
    print(f"\nĐang tính toán statistics cho {df['item_id'].n_unique():,} items...")
    
    # Group by item_id và tính statistics
    popularity_df = df.group_by("item_id").agg([
        pl.len().alias("interaction_count"),
        pl.col("rating").mean().alias("mean_rating")
    ])
    
    # Sắp xếp theo interaction_count giảm dần để dễ xem
    popularity_df = popularity_df.sort("interaction_count", descending=True)
    
    print(f"[OK] Đã tính toán statistics cho {len(popularity_df):,} items")
    
    # Thống kê
    interaction_counts = popularity_df['interaction_count'].to_numpy()
    mean_ratings = popularity_df['mean_rating'].to_numpy()
    
    print(f"\nThống kê popularity:")
    print(f"  - interaction_count min: {interaction_counts.min()}")
    print(f"  - interaction_count max: {interaction_counts.max()}")
    print(f"  - interaction_count mean: {interaction_counts.mean():.2f}")
    print(f"  - interaction_count median: {np.median(interaction_counts):.2f}")
    print(f"  - mean_rating min: {mean_ratings.min():.2f}")
    print(f"  - mean_rating max: {mean_ratings.max():.2f}")
    print(f"  - mean_rating mean: {mean_ratings.mean():.2f}")
    
    return popularity_df


def display_sample_stats(popularity_df: pl.DataFrame, num_samples: int = 10):
    """
    Hiển thị một vài dòng sample popularity stats.
    
    Args:
        popularity_df: DataFrame với popularity statistics
        num_samples: Số lượng samples cần hiển thị
    """
    print("\n" + "=" * 80)
    print("SAMPLE POPULARITY STATISTICS")
    print("=" * 80)
    
    num_samples = min(num_samples, len(popularity_df))
    sample_df = popularity_df.head(num_samples)
    
    print(f"\nTop {num_samples} items theo interaction_count:")
    print(f"\n{'Item ID':<15} {'Interaction Count':<20} {'Mean Rating':<15}")
    print("-" * 50)
    
    for row in sample_df.iter_rows(named=True):
        item_id = row['item_id']
        interaction_count = row['interaction_count']
        mean_rating = row['mean_rating']
        print(f"{item_id:<15} {interaction_count:<20} {mean_rating:<15.4f}")
    
    # Hiển thị thêm một vài items ở giữa và cuối
    if len(popularity_df) > num_samples * 2:
        print(f"\nMột vài items ở giữa (rank ~{len(popularity_df)//2}):")
        mid_df = popularity_df.slice(len(popularity_df)//2, num_samples)
        print(f"\n{'Item ID':<15} {'Interaction Count':<20} {'Mean Rating':<15}")
        print("-" * 50)
        for row in mid_df.iter_rows(named=True):
            item_id = row['item_id']
            interaction_count = row['interaction_count']
            mean_rating = row['mean_rating']
            print(f"{item_id:<15} {interaction_count:<20} {mean_rating:<15.4f}")


def save_popularity_stats(popularity_df: pl.DataFrame, output_path: Path):
    """
    Lưu popularity statistics ra file parquet.
    
    Args:
        popularity_df: DataFrame với popularity statistics
        output_path: Đường dẫn file output
    """
    print("\n" + "=" * 80)
    print("BƯỚC 3: LƯU KẾT QUẢ")
    print("=" * 80)
    
    # Tạo thư mục nếu chưa có
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\nĐang lưu popularity statistics: {output_path}")
    popularity_df.write_parquet(str(output_path))
    
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"[OK] Đã lưu {len(popularity_df):,} items vào {output_path}")
    print(f"  File size: {file_size_mb:.2f} MB")
    
    # In schema
    print(f"\nSchema của file output:")
    for col, dtype in zip(popularity_df.columns, popularity_df.dtypes):
        print(f"  {col}: {dtype}")


def main():
    """
    Hàm chính để chạy toàn bộ pipeline.
    """
    print("\n" + "=" * 80)
    print("PIPELINE TẠO ITEM POPULARITY SIGNAL")
    print("=" * 80)
    
    # Đường dẫn dữ liệu
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    train_path = BASE_DIR / "data" / "processed" / "interactions_5core_train.parquet"
    test_path = BASE_DIR / "data" / "processed" / "interactions_5core_test.parquet"
    
    # Đường dẫn output - lưu vào artifacts
    output_path = BASE_DIR / "artifacts" / "popularity" / "item_popularity.parquet"
    
    try:
        # Bước 1: Đọc và gộp dữ liệu
        full_df = load_and_merge_data(str(train_path), str(test_path))
        
        # Bước 2: Tính toán popularity statistics
        popularity_df = compute_popularity_stats(full_df)
        
        # Hiển thị sample stats
        display_sample_stats(popularity_df, num_samples=10)
        
        # Bước 3: Lưu kết quả
        save_popularity_stats(popularity_df, output_path)
        
        # Tóm tắt
        print("\n" + "=" * 80)
        print("TÓM TẮT")
        print("=" * 80)
        print(f"Tổng số items: {len(popularity_df):,}")
        print(f"Output file: {output_path}")
        print(f"\nSchema output:")
        print(f"  - item_id: string")
        print(f"  - interaction_count: int64 (số lần item xuất hiện)")
        print(f"  - mean_rating: float64 (rating trung bình)")
        print(f"\n[OK] Popularity signal đã sẵn sàng để dùng cho ranking model!")
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("[ERROR] PIPELINE THẤT BẠI")
        print("=" * 80)
        print(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

