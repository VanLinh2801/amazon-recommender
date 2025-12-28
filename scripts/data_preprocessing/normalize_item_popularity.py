"""
Normalize Item Popularity Signal
=================================

Chuẩn hóa popularity signal để dùng làm feature cho ranking model.

Usage:
    python -m app.data_preprocessing.normalize_item_popularity
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


def load_popularity_data(input_path: Path):
    """
    Đọc dữ liệu popularity từ parquet file.
    
    Args:
        input_path: Đường dẫn đến file item_popularity.parquet
        
    Returns:
        Polars DataFrame với columns: item_id, interaction_count, mean_rating
    """
    print("\n" + "=" * 80)
    print("BƯỚC 1: ĐỌC DỮ LIỆU POPULARITY")
    print("=" * 80)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {input_path}")
    
    print(f"Đang đọc dữ liệu từ: {input_path}")
    df = pl.read_parquet(str(input_path))
    print(f"[OK] Đã đọc {len(df):,} items")
    
    # Kiểm tra schema
    required_cols = ['item_id', 'interaction_count', 'mean_rating']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Thiếu cột '{col}' trong dữ liệu")
    
    # Thống kê
    interaction_counts = df['interaction_count'].to_numpy()
    mean_ratings = df['mean_rating'].to_numpy()
    
    print(f"\nThống kê dữ liệu đầu vào:")
    print(f"  - interaction_count min: {interaction_counts.min()}")
    print(f"  - interaction_count max: {interaction_counts.max()}")
    print(f"  - interaction_count mean: {interaction_counts.mean():.2f}")
    print(f"  - mean_rating min: {mean_ratings.min():.2f}")
    print(f"  - mean_rating max: {mean_ratings.max():.2f}")
    print(f"  - mean_rating mean: {mean_ratings.mean():.2f}")
    
    return df


def normalize_popularity(df: pl.DataFrame):
    """
    Chuẩn hóa popularity signal.
    
    Args:
        df: DataFrame với columns: item_id, interaction_count, mean_rating
        
    Returns:
        DataFrame với columns: item_id, popularity_score, rating_score
    """
    print("\n" + "=" * 80)
    print("BƯỚC 2: CHUẨN HÓA POPULARITY SIGNAL")
    print("=" * 80)
    
    # 1. Chuẩn hóa interaction_count
    print(f"\n1. Chuẩn hóa interaction_count:")
    print(f"   - Áp dụng log transform: log(1 + interaction_count)")
    
    # Log transform
    log_interaction = pl.col("interaction_count").log1p()
    
    # Tính min và max của log_interaction để min-max scale
    log_values = np.log1p(df['interaction_count'].to_numpy())
    log_min = log_values.min()
    log_max = log_values.max()
    
    print(f"   - Log values range: [{log_min:.4f}, {log_max:.4f}]")
    print(f"   - Min-max scale về [0, 1]")
    
    # Min-max scale: (x - min) / (max - min)
    popularity_score = (log_interaction - log_min) / (log_max - log_min)
    
    # 2. Chuẩn hóa mean_rating
    print(f"\n2. Chuẩn hóa mean_rating:")
    print(f"   - Linear scale từ [1, 5] → [0, 1]")
    print(f"   - Formula: (mean_rating - 1) / 4")
    
    # Linear scale: (mean_rating - 1) / 4
    rating_score = (pl.col("mean_rating") - 1.0) / 4.0
    
    # Tạo DataFrame mới
    normalized_df = df.select([
        pl.col("item_id"),
        popularity_score.alias("popularity_score"),
        rating_score.alias("rating_score")
    ])
    
    print(f"\n[OK] Đã chuẩn hóa {len(normalized_df):,} items")
    
    return normalized_df


def display_statistics(normalized_df: pl.DataFrame):
    """
    Hiển thị thống kê của dữ liệu đã chuẩn hóa.
    
    Args:
        normalized_df: DataFrame đã chuẩn hóa
    """
    print("\n" + "=" * 80)
    print("THỐNG KÊ DỮ LIỆU ĐÃ CHUẨN HÓA")
    print("=" * 80)
    
    popularity_scores = normalized_df['popularity_score'].to_numpy()
    rating_scores = normalized_df['rating_score'].to_numpy()
    
    print(f"\npopularity_score:")
    print(f"  - Min: {popularity_scores.min():.6f}")
    print(f"  - Max: {popularity_scores.max():.6f}")
    print(f"  - Mean: {popularity_scores.mean():.6f}")
    print(f"  - Median: {np.median(popularity_scores):.6f}")
    
    print(f"\nrating_score:")
    print(f"  - Min: {rating_scores.min():.6f}")
    print(f"  - Max: {rating_scores.max():.6f}")
    print(f"  - Mean: {rating_scores.mean():.6f}")
    print(f"  - Median: {np.median(rating_scores):.6f}")


def display_sample(normalized_df: pl.DataFrame, num_samples: int = 10):
    """
    Hiển thị một vài dòng sample để kiểm tra.
    
    Args:
        normalized_df: DataFrame đã chuẩn hóa
        num_samples: Số lượng samples cần hiển thị
    """
    print("\n" + "=" * 80)
    print("SAMPLE DỮ LIỆU ĐÃ CHUẨN HÓA")
    print("=" * 80)
    
    num_samples = min(num_samples, len(normalized_df))
    sample_df = normalized_df.head(num_samples)
    
    print(f"\n{num_samples} dòng đầu tiên:")
    print(f"\n{'Item ID':<15} {'Popularity Score':<20} {'Rating Score':<15}")
    print("-" * 50)
    
    for row in sample_df.iter_rows(named=True):
        item_id = row['item_id']
        popularity_score = row['popularity_score']
        rating_score = row['rating_score']
        print(f"{item_id:<15} {popularity_score:<20.6f} {rating_score:<15.6f}")


def save_normalized_data(normalized_df: pl.DataFrame, output_path: Path):
    """
    Lưu dữ liệu đã chuẩn hóa ra file parquet.
    
    Args:
        normalized_df: DataFrame đã chuẩn hóa
        output_path: Đường dẫn file output
    """
    print("\n" + "=" * 80)
    print("BƯỚC 3: LƯU KẾT QUẢ")
    print("=" * 80)
    
    # Tạo thư mục nếu chưa có
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\nĐang lưu normalized popularity: {output_path}")
    normalized_df.write_parquet(str(output_path))
    
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"[OK] Đã lưu {len(normalized_df):,} items vào {output_path}")
    print(f"  File size: {file_size_mb:.2f} MB")
    
    # In schema
    print(f"\nSchema của file output:")
    for col, dtype in zip(normalized_df.columns, normalized_df.dtypes):
        print(f"  {col}: {dtype}")


def main():
    """
    Hàm chính để chạy toàn bộ pipeline.
    """
    print("\n" + "=" * 80)
    print("PIPELINE CHUẨN HÓA ITEM POPULARITY SIGNAL")
    print("=" * 80)
    
    # Đường dẫn
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    input_path = BASE_DIR / "artifacts" / "popularity" / "item_popularity.parquet"
    output_path = BASE_DIR / "artifacts" / "popularity" / "item_popularity_normalized.parquet"
    
    try:
        # Bước 1: Đọc dữ liệu
        df = load_popularity_data(input_path)
        
        # Bước 2: Chuẩn hóa
        normalized_df = normalize_popularity(df)
        
        # Hiển thị thống kê
        display_statistics(normalized_df)
        
        # Hiển thị sample
        display_sample(normalized_df, num_samples=10)
        
        # Bước 3: Lưu kết quả
        save_normalized_data(normalized_df, output_path)
        
        # Tóm tắt
        print("\n" + "=" * 80)
        print("TÓM TẮT")
        print("=" * 80)
        print(f"Tổng số items: {len(normalized_df):,}")
        print(f"Output file: {output_path}")
        print(f"\nSchema output:")
        print(f"  - item_id: string")
        print(f"  - popularity_score: float64 (log + min-max scaled, range [0, 1])")
        print(f"  - rating_score: float64 (linear scaled, range [0, 1])")
        print(f"\n[OK] Normalized popularity features đã sẵn sàng để dùng cho ranking model!")
        
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

