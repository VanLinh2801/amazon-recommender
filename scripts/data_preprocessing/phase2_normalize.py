"""
Phase 2: Schema Normalization
==============================
Mục tiêu: Chuẩn hóa schema của reviews và metadata
- Đổi tên cột theo chuẩn
- Convert timestamp
- Tạo các cột mới cần thiết
- Lưu ra parquet files

Chạy độc lập: python app/data_preprocessing/phase2_normalize.py
"""

import polars as pl
from pathlib import Path
import sys
import io
import json

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


def load_raw_data(project_root: Path):
    """
    Load dữ liệu thô từ Phase 1.
    
    Args:
        project_root: Đường dẫn đến root của project
        
    Returns:
        Tuple (reviews_df, metadata_df)
    """
    data_raw_dir = project_root / "data" / "raw"
    
    # Tìm file reviews
    reviews_file = None
    for ext in [".jsonl", ".json"]:
        candidate = data_raw_dir / f"All_Beauty{ext}"
        if candidate.exists():
            reviews_file = candidate
            break
    
    # Tìm file metadata
    metadata_file = None
    for ext in [".jsonl", ".json"]:
        candidate = data_raw_dir / f"meta_All_Beauty{ext}"
        if candidate.exists():
            metadata_file = candidate
            break
    
    if not reviews_file or not reviews_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file reviews: {data_raw_dir}")
    
    if not metadata_file or not metadata_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file metadata: {data_raw_dir}")
    
    print("Đang load dữ liệu thô từ Phase 1...")
    reviews_df = pl.read_ndjson(str(reviews_file))
    metadata_df = pl.read_ndjson(str(metadata_file))
    
    return reviews_df, metadata_df


def normalize_reviews(reviews_df: pl.DataFrame) -> pl.DataFrame:
    """
    Chuẩn hóa schema của reviews DataFrame.
    
    Yêu cầu:
    - Đổi tên: user_id → amazon_user_id, title → review_title, 
               text → review_text, verified_purchase → verified
    - Giữ: asin, parent_asin, rating, helpful_vote, timestamp
    - Convert timestamp (ms) → ts (datetime)
    - Drop cột images
    """
    print("\n[REVIEWS] Đang chuẩn hóa schema...")
    
    # Chọn và đổi tên các cột cần giữ
    normalized = reviews_df.select([
        pl.col("user_id").alias("amazon_user_id"),
        pl.col("asin"),
        pl.col("parent_asin"),
        pl.col("rating"),
        pl.col("title").alias("review_title"),
        pl.col("text").alias("review_text"),
        pl.col("helpful_vote"),
        pl.col("verified_purchase").alias("verified"),
        pl.col("timestamp"),
    ])
    
    # Convert timestamp từ milliseconds sang datetime
    normalized = normalized.with_columns([
        (pl.col("timestamp") / 1000).cast(pl.Datetime(time_unit="ms", time_zone=None)).alias("ts")
    ]).drop("timestamp")
    
    return normalized


def normalize_metadata(metadata_df: pl.DataFrame) -> pl.DataFrame:
    """
    Chuẩn hóa schema của metadata DataFrame.
    
    Yêu cầu:
    - Giữ: parent_asin, title, store, main_category, 
           average_rating → avg_rating, rating_number, images, details, price
    - Tạo primary_image: ưu tiên images[0].large, nếu null thì images[0].hi_res
    - Gộp toàn bộ record gốc vào raw_metadata (JSON)
    """
    print("\n[METADATA] Đang chuẩn hóa schema...")
    
    # Tạo raw_metadata từ DataFrame gốc trước (trước khi normalize)
    def row_to_json(row_dict):
        """Convert row dict thành JSON string."""
        try:
            return json.dumps(row_dict, default=str, ensure_ascii=False)
        except:
            return None
    
    # Tạo struct từ tất cả các cột gốc và convert sang JSON
    all_original_cols = [pl.col(col) for col in metadata_df.columns]
    raw_metadata_col = pl.struct(all_original_cols).map_elements(
        row_to_json,
        return_dtype=pl.String
    ).alias("raw_metadata")
    
    # Chọn các cột cần giữ và đổi tên, đồng thời thêm raw_metadata
    normalized = metadata_df.select([
        pl.col("parent_asin"),
        pl.col("title"),
        pl.col("store"),
        pl.col("main_category"),
        pl.col("average_rating").alias("avg_rating"),
        pl.col("rating_number"),
        pl.col("images"),
        pl.col("details"),
        pl.col("price"),
        raw_metadata_col,
    ])
    
    # Tạo cột primary_image
    # Ưu tiên images[0].large, nếu null thì images[0].hi_res
    def extract_primary_image(images_list):
        """Extract primary image URL từ list images."""
        if images_list is None or len(images_list) == 0:
            return None
        first_img = images_list[0]
        if first_img is None:
            return None
        # Lấy large nếu có, nếu không thì lấy hi_res
        if "large" in first_img and first_img["large"]:
            return first_img["large"]
        elif "hi_res" in first_img and first_img["hi_res"]:
            return first_img["hi_res"]
        return None
    
    normalized = normalized.with_columns([
        pl.col("images").map_elements(
            extract_primary_image,
            return_dtype=pl.String
        ).alias("primary_image")
    ])
    
    return normalized


def save_normalized_data(reviews_df: pl.DataFrame, metadata_df: pl.DataFrame, output_dir: Path):
    """
    Lưu các DataFrame đã chuẩn hóa ra parquet files.
    
    Args:
        reviews_df: Reviews DataFrame đã chuẩn hóa
        metadata_df: Metadata DataFrame đã chuẩn hóa
        output_dir: Thư mục output
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    reviews_path = output_dir / "reviews_normalized.parquet"
    metadata_path = output_dir / "metadata_normalized.parquet"
    
    print(f"\nĐang lưu reviews: {reviews_path}")
    reviews_df.write_parquet(str(reviews_path))
    
    print(f"Đang lưu metadata: {metadata_path}")
    metadata_df.write_parquet(str(metadata_path))
    
    print("\n[OK] Đã lưu dữ liệu chuẩn hóa thành công!")


def main():
    """Hàm chính để chạy phase 2 normalization."""
    # Xác định đường dẫn project root
    script_path = Path(__file__).resolve()
    current = script_path.parent
    while current != current.parent:
        data_dir = current / "data"
        if data_dir.exists() and (data_dir / "raw").exists():
            project_root = current
            break
        current = current.parent
    else:
        project_root = script_path.parent.parent.parent
    
    data_processed_dir = project_root / "data" / "processed"
    
    print("=" * 80)
    print("PHASE 2: SCHEMA NORMALIZATION")
    print("=" * 80)
    
    # Load dữ liệu thô
    reviews_raw, metadata_raw = load_raw_data(project_root)
    print(f"[OK] Đã load reviews: {len(reviews_raw):,} dòng")
    print(f"[OK] Đã load metadata: {len(metadata_raw):,} dòng")
    
    # Normalize reviews
    reviews_normalized = normalize_reviews(reviews_raw)
    
    # Normalize metadata
    metadata_normalized = normalize_metadata(metadata_raw)
    
    # Lưu ra parquet
    save_normalized_data(reviews_normalized, metadata_normalized, data_processed_dir)
    
    # In kết quả
    print("\n" + "=" * 80)
    print("KẾT QUẢ CHUẨN HÓA")
    print("=" * 80)
    
    print("\n[REVIEWS NORMALIZED]")
    print("-" * 80)
    print(f"Số dòng: {len(reviews_normalized):,}")
    print(f"Số cột: {len(reviews_normalized.columns)}")
    print("\nSchema:")
    print(reviews_normalized.schema)
    
    print("\n[METADATA NORMALIZED]")
    print("-" * 80)
    print(f"Số dòng: {len(metadata_normalized):,}")
    print(f"Số cột: {len(metadata_normalized.columns)}")
    print("\nSchema:")
    print(metadata_normalized.schema)
    
    print("\n" + "=" * 80)
    print("[OK] PHASE 2 HOÀN TẤT: Schema đã được chuẩn hóa thành công!")
    print("=" * 80)
    
    return reviews_normalized, metadata_normalized


if __name__ == "__main__":
    main()

