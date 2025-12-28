"""
Phase 4: Build Interactions for Recommendation System
======================================================
Mục tiêu: Xây dựng dữ liệu lõi cho Recommendation System
- Tạo item_id_train cho reviews
- Join reviews với metadata
- Tạo interactions table
- Tạo items table cho RS

Chạy độc lập: python app/data_preprocessing/phase4_build_interactions.py
"""

import polars as pl
from pathlib import Path
import sys
import io

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


def load_cleaned_data(project_root: Path):
    """
    Load dữ liệu đã clean từ Phase 3.
    
    Args:
        project_root: Đường dẫn đến root của project
        
    Returns:
        Tuple (reviews_df, metadata_df)
    """
    data_processed_dir = project_root / "data" / "processed"
    
    reviews_path = data_processed_dir / "reviews_clean.parquet"
    metadata_path = data_processed_dir / "metadata_clean.parquet"
    
    if not reviews_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {reviews_path}")
    
    if not metadata_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {metadata_path}")
    
    print("Đang load dữ liệu đã clean từ Phase 3...")
    reviews_df = pl.read_parquet(str(reviews_path))
    metadata_df = pl.read_parquet(str(metadata_path))
    
    return reviews_df, metadata_df


def step1_create_item_id_train(reviews_df: pl.DataFrame) -> pl.DataFrame:
    """
    Step 1: Tạo cột item_id_train cho reviews.
    
    item_id_train = parent_asin nếu không null
    fallback sang asin nếu parent_asin null
    """
    print("\n[Step 1] Tạo item_id_train cho reviews...")
    
    reviews_with_item_id = reviews_df.with_columns([
        pl.when(pl.col("parent_asin").is_not_null())
        .then(pl.col("parent_asin"))
        .otherwise(pl.col("asin"))
        .alias("item_id_train")
    ])
    
    # Thống kê
    parent_asin_count = reviews_with_item_id.filter(pl.col("parent_asin").is_not_null()).height
    asin_fallback_count = reviews_with_item_id.filter(pl.col("parent_asin").is_null()).height
    
    print(f"  Sử dụng parent_asin: {parent_asin_count:,} records")
    print(f"  Fallback sang asin: {asin_fallback_count:,} records")
    
    return reviews_with_item_id


def step2_join_reviews_metadata(reviews_df: pl.DataFrame, metadata_df: pl.DataFrame) -> pl.DataFrame:
    """
    Step 2: Join reviews với metadata theo item_id_train = metadata.parent_asin.
    
    Giữ LEFT JOIN (không drop review nếu thiếu metadata).
    """
    print("\n[Step 2] Join reviews với metadata (LEFT JOIN)...")
    
    # Đảm bảo metadata có cột parent_asin (đã có sẵn)
    # Join theo item_id_train = parent_asin
    joined_df = reviews_df.join(
        metadata_df,
        left_on="item_id_train",
        right_on="parent_asin",
        how="left"
    )
    
    # Thống kê
    total_reviews = len(reviews_df)
    reviews_with_metadata = joined_df.filter(pl.col("parent_asin").is_not_null()).height
    reviews_without_metadata = total_reviews - reviews_with_metadata
    
    print(f"  Tổng số reviews: {total_reviews:,}")
    print(f"  Reviews có metadata: {reviews_with_metadata:,} ({reviews_with_metadata/total_reviews*100:.2f}%)")
    print(f"  Reviews không có metadata: {reviews_without_metadata:,} ({reviews_without_metadata/total_reviews*100:.2f}%)")
    
    return joined_df


def step3_create_interactions(joined_df: pl.DataFrame) -> pl.DataFrame:
    """
    Step 3: Tạo interaction table (RS core).
    
    Tạo DataFrame interactions_all với các cột:
    - user_id (từ amazon_user_id)
    - item_id (=item_id_train)
    - rating
    - ts
    
    Sort theo ts.
    """
    print("\n[Step 3] Tạo interactions table...")
    
    interactions = joined_df.select([
        pl.col("amazon_user_id").alias("user_id"),
        pl.col("item_id_train").alias("item_id"),
        pl.col("rating"),
        pl.col("ts"),
    ]).sort("ts")
    
    print(f"  Tổng số interactions: {len(interactions):,}")
    print(f"  Số user unique: {interactions['user_id'].n_unique():,}")
    print(f"  Số item unique: {interactions['item_id'].n_unique():,}")
    
    return interactions


def step4_create_items_table(metadata_df: pl.DataFrame) -> pl.DataFrame:
    """
    Step 4: Tạo items table cho RS.
    
    Tạo DataFrame items_for_rs với:
    - item_id (=parent_asin)
    - title
    - store
    - main_category
    - avg_rating
    - rating_number
    - primary_image
    - raw_metadata
    
    Remove duplicate theo item_id.
    """
    print("\n[Step 4] Tạo items table cho RS...")
    
    items = metadata_df.select([
        pl.col("parent_asin").alias("item_id"),
        pl.col("title"),
        pl.col("store"),
        pl.col("main_category"),
        pl.col("avg_rating"),
        pl.col("rating_number"),
        pl.col("primary_image"),
        pl.col("raw_metadata"),
    ])
    
    # Remove duplicate theo item_id
    before = len(items)
    items = items.unique(subset=["item_id"], keep="first")
    after = len(items)
    dropped = before - after
    
    print(f"  Trước deduplication: {before:,} items")
    print(f"  Sau deduplication: {after:,} items")
    print(f"  Bị loại: {dropped:,} duplicate items")
    
    return items


def save_interactions_data(interactions_df: pl.DataFrame, items_df: pl.DataFrame, output_dir: Path):
    """
    Lưu interactions và items ra parquet files.
    
    Args:
        interactions_df: Interactions DataFrame
        items_df: Items DataFrame
        output_dir: Thư mục output
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    interactions_path = output_dir / "interactions_all.parquet"
    items_path = output_dir / "items_for_rs.parquet"
    
    print(f"\nĐang lưu interactions: {interactions_path}")
    interactions_df.write_parquet(str(interactions_path))
    
    print(f"Đang lưu items: {items_path}")
    items_df.write_parquet(str(items_path))
    
    print("\n[OK] Đã lưu dữ liệu interactions thành công!")


def main():
    """Hàm chính để chạy phase 4 build interactions."""
    # Xác định đường dẫn project root
    script_path = Path(__file__).resolve()
    current = script_path.parent
    while current != current.parent:
        data_dir = current / "data"
        if data_dir.exists() and (data_dir / "processed").exists():
            project_root = current
            break
        current = current.parent
    else:
        project_root = script_path.parent.parent.parent
    
    data_processed_dir = project_root / "data" / "processed"
    
    print("=" * 80)
    print("PHASE 4: BUILD INTERACTIONS FOR RECOMMENDATION SYSTEM")
    print("=" * 80)
    
    # Load dữ liệu đã clean
    reviews_df, metadata_df = load_cleaned_data(project_root)
    print(f"[OK] Đã load reviews: {len(reviews_df):,} dòng")
    print(f"[OK] Đã load metadata: {len(metadata_df):,} dòng")
    
    # Step 1: Tạo item_id_train
    reviews_with_item_id = step1_create_item_id_train(reviews_df)
    
    # Step 2: Join reviews với metadata
    joined_df = step2_join_reviews_metadata(reviews_with_item_id, metadata_df)
    
    # Step 3: Tạo interactions table
    interactions_all = step3_create_interactions(joined_df)
    
    # Step 4: Tạo items table
    items_for_rs = step4_create_items_table(metadata_df)
    
    # Lưu ra parquet
    save_interactions_data(interactions_all, items_for_rs, data_processed_dir)
    
    # In kết quả tổng hợp
    print("\n" + "=" * 80)
    print("KẾT QUẢ TỔNG HỢP")
    print("=" * 80)
    
    print("\n[INTERACTIONS_ALL]")
    print("-" * 80)
    print(f"Số dòng interactions: {len(interactions_all):,}")
    print(f"Số user unique: {interactions_all['user_id'].n_unique():,}")
    print(f"Số item unique: {interactions_all['item_id'].n_unique():,}")
    print("\nSchema:")
    print(interactions_all.schema)
    
    print("\n[ITEMS_FOR_RS]")
    print("-" * 80)
    print(f"Số item unique: {len(items_for_rs):,}")
    print("\nSchema:")
    print(items_for_rs.schema)
    
    # Thống kê thêm
    print("\n[THỐNG KÊ BỔ SUNG]")
    print("-" * 80)
    avg_rating = interactions_all["rating"].mean()
    print(f"Rating trung bình: {avg_rating:.2f}")
    
    interactions_per_user = interactions_all.group_by("user_id").agg(pl.len().alias("count"))
    avg_interactions_per_user = interactions_per_user["count"].mean()
    print(f"Trung bình interactions/user: {avg_interactions_per_user:.2f}")
    
    interactions_per_item = interactions_all.group_by("item_id").agg(pl.len().alias("count"))
    avg_interactions_per_item = interactions_per_item["count"].mean()
    print(f"Trung bình interactions/item: {avg_interactions_per_item:.2f}")
    
    print("\n" + "=" * 80)
    print("[OK] PHASE 4 HOÀN TẤT: Dữ liệu interactions đã được xây dựng thành công!")
    print("=" * 80)
    
    return interactions_all, items_for_rs


if __name__ == "__main__":
    main()

