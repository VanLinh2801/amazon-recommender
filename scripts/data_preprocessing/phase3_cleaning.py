"""
Phase 3: Data Cleaning
======================
Mục tiêu: Làm sạch dữ liệu với 5 tasks cho reviews và metadata
- Missing values handling
- Sanity checks
- Deduplication
- Type normalization
- Feature pruning

Chạy độc lập: python app/data_preprocessing/phase3_cleaning.py
"""

import polars as pl
from pathlib import Path
import sys
import io

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


def load_normalized_data(project_root: Path):
    """
    Load dữ liệu đã normalize từ Phase 2.
    
    Args:
        project_root: Đường dẫn đến root của project
        
    Returns:
        Tuple (reviews_df, metadata_df)
    """
    data_processed_dir = project_root / "data" / "processed"
    
    reviews_path = data_processed_dir / "reviews_normalized.parquet"
    metadata_path = data_processed_dir / "metadata_normalized.parquet"
    
    if not reviews_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {reviews_path}")
    
    if not metadata_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {metadata_path}")
    
    print("Đang load dữ liệu đã normalize từ Phase 2...")
    reviews_df = pl.read_parquet(str(reviews_path))
    metadata_df = pl.read_parquet(str(metadata_path))
    
    return reviews_df, metadata_df


def clean_reviews(reviews_df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    """
    Clean reviews DataFrame với 5 tasks.
    
    Returns:
        Tuple (cleaned_df, stats_dict)
    """
    print("\n" + "=" * 80)
    print("REVIEWS CLEANING")
    print("=" * 80)
    
    initial_count = len(reviews_df)
    stats = {"initial": initial_count}
    current_df = reviews_df
    
    # Task 1: Missing values
    print("\n[Task 1] Missing values handling...")
    before = len(current_df)
    
    # Drop record thiếu amazon_user_id hoặc asin hoặc rating
    current_df = current_df.filter(
        pl.col("amazon_user_id").is_not_null() &
        pl.col("asin").is_not_null() &
        pl.col("rating").is_not_null()
    )
    
    # Fill missing values
    current_df = current_df.with_columns([
        pl.col("review_title").fill_null(""),
        pl.col("review_text").fill_null(""),
        pl.col("helpful_vote").fill_null(0),
        pl.col("verified").fill_null(False),
    ])
    
    after = len(current_df)
    dropped = before - after
    stats["task1_dropped"] = dropped
    print(f"  Dropped {dropped:,} records (thiếu amazon_user_id/asin/rating)")
    print(f"  Filled missing values: review_title, review_text, helpful_vote, verified")
    
    # Task 2: Sanity check
    print("\n[Task 2] Sanity checks...")
    before = len(current_df)
    
    # Giữ rating trong [1, 5]
    current_df = current_df.filter(
        (pl.col("rating") >= 1) & (pl.col("rating") <= 5)
    )
    
    # helpful_vote < 0 → set = 0
    current_df = current_df.with_columns([
        pl.when(pl.col("helpful_vote") < 0)
        .then(0)
        .otherwise(pl.col("helpful_vote"))
        .alias("helpful_vote")
    ])
    
    after = len(current_df)
    dropped = before - after
    stats["task2_dropped"] = dropped
    
    # Đếm số record bị fix helpful_vote (trước khi fix)
    fixed_count = len(current_df.filter(pl.col("helpful_vote") == 0)) - len(
        reviews_df.filter(
            (pl.col("helpful_vote").is_not_null()) & 
            (pl.col("helpful_vote") == 0)
        )
    ) if len(current_df) > 0 else 0
    
    print(f"  Dropped {dropped:,} records (rating ngoài [1, 5])")
    # Note: Khó đếm chính xác số record bị fix vì cần so sánh với data trước đó
    print(f"  Fixed helpful_vote < 0 → 0")
    
    # Task 3: Deduplication
    print("\n[Task 3] Deduplication...")
    before = len(current_df)
    
    # Remove duplicate theo (amazon_user_id, asin, ts)
    # Nếu có duplicate theo cả 3 cột, giữ 1 record (đã sort theo ts desc nên giữ mới nhất)
    # Nếu chỉ duplicate theo (amazon_user_id, asin), giữ record có ts mới nhất
    current_df = current_df.sort("ts", descending=True)
    # Đầu tiên remove duplicate theo (amazon_user_id, asin, ts)
    current_df = current_df.unique(subset=["amazon_user_id", "asin", "ts"], keep="first")
    # Sau đó remove duplicate theo (amazon_user_id, asin) và giữ mới nhất theo ts
    current_df = current_df.unique(subset=["amazon_user_id", "asin"], keep="first")
    
    after = len(current_df)
    dropped = before - after
    stats["task3_dropped"] = dropped
    print(f"  Dropped {dropped:,} duplicate records (giữ mới nhất theo ts)")
    
    # Task 4: Type normalization
    print("\n[Task 4] Type normalization...")
    current_df = current_df.with_columns([
        pl.col("rating").cast(pl.Float64),
        pl.col("helpful_vote").cast(pl.Int64),
        pl.col("verified").cast(pl.Boolean),
    ])
    print("  Cast: rating → Float64, helpful_vote → Int64, verified → Boolean")
    
    # Task 5: Feature pruning
    print("\n[Task 5] Feature pruning...")
    columns_to_keep = [
        "amazon_user_id",
        "asin",
        "parent_asin",
        "rating",
        "review_title",
        "review_text",
        "helpful_vote",
        "verified",
        "ts",
    ]
    current_df = current_df.select(columns_to_keep)
    print(f"  Giữ {len(columns_to_keep)} cột: {', '.join(columns_to_keep)}")
    
    final_count = len(current_df)
    stats["final"] = final_count
    stats["total_dropped"] = initial_count - final_count
    
    print(f"\n[SUMMARY] Reviews cleaning:")
    print(f"  Ban đầu: {initial_count:,} records")
    print(f"  Sau cleaning: {final_count:,} records")
    print(f"  Tổng số bị loại: {stats['total_dropped']:,} records ({stats['total_dropped']/initial_count*100:.2f}%)")
    
    return current_df, stats


def clean_metadata(metadata_df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    """
    Clean metadata DataFrame với 5 tasks.
    
    Returns:
        Tuple (cleaned_df, stats_dict)
    """
    print("\n" + "=" * 80)
    print("METADATA CLEANING")
    print("=" * 80)
    
    initial_count = len(metadata_df)
    stats = {"initial": initial_count}
    current_df = metadata_df
    
    # Task 1: Missing values
    print("\n[Task 1] Missing values handling...")
    before = len(current_df)
    
    # Drop record thiếu parent_asin hoặc title
    current_df = current_df.filter(
        pl.col("parent_asin").is_not_null() &
        pl.col("title").is_not_null()
    )
    
    # Fill missing values
    current_df = current_df.with_columns([
        pl.col("store").fill_null("Unknown"),
        pl.col("rating_number").fill_null(0),
    ])
    
    after = len(current_df)
    dropped = before - after
    stats["task1_dropped"] = dropped
    print(f"  Dropped {dropped:,} records (thiếu parent_asin/title)")
    print(f"  Filled missing values: store='Unknown', rating_number=0")
    
    # Task 2: Sanity check
    print("\n[Task 2] Sanity checks...")
    before = len(current_df)
    
    # rating_number < 0 → set = 0
    current_df = current_df.with_columns([
        pl.when(pl.col("rating_number") < 0)
        .then(0)
        .otherwise(pl.col("rating_number"))
        .alias("rating_number")
    ])
    
    after = len(current_df)
    dropped = before - after
    stats["task2_dropped"] = dropped
    fixed_count = len(current_df.filter(pl.col("rating_number") == 0))
    print(f"  Fixed {fixed_count:,} records (rating_number < 0 → 0)")
    
    # Task 3: Deduplication
    print("\n[Task 3] Deduplication...")
    before = len(current_df)
    
    # Remove duplicate theo parent_asin
    # Ưu tiên giữ record có: primary_image khác null, details không rỗng
    # Tạo score để sort: primary_image not null = 2, details not null = 1
    current_df = current_df.with_columns([
        (pl.col("primary_image").is_not_null().cast(pl.Int64) * 2 +
         pl.col("details").is_not_null().cast(pl.Int64) * 1).alias("_priority_score")
    ])
    
    # Sort theo priority score (desc) rồi giữ first
    current_df = current_df.sort("_priority_score", descending=True)
    current_df = current_df.unique(subset=["parent_asin"], keep="first")
    current_df = current_df.drop("_priority_score")
    
    after = len(current_df)
    dropped = before - after
    stats["task3_dropped"] = dropped
    print(f"  Dropped {dropped:,} duplicate records (ưu tiên primary_image và details)")
    
    # Task 4: Type normalization
    print("\n[Task 4] Type normalization...")
    current_df = current_df.with_columns([
        pl.col("avg_rating").cast(pl.Float64),
        pl.col("rating_number").cast(pl.Int64),
    ])
    print("  Cast: avg_rating → Float64, rating_number → Int64")
    
    # Task 5: Category cleaning and normalization
    print("\n[Task 5] Category cleaning and normalization...")
    import json
    
    def extract_and_normalize_category(raw_metadata_str: str, current_category: str) -> str:
        """
        Extract và normalize category từ raw_metadata.
        
        Logic:
        1. Nếu main_category là "All Beauty" (tên dataset), cố gắng extract từ raw_metadata
        2. Extract từ categories list nếu có
        3. Extract từ details nếu có
        4. Nếu không tìm thấy, giữ nguyên hoặc set thành "Beauty" (generic)
        """
        if current_category and current_category != "All Beauty":
            return current_category
        
        if not raw_metadata_str:
            return "Beauty"  # Default category
        
        try:
            raw = json.loads(raw_metadata_str)
            
            # Thử extract từ categories list
            categories = raw.get("categories", [])
            if categories and len(categories) > 0:
                # categories thường là nested list: [["Beauty", "Personal Care", "Hair Care"]]
                # Lấy category đầu tiên của level đầu tiên
                if isinstance(categories[0], list) and len(categories[0]) > 0:
                    cat = categories[0][0]
                    if cat and cat != "All Beauty":
                        return cat
                elif isinstance(categories[0], str):
                    cat = categories[0]
                    if cat and cat != "All Beauty":
                        return cat
            
            # Thử extract từ details
            details = raw.get("details", {})
            if isinstance(details, dict):
                # Tìm các keys liên quan đến category
                for key in ["category", "main_category", "product_category", "category_name"]:
                    if key in details:
                        cat = details[key]
                        if cat and cat != "All Beauty":
                            return str(cat)
            
            # Nếu vẫn không tìm thấy, thử extract từ title (keyword-based)
            title = raw.get("title", "").lower()
            category_keywords = {
                "hair": "Hair Care",
                "skin": "Skin Care",
                "makeup": "Makeup",
                "fragrance": "Fragrance",
                "nail": "Nail Care",
                "bath": "Bath & Body",
                "oral": "Oral Care",
                "shave": "Shaving",
                "wig": "Hair Care",
                "cleansing": "Skin Care",
                "moisturizer": "Skin Care",
                "serum": "Skin Care",
                "lip": "Makeup",
                "eye": "Makeup",
                "foundation": "Makeup",
                "perfume": "Fragrance",
                "cologne": "Fragrance",
            }
            
            for keyword, category in category_keywords.items():
                if keyword in title:
                    return category
            
            # Default: Beauty
            return "Beauty"
            
        except (json.JSONDecodeError, KeyError, IndexError, TypeError):
            # Nếu có lỗi, giữ nguyên hoặc return default
            return "Beauty"
    
    # Apply category normalization
    before_cat_normalization = len(current_df.filter(pl.col("main_category") == "All Beauty"))
    
    current_df = current_df.with_columns([
        pl.struct(["raw_metadata", "main_category"]).map_elements(
            lambda x: extract_and_normalize_category(x["raw_metadata"], x["main_category"]),
            return_dtype=pl.Utf8
        ).alias("main_category")
    ])
    
    after_cat_normalization = len(current_df.filter(pl.col("main_category") == "All Beauty"))
    normalized_count = before_cat_normalization - after_cat_normalization
    
    # Thống kê category distribution
    cat_dist = current_df.group_by("main_category").agg(pl.count().alias("count")).sort("count", descending=True)
    print(f"  Normalized {normalized_count:,} products từ 'All Beauty' sang category cụ thể")
    print(f"  Category distribution:")
    for row in cat_dist.head(10).to_dicts():
        print(f"    {row['main_category']}: {row['count']:,}")
    
    stats["task5_normalized"] = normalized_count
    
    # Task 6: Feature pruning
    print("\n[Task 5] Feature pruning...")
    columns_to_keep = [
        "parent_asin",
        "title",
        "store",
        "main_category",
        "avg_rating",
        "rating_number",
        "primary_image",
        "raw_metadata",
    ]
    current_df = current_df.select(columns_to_keep)
    print(f"  Giữ {len(columns_to_keep)} cột: {', '.join(columns_to_keep)}")
    
    final_count = len(current_df)
    stats["final"] = final_count
    stats["total_dropped"] = initial_count - final_count
    
    print(f"\n[SUMMARY] Metadata cleaning:")
    print(f"  Ban đầu: {initial_count:,} records")
    print(f"  Sau cleaning: {final_count:,} records")
    print(f"  Tổng số bị loại: {stats['total_dropped']:,} records ({stats['total_dropped']/initial_count*100:.2f}%)")
    
    return current_df, stats


def save_cleaned_data(reviews_df: pl.DataFrame, metadata_df: pl.DataFrame, output_dir: Path):
    """
    Lưu các DataFrame đã clean ra parquet files.
    
    Args:
        reviews_df: Reviews DataFrame đã clean
        metadata_df: Metadata DataFrame đã clean
        output_dir: Thư mục output
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    reviews_path = output_dir / "reviews_clean.parquet"
    metadata_path = output_dir / "metadata_clean.parquet"
    
    print(f"\nĐang lưu reviews: {reviews_path}")
    reviews_df.write_parquet(str(reviews_path))
    
    print(f"Đang lưu metadata: {metadata_path}")
    metadata_df.write_parquet(str(metadata_path))
    
    print("\n[OK] Đã lưu dữ liệu clean thành công!")


def main():
    """Hàm chính để chạy phase 3 cleaning."""
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
    print("PHASE 3: DATA CLEANING")
    print("=" * 80)
    
    # Load dữ liệu đã normalize
    reviews_raw, metadata_raw = load_normalized_data(project_root)
    print(f"[OK] Đã load reviews: {len(reviews_raw):,} dòng")
    print(f"[OK] Đã load metadata: {len(metadata_raw):,} dòng")
    
    # Clean reviews
    reviews_clean, reviews_stats = clean_reviews(reviews_raw)
    
    # Clean metadata
    metadata_clean, metadata_stats = clean_metadata(metadata_raw)
    
    # Lưu ra parquet
    save_cleaned_data(reviews_clean, metadata_clean, data_processed_dir)
    
    # In kết quả tổng hợp
    print("\n" + "=" * 80)
    print("KẾT QUẢ TỔNG HỢP")
    print("=" * 80)
    
    print("\n[REVIEWS CLEAN]")
    print("-" * 80)
    print(f"Số dòng trước cleaning: {reviews_stats['initial']:,}")
    print(f"Số dòng sau cleaning: {reviews_stats['final']:,}")
    print(f"Task 1 (Missing values): -{reviews_stats['task1_dropped']:,}")
    print(f"Task 2 (Sanity check): -{reviews_stats['task2_dropped']:,}")
    print(f"Task 3 (Deduplication): -{reviews_stats['task3_dropped']:,}")
    print(f"Tổng số bị loại: {reviews_stats['total_dropped']:,} ({reviews_stats['total_dropped']/reviews_stats['initial']*100:.2f}%)")
    print("\nSchema:")
    print(reviews_clean.schema)
    
    print("\n[METADATA CLEAN]")
    print("-" * 80)
    print(f"Số dòng trước cleaning: {metadata_stats['initial']:,}")
    print(f"Số dòng sau cleaning: {metadata_stats['final']:,}")
    print(f"Task 1 (Missing values): -{metadata_stats['task1_dropped']:,}")
    print(f"Task 2 (Sanity check): -{metadata_stats['task2_dropped']:,}")
    print(f"Task 3 (Deduplication): -{metadata_stats['task3_dropped']:,}")
    if 'task5_normalized' in metadata_stats:
        print(f"Task 5 (Category normalization): {metadata_stats['task5_normalized']:,} products normalized")
    print(f"Tổng số bị loại: {metadata_stats['total_dropped']:,} ({metadata_stats['total_dropped']/metadata_stats['initial']*100:.2f}%)")
    print("\nSchema:")
    print(metadata_clean.schema)
    
    print("\n" + "=" * 80)
    print("[OK] PHASE 3 HOÀN TẤT: Dữ liệu đã được clean thành công!")
    print("=" * 80)
    
    return reviews_clean, metadata_clean


if __name__ == "__main__":
    main()

