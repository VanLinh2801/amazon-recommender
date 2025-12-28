"""
Phase 5: Build 5-Core Dataset and Train/Test Split
===================================================
Mục tiêu: Xây dựng 5-core dataset và split train/test
- Iterative filtering để đảm bảo mỗi user và item có >= 5 interactions
- Sort theo user_id và ts
- Time-based split: giữ 1 interaction cuối làm test

Chạy độc lập: python app/data_preprocessing/phase5_build_5core.py
"""

import polars as pl
from pathlib import Path
import sys
import io

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


def load_interactions(project_root: Path) -> pl.DataFrame:
    """
    Load interactions từ Phase 4.
    
    Args:
        project_root: Đường dẫn đến root của project
        
    Returns:
        Interactions DataFrame
    """
    data_processed_dir = project_root / "data" / "processed"
    
    interactions_path = data_processed_dir / "interactions_all.parquet"
    
    if not interactions_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {interactions_path}")
    
    print("Đang load interactions từ Phase 4...")
    interactions_df = pl.read_parquet(str(interactions_path))
    
    return interactions_df


def step1_build_5core(interactions_df: pl.DataFrame) -> pl.DataFrame:
    """
    Step 1: Build 5-core dataset với iterative filtering.
    
    Lọc dữ liệu sao cho:
    - mỗi user_id có >= 5 interactions
    - mỗi item_id có >= 5 interactions
    
    Thực hiện iterative filtering:
    filter item → filter user → lặp đến khi ổn định
    """
    print("\n[Step 1] Building 5-core dataset với iterative filtering...")
    
    initial_count = len(interactions_df)
    initial_users = interactions_df["user_id"].n_unique()
    initial_items = interactions_df["item_id"].n_unique()
    
    print(f"  Ban đầu: {initial_count:,} interactions, {initial_users:,} users, {initial_items:,} items")
    
    current_df = interactions_df
    iteration = 0
    prev_count = 0
    
    while True:
        iteration += 1
        before_count = len(current_df)
        
        # Filter items: chỉ giữ items có >= 5 interactions
        item_counts = current_df.group_by("item_id").agg(pl.len().alias("count"))
        valid_items = item_counts.filter(pl.col("count") >= 5).select("item_id")
        current_df = current_df.join(valid_items, on="item_id", how="inner")
        
        # Filter users: chỉ giữ users có >= 5 interactions
        user_counts = current_df.group_by("user_id").agg(pl.len().alias("count"))
        valid_users = user_counts.filter(pl.col("count") >= 5).select("user_id")
        current_df = current_df.join(valid_users, on="user_id", how="inner")
        
        after_count = len(current_df)
        dropped = before_count - after_count
        
        print(f"  Iteration {iteration}: {before_count:,} → {after_count:,} interactions (dropped {dropped:,})")
        
        # Kiểm tra điều kiện dừng: số lượng không thay đổi
        if after_count == prev_count:
            print(f"  Đã ổn định sau {iteration} iterations")
            break
        
        prev_count = after_count
    
    final_count = len(current_df)
    final_users = current_df["user_id"].n_unique()
    final_items = current_df["item_id"].n_unique()
    
    print(f"\n  Kết quả 5-core:")
    print(f"    Interactions: {initial_count:,} → {final_count:,} ({final_count/initial_count*100:.2f}%)")
    print(f"    Users: {initial_users:,} → {final_users:,} ({final_users/initial_users*100:.2f}%)")
    print(f"    Items: {initial_items:,} → {final_items:,} ({final_items/initial_items*100:.2f}%)")
    
    return current_df


def step2_sort_by_time(interactions_df: pl.DataFrame) -> pl.DataFrame:
    """
    Step 2: Sort interactions theo user_id, sau đó theo ts.
    """
    print("\n[Step 2] Sorting interactions theo user_id và ts...")
    
    sorted_df = interactions_df.sort(["user_id", "ts"])
    
    print(f"  Đã sort {len(sorted_df):,} interactions")
    
    return sorted_df


def step3_split_train_test(interactions_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Step 3: Train/Test split (time-based).
    
    Với mỗi user_id:
    - Giữ 1 interaction cuối làm test
    - Phần còn lại làm train
    """
    print("\n[Step 3] Splitting train/test (time-based)...")
    
    # Với mỗi user, lấy interaction cuối cùng (theo ts) làm test
    # Sort đã được thực hiện ở Step 2, nên interaction cuối là row cuối cùng của mỗi user
    
    # Tạo rank cho mỗi user (1 = oldest, n = newest)
    interactions_with_rank = interactions_df.with_columns([
        pl.col("ts").rank("dense").over("user_id").alias("_rank")
    ])
    
    # Lấy max rank cho mỗi user (interaction cuối cùng)
    max_ranks = interactions_with_rank.group_by("user_id").agg(
        pl.col("_rank").max().alias("_max_rank")
    )
    
    # Join để xác định interaction cuối của mỗi user
    interactions_with_max = interactions_with_rank.join(
        max_ranks,
        on="user_id",
        how="left"
    )
    
    # Test: interaction cuối cùng (_rank == _max_rank)
    test_df = interactions_with_max.filter(
        pl.col("_rank") == pl.col("_max_rank")
    ).drop(["_rank", "_max_rank"])
    
    # Train: các interaction còn lại
    train_df = interactions_with_max.filter(
        pl.col("_rank") != pl.col("_max_rank")
    ).drop(["_rank", "_max_rank"])
    
    train_count = len(train_df)
    test_count = len(test_df)
    total_count = train_count + test_count
    train_ratio = train_count / total_count * 100 if total_count > 0 else 0
    test_ratio = test_count / total_count * 100 if total_count > 0 else 0
    
    print(f"  Train: {train_count:,} interactions ({train_ratio:.2f}%)")
    print(f"  Test: {test_count:,} interactions ({test_ratio:.2f}%)")
    print(f"  Tổng: {total_count:,} interactions")
    
    return train_df, test_df


def save_5core_data(
    interactions_5core: pl.DataFrame,
    train_df: pl.DataFrame,
    test_df: pl.DataFrame,
    output_dir: Path
):
    """
    Lưu 5-core dataset và train/test splits ra parquet files.
    
    Args:
        interactions_5core: 5-core interactions DataFrame
        train_df: Train DataFrame
        test_df: Test DataFrame
        output_dir: Thư mục output
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    interactions_path = output_dir / "interactions_5core.parquet"
    train_path = output_dir / "interactions_5core_train.parquet"
    test_path = output_dir / "interactions_5core_test.parquet"
    
    print(f"\nĐang lưu 5-core interactions: {interactions_path}")
    interactions_5core.write_parquet(str(interactions_path))
    
    print(f"Đang lưu train: {train_path}")
    train_df.write_parquet(str(train_path))
    
    print(f"Đang lưu test: {test_path}")
    test_df.write_parquet(str(test_path))
    
    print("\n[OK] Đã lưu dữ liệu 5-core thành công!")


def main():
    """Hàm chính để chạy phase 5 build 5-core."""
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
    print("PHASE 5: BUILD 5-CORE DATASET AND TRAIN/TEST SPLIT")
    print("=" * 80)
    
    # Load interactions
    interactions_all = load_interactions(project_root)
    print(f"[OK] Đã load interactions: {len(interactions_all):,} dòng")
    print(f"  Users: {interactions_all['user_id'].n_unique():,}")
    print(f"  Items: {interactions_all['item_id'].n_unique():,}")
    
    # Step 1: Build 5-core
    interactions_5core = step1_build_5core(interactions_all)
    
    # Step 2: Sort by time
    interactions_5core_sorted = step2_sort_by_time(interactions_5core)
    
    # Step 3: Split train/test
    train_df, test_df = step3_split_train_test(interactions_5core_sorted)
    
    # Lưu ra parquet
    save_5core_data(interactions_5core_sorted, train_df, test_df, data_processed_dir)
    
    # In kết quả tổng hợp
    print("\n" + "=" * 80)
    print("KẾT QUẢ TỔNG HỢP")
    print("=" * 80)
    
    print("\n[5-CORE DATASET]")
    print("-" * 80)
    print(f"Số interactions trước 5-core: {len(interactions_all):,}")
    print(f"Số interactions sau 5-core: {len(interactions_5core_sorted):,}")
    print(f"Số user unique: {interactions_5core_sorted['user_id'].n_unique():,}")
    print(f"Số item unique: {interactions_5core_sorted['item_id'].n_unique():,}")
    
    print("\n[TRAIN/TEST SPLIT]")
    print("-" * 80)
    train_count = len(train_df)
    test_count = len(test_df)
    total_count = train_count + test_count
    train_ratio = train_count / total_count * 100 if total_count > 0 else 0
    test_ratio = test_count / total_count * 100 if total_count > 0 else 0
    
    print(f"Train: {train_count:,} interactions ({train_ratio:.2f}%)")
    print(f"Test: {test_count:,} interactions ({test_ratio:.2f}%)")
    print(f"Tỷ lệ train/test: {train_ratio:.2f}% / {test_ratio:.2f}%")
    
    print("\n[SCHEMA]")
    print("-" * 80)
    print("Interactions schema:")
    print(interactions_5core_sorted.schema)
    
    # Thống kê bổ sung
    print("\n[THỐNG KÊ BỔ SUNG]")
    print("-" * 80)
    avg_interactions_per_user = train_df.group_by("user_id").agg(pl.len().alias("count"))["count"].mean()
    print(f"Trung bình interactions/user trong train: {avg_interactions_per_user:.2f}")
    
    avg_interactions_per_item = train_df.group_by("item_id").agg(pl.len().alias("count"))["count"].mean()
    print(f"Trung bình interactions/item trong train: {avg_interactions_per_item:.2f}")
    
    print("\n" + "=" * 80)
    print("[OK] PHASE 5 HOÀN TẤT: 5-core dataset và train/test split đã được xây dựng thành công!")
    print("=" * 80)
    
    return interactions_5core_sorted, train_df, test_df


if __name__ == "__main__":
    main()

