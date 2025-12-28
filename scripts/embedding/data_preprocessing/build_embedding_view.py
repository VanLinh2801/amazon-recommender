"""
Build Embedding View from Raw Metadata
=======================================
Mục tiêu: Tạo một embedding view riêng từ raw metadata Amazon All Beauty,
dùng cho content-based embedding sau này.

File này chỉ làm sạch tối thiểu và extract các cột cần thiết:
- parent_asin, title, store, main_category, details, features, description

KHÔNG làm semantic inference, phân loại, hay generate embedding.
Chỉ tạo view sạch từ raw data để phục vụ embedding pipeline sau này.

Chạy độc lập: python app/data_preprocessing/build_embedding_view.py
"""

import polars as pl
from pathlib import Path
import sys
import io

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


# ============================================================================
# CONSTANTS - Đường dẫn file
# ============================================================================

# Raw metadata file (JSONL format)
RAW_METADATA_FILE = Path("data/raw/meta_All_Beauty.jsonl")

# Output file
OUTPUT_FILE = Path("data/embedding/metadata_for_embedding.parquet")


# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def load_raw_metadata(file_path: Path) -> pl.DataFrame:
    """
    Load raw metadata từ file JSONL.
    
    Args:
        file_path: Đường dẫn đến file raw metadata
        
    Returns:
        DataFrame chứa raw metadata
    """
    print(f"Đang đọc raw metadata từ: {file_path}")
    
    if not file_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {file_path}")
    
    # Đọc JSONL file
    df = pl.read_ndjson(str(file_path))
    print(f"[OK] Đã đọc {len(df):,} dòng từ raw metadata")
    print(f"Columns: {list(df.columns)}")
    
    return df


def extract_required_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Extract chỉ các cột cần thiết cho embedding view.
    
    Các cột cần giữ:
    - parent_asin
    - title
    - store
    - main_category
    - details
    - features
    - description
    
    Args:
        df: DataFrame chứa raw metadata
        
    Returns:
        DataFrame chỉ chứa các cột cần thiết
    """
    print("\nĐang extract các cột cần thiết...")
    
    # Danh sách cột cần giữ
    required_columns = [
        'parent_asin',
        'title',
        'store',
        'main_category',
        'details',
        'features',
        'description'
    ]
    
    # Kiểm tra cột nào có sẵn
    available_columns = [col for col in required_columns if col in df.columns]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"[WARNING] Thiếu các cột: {missing_columns}")
        print(f"[INFO] Chỉ giữ các cột có sẵn: {available_columns}")
    
    # Select chỉ các cột có sẵn
    if available_columns:
        df_selected = df.select(available_columns)
    else:
        raise ValueError("Không có cột nào trong danh sách required columns!")
    
    print(f"[OK] Đã extract {len(available_columns)} cột")
    
    return df_selected


def clean_minimal(df: pl.DataFrame) -> pl.DataFrame:
    """
    Làm sạch tối thiểu:
    - Bỏ dòng thiếu parent_asin hoặc title
    - Đảm bảo title là string
    - Giữ nguyên details/features/description (kể cả rỗng)
    
    Args:
        df: DataFrame đã extract columns
        
    Returns:
        DataFrame đã được làm sạch
    """
    print("\nĐang làm sạch tối thiểu...")
    
    initial_count = len(df)
    print(f"  Số dòng ban đầu: {initial_count:,}")
    
    # Bỏ dòng thiếu parent_asin
    if 'parent_asin' in df.columns:
        before = len(df)
        df = df.filter(pl.col('parent_asin').is_not_null())
        after = len(df)
        dropped = before - after
        print(f"  Đã bỏ {dropped:,} dòng thiếu parent_asin")
    
    # Bỏ dòng thiếu title
    if 'title' in df.columns:
        before = len(df)
        df = df.filter(pl.col('title').is_not_null())
        after = len(df)
        dropped = before - after
        print(f"  Đã bỏ {dropped:,} dòng thiếu title")
    
    # Đảm bảo title là string
    if 'title' in df.columns:
        df = df.with_columns([
            pl.col('title').cast(pl.Utf8).alias('title')
        ])
        print(f"  Đã đảm bảo title là string")
    
    # Loại bỏ dòng có title rỗng sau khi convert
    if 'title' in df.columns:
        before = len(df)
        df = df.filter(pl.col('title').str.strip_chars() != '')
        after = len(df)
        dropped = before - after
        print(f"  Đã bỏ {dropped:,} dòng có title rỗng")
    
    # Giữ nguyên details/features/description (kể cả null hoặc rỗng)
    # Không cần xử lý gì thêm
    
    final_count = len(df)
    total_dropped = initial_count - final_count
    
    print(f"\n[OK] Đã làm sạch xong")
    print(f"  Số dòng còn lại: {final_count:,}")
    print(f"  Tổng số dòng đã bỏ: {total_dropped:,}")
    
    return df


def ensure_one_row_per_parent_asin(df: pl.DataFrame) -> pl.DataFrame:
    """
    Đảm bảo mỗi parent_asin chỉ có một dòng.
    Nếu có duplicate, giữ dòng đầu tiên.
    
    Args:
        df: DataFrame đã làm sạch
        
    Returns:
        DataFrame đã deduplicate
    """
    print("\nĐang đảm bảo mỗi parent_asin chỉ có một dòng...")
    
    if 'parent_asin' not in df.columns:
        print("[WARNING] Không có cột parent_asin, bỏ qua deduplication")
        return df
    
    before = len(df)
    
    # Remove duplicates, giữ dòng đầu tiên
    df = df.unique(subset=['parent_asin'], keep='first')
    
    after = len(df)
    dropped = before - after
    
    if dropped > 0:
        print(f"  Đã loại bỏ {dropped:,} duplicate parent_asin")
    else:
        print(f"  Không có duplicate parent_asin")
    
    print(f"[OK] Số dòng sau deduplication: {after:,}")
    
    return df


def save_embedding_view(df: pl.DataFrame, output_path: Path):
    """
    Lưu embedding view ra file parquet.
    
    Args:
        df: DataFrame đã xử lý
        output_path: Đường dẫn file output
    """
    print(f"\nĐang lưu embedding view: {output_path}")
    
    # Tạo thư mục nếu chưa có
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Lưu file
    df.write_parquet(str(output_path))
    
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"[OK] Đã lưu {len(df):,} dòng vào {output_path}")
    print(f"  File size: {file_size_mb:.2f} MB")
    
    # In schema
    print(f"\nSchema của file output:")
    for col, dtype in zip(df.columns, df.dtypes):
        print(f"  {col}: {dtype}")


def main():
    """
    Hàm chính để build embedding view từ raw metadata.
    """
    print("=" * 80)
    print("BUILD EMBEDDING VIEW FROM RAW METADATA")
    print("=" * 80)
    
    try:
        # Bước 1: Load raw metadata
        df = load_raw_metadata(RAW_METADATA_FILE)
        
        # Bước 2: Extract các cột cần thiết
        df = extract_required_columns(df)
        
        # Bước 3: Làm sạch tối thiểu
        df = clean_minimal(df)
        
        # Bước 4: Đảm bảo mỗi parent_asin chỉ có một dòng
        df = ensure_one_row_per_parent_asin(df)
        
        # Bước 5: Lưu embedding view
        save_embedding_view(df, OUTPUT_FILE)
        
        # Thống kê cuối cùng
        print("\n" + "=" * 80)
        print("THỐNG KÊ CUỐI CÙNG")
        print("=" * 80)
        print(f"Tổng số items: {len(df):,}")
        
        if 'parent_asin' in df.columns:
            unique_asins = df['parent_asin'].n_unique()
            print(f"Số parent_asin unique: {unique_asins:,}")
        
        if 'title' in df.columns:
            titles_with_data = df.filter(pl.col('title').is_not_null()).height
            print(f"Items có title: {titles_with_data:,}")
        
        if 'details' in df.columns:
            details_with_data = df.filter(pl.col('details').is_not_null()).height
            print(f"Items có details: {details_with_data:,}")
        
        if 'features' in df.columns:
            features_with_data = df.filter(pl.col('features').is_not_null()).height
            print(f"Items có features: {features_with_data:,}")
        
        if 'description' in df.columns:
            description_with_data = df.filter(pl.col('description').is_not_null()).height
            print(f"Items có description: {description_with_data:,}")
        
        print("\n" + "=" * 80)
        print("[OK] HOÀN TẤT: Embedding view đã được tạo thành công!")
        print("=" * 80)
        print(f"\nFile output: {OUTPUT_FILE}")
        print("File này có thể được sử dụng cho content-based embedding pipeline.")
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("[ERROR] LỖI KHI XỬ LÝ")
        print("=" * 80)
        print(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()

