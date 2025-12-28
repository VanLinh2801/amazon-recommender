"""
Clean Embedding Title
=====================
Mục tiêu: Làm sạch title cho embedding, giữ semantic quan trọng, loại bỏ noise marketing.

Bối cảnh:
- Dữ liệu Amazon All Beauty
- Title thường rất dài, nhiều keyword spam
- Title là semantic signal chính cho embedding

Input: data/embedding/metadata_for_embedding.parquet
Output: data/embedding/metadata_for_embedding_clean.parquet

Chạy độc lập: python app/embedding/data_preprocessing/clean_embedding_title.py
"""

import polars as pl
from pathlib import Path
import sys
import io
import re

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


# ============================================================================
# CONSTANTS
# ============================================================================

INPUT_FILE = Path("data/embedding/metadata_for_embedding.parquet")
OUTPUT_FILE = Path("data/embedding/metadata_for_embedding_clean.parquet")

# Tokens cần loại bỏ - Số đo
MEASUREMENT_TOKENS = {
    'inch', 'inches', 'in', 'oz', 'ounce', 'ounces', 'ml', 'milliliter', 'milliliters',
    'l', 'liter', 'liters', 'g', 'gram', 'grams', 'kg', 'kilogram', 'kilograms',
    'lb', 'pound', 'pounds', 'cm', 'centimeter', 'centimeters', 'm', 'meter', 'meters',
    'mm', 'millimeter', 'millimeters', '%', 'percent', 'pack', 'packs', 'count',
    'pcs', 'pieces', 'piece', 'unit', 'units', 'size', 'sizes', 'dimension', 'dimensions'
}

# Tokens cần loại bỏ - Marketing
MARKETING_TOKENS = {
    'new', 'best', 'gift', 'perfect', 'trendy', 'hot', 'popular', 'bestseller',
    'bestselling', 'top', 'premium', 'luxury', 'exclusive', 'limited', 'special',
    'amazing', 'wonderful', 'great', 'excellent', 'super', 'ultra', 'pro',
    'professional', 'advanced', 'improved', 'enhanced', 'upgraded'
}

# Tokens cần loại bỏ - Demographic
DEMOGRAPHIC_TOKENS = {
    'for women', 'for men', 'for girls', 'for boys', 'for kids', 'for children',
    'women', 'men', 'girls', 'boys', 'unisex', 'adult', 'adults'
}

# Soft cap: chỉ cắt khi > 35 tokens
MAX_TOKENS = 35


# ============================================================================
# CLEAN TITLE FUNCTION
# ============================================================================

def clean_title(title: str) -> str:
    """
    Làm sạch title: lowercase, bỏ ký tự đặc biệt, loại bỏ noise tokens.
    
    Args:
        title: Title gốc của sản phẩm
        
    Returns:
        Title đã được làm sạch
    """
    if not title or not isinstance(title, str):
        return ""
    
    # Bước 1: Lowercase
    cleaned = title.lower()
    
    # Bước 2: Bỏ ký tự đặc biệt (giữ chữ, số, khoảng trắng)
    # Giữ dấu gạch ngang và dấu cách
    cleaned = re.sub(r'[^\w\s-]', ' ', cleaned)
    
    # Bước 3: Chuẩn hóa khoảng trắng
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = cleaned.strip()
    
    # Bước 4: Tách thành tokens
    tokens = cleaned.split()
    
    if not tokens:
        return ""
    
    # Bước 5: Loại bỏ các token không cần thiết
    filtered_tokens = []
    seen_tokens = set()  # Để loại bỏ trùng lặp
    
    for token in tokens:
        token_lower = token.lower().strip()
        
        # Bỏ qua token rỗng
        if not token_lower:
            continue
        
        # Bỏ qua số thuần (có thể là size, model number)
        if token_lower.isdigit() and len(token_lower) > 2:
            continue
        
        # Bỏ qua measurement tokens
        if token_lower in MEASUREMENT_TOKENS:
            continue
        
        # Bỏ qua marketing tokens
        if token_lower in MARKETING_TOKENS:
            continue
        
        # Bỏ qua demographic tokens (kiểm tra cả cụm từ)
        is_demographic = False
        for demo in DEMOGRAPHIC_TOKENS:
            if demo in token_lower or token_lower in demo:
                is_demographic = True
                break
        if is_demographic:
            continue
        
        # Loại bỏ token trùng lặp
        if token_lower in seen_tokens:
            continue
        
        # Giữ lại token
        filtered_tokens.append(token)
        seen_tokens.add(token_lower)
    
    # Bước 6: Soft cap - chỉ cắt nếu > MAX_TOKENS
    if len(filtered_tokens) > MAX_TOKENS:
        filtered_tokens = filtered_tokens[:MAX_TOKENS]
    
    # Bước 7: Kết hợp lại thành string
    cleaned_title = ' '.join(filtered_tokens)
    
    return cleaned_title.strip()


def process_metadata_file(input_path: Path, output_path: Path):
    """
    Xử lý file metadata để làm sạch title.
    
    Args:
        input_path: Đường dẫn file input
        output_path: Đường dẫn file output
    """
    print("=" * 80)
    print("CLEAN EMBEDDING TITLE")
    print("=" * 80)
    
    # Đọc file input
    print(f"\nĐang đọc file: {input_path}")
    if not input_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {input_path}")
    
    df = pl.read_parquet(str(input_path))
    print(f"[OK] Đã đọc {len(df):,} items")
    print(f"Columns: {list(df.columns)}")
    
    # Kiểm tra có cột title không
    if 'title' not in df.columns:
        raise ValueError("File phải có cột 'title'")
    
    # Thống kê ban đầu
    print(f"\nThống kê TRƯỚC khi làm sạch:")
    title_lengths = df['title'].str.len_chars()
    print(f"  Độ dài trung bình: {title_lengths.mean():.1f} ký tự")
    print(f"  Độ dài min: {title_lengths.min()} ký tự")
    print(f"  Độ dài max: {title_lengths.max()} ký tự")
    
    # Đếm số token trung bình (ước tính)
    sample_titles = df['title'].head(100).to_list()
    token_counts = [len(str(t).split()) for t in sample_titles if t]
    if token_counts:
        print(f"  Số token trung bình (ước tính): {sum(token_counts)/len(token_counts):.1f}")
    
    # Làm sạch title
    print(f"\nĐang làm sạch title cho {len(df):,} items...")
    
    # Apply clean_title function
    df = df.with_columns([
        pl.col('title').map_elements(
            lambda x: clean_title(str(x)) if x else "",
            return_dtype=pl.Utf8
        ).alias('title_clean')
    ])
    
    # Thay thế cột title bằng title_clean
    df = df.drop('title').rename({'title_clean': 'title'})
    
    # Loại bỏ các dòng có title rỗng sau khi clean
    before = len(df)
    df = df.filter(pl.col('title').str.strip_chars() != '')
    after = len(df)
    dropped = before - after
    
    if dropped > 0:
        print(f"  Đã loại bỏ {dropped:,} items có title rỗng sau khi clean")
    
    # Thống kê sau khi làm sạch
    print(f"\nThống kê SAU khi làm sạch:")
    title_lengths_after = df['title'].str.len_chars()
    print(f"  Độ dài trung bình: {title_lengths_after.mean():.1f} ký tự")
    print(f"  Độ dài min: {title_lengths_after.min()} ký tự")
    print(f"  Độ dài max: {title_lengths_after.max()} ký tự")
    
    # Đếm số token sau khi clean
    sample_titles_after = df['title'].head(100).to_list()
    token_counts_after = [len(str(t).split()) for t in sample_titles_after if t]
    if token_counts_after:
        avg_tokens = sum(token_counts_after) / len(token_counts_after)
        max_tokens = max(token_counts_after)
        print(f"  Số token trung bình: {avg_tokens:.1f}")
        print(f"  Số token tối đa: {max_tokens}")
        print(f"  Items có > {MAX_TOKENS} tokens: {sum(1 for tc in token_counts_after if tc > MAX_TOKENS)}")
    
    # Lưu file output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nĐang lưu file: {output_path}")
    df.write_parquet(str(output_path))
    
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"[OK] Đã lưu {len(df):,} items vào {output_path}")
    print(f"  File size: {file_size_mb:.2f} MB")
    
    # In một vài mẫu để so sánh
    print("\n" + "=" * 80)
    print("MẪU SO SÁNH (5 items đầu tiên):")
    print("=" * 80)
    
    # Đọc lại file gốc để so sánh
    df_original = pl.read_parquet(str(input_path))
    
    for i in range(min(5, len(df))):
        original_title = df_original['title'][i] if i < len(df_original) else ""
        cleaned_title = df['title'][i]
        
        print(f"\n[Item {i+1}]")
        print(f"TRƯỚC ({len(str(original_title))} ký tự, {len(str(original_title).split())} tokens):")
        print(f"  {original_title}")
        print(f"SAU ({len(cleaned_title)} ký tự, {len(cleaned_title.split())} tokens):")
        print(f"  {cleaned_title}")
    
    print("\n" + "=" * 80)
    print("[OK] HOÀN TẤT: File metadata_for_embedding_clean.parquet đã được tạo thành công!")
    print("=" * 80)


def main():
    """
    Hàm chính để chạy clean embedding title.
    """
    try:
        process_metadata_file(INPUT_FILE, OUTPUT_FILE)
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

