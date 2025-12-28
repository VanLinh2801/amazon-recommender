"""
Filter Embedding Eligible Items
================================
Mục tiêu: Xác định item nào đủ điều kiện đưa vào embedding pipeline.

Bối cảnh:
- Không phải mọi item đều phù hợp cho content-based embedding
- Cần lọc các item có title đủ semantic signal
- Title đã được clean trước đó

Logic:
- Title phải có ít nhất 3 token (đủ thông tin)
- Title phải chứa ít nhất 1 keyword semantic (có ý nghĩa rõ ràng)
- Loại bỏ các item chỉ có từ chung chung như "hair" mà không có object cụ thể

Input: data/embedding/metadata_for_embedding_clean.parquet
Output: data/embedding/metadata_for_embedding_final.parquet (thêm cột embedding_eligible)

Chạy độc lập: python app/embedding/data_preprocessing/filter_embedding_eligible.py
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
# CONSTANTS
# ============================================================================

INPUT_FILE = Path("data/embedding/metadata_for_embedding_clean.parquet")
OUTPUT_FILE = Path("data/embedding/metadata_for_embedding_final.parquet")

# Danh sách keyword semantic - match theo token
EMBED_KEYWORDS = {
    # Eye / Makeup
    'eyelash', 'eyelashes', 'lash', 'lashes', 'false eyelash', 'false eyelashes',
    'eyeliner', 'mascara', 'eyebrow', 'brow', 'eyebrows', 'brows',
    'eyeshadow', 'eye shadow', 'eye makeup', 'eye liner', 'eye shadow',
    'shadow', 'shadow paint',
    
    # Lip
    'lipstick', 'lip stick', 'lip gloss', 'lipgloss', 'lip balm', 
    'lipbalm', 'lip tint', 'liptint',
    
    # Face / Makeup
    'foundation', 'concealer', 'blush', 'bronzer', 'highlighter', 
    'makeup', 'make up', 'makeup kit', 'makeup bag', 'face paint', 'facepaint',
    'paint',  # shadow paint, face paint
    
    # Skin Care
    'cream', 'gel', 'serum', 'lotion', 'moisturizer', 'moisturiser',
    'cleanser', 'toner', 'mask', 'face mask', 'skincare', 'skin care',
    'sunscreen', 'sun screen', 'sunscreen lotion',
    
    # Hair - Phải có object cụ thể
    'wig', 'wigs', 'hair extension', 'hair extensions', 'extension',
    'extensions', 'scrunchie', 'scrunchies', 'headband', 'hairband',
    'barrette', 'hair clip', 'hair clips', 'hair brush', 'hairbrush',
    'hair comb', 'hairspray', 'hair spray', 'hair gel', 'hair mousse',
    'hair weave', 'weave', 'lace front', 'lacefront', 'hair tie',
    'hair ties', 'ponytail holder', 'ponytail holders', 'ponytail',
    
    # Nail
    'nail', 'nails', 'nail polish', 'nailpolish', 'gel nail', 'gelnail',
    'acrylic nail', 'acrylicnail', 'fake nail', 'fakenail',
    'press on nail', 'presson nail', 'manicure', 'pedicure',
    
    # Tools / Accessories
    'brush', 'brushes', 'comb', 'combs', 'applicator', 'applicators',
    'makeup sponge', 'makeupsponge', 'beauty blender', 'beautyblender',
    'tweezer', 'tweezers', 'eyelash glue', 'eyelashglue',
    
    # Body / Temporary
    'temporary tattoo', 'temporarytattoo', 'body paint', 'bodypaint',
    'body makeup', 'bodymakeup', 'tattoo', 'tattoos',  # face tattoo, arm tattoo
    'face tattoo', 'arm tattoo', 'body tattoo',
    
    # Fragrance
    'perfume', 'cologne', 'fragrance', 'body spray', 'bodyspray',
    
    # Personal Care
    'deodorant', 'antiperspirant', 'anti-perspirant', 'shampoo',
    'conditioner', 'soap', 'body wash', 'bodywash',
    
    # Fashion Accessories
    # Lưu ý: 'scarf' đã được loại bỏ vì có thể là face covering
    'hat', 'cap', 'gloves', 'sunglasses'
}

# Từ chung chung cần loại bỏ nếu không có object cụ thể
GENERIC_WORDS = {
    'hair',  # Chỉ chấp nhận nếu có: wig, extension, clip, brush, etc.
    'makeup',  # Chỉ chấp nhận nếu có: eye makeup, face makeup, body makeup
    'beauty',  # Quá chung chung
    'cosmetic',  # Quá chung chung
    'product',  # Quá chung chung
    'accessory',  # Quá chung chung
    'item'  # Quá chung chung
}

# Từ cụ thể đi kèm với generic words để chấp nhận
SPECIFIC_HAIR_OBJECTS = {
    'wig', 'wigs', 'extension', 'extensions', 'clip', 'clips', 'brush',
    'comb', 'scrunchie', 'scrunchies', 'headband', 'hairband',
    'barrette', 'hairspray', 'hair spray', 'gel', 'mousse', 'weave',
    'lace front', 'lacefront', 'hair tie', 'hair ties',
    'ponytail holder', 'ponytail holders', 'ponytail'
}

SPECIFIC_MAKEUP_OBJECTS = {
    'eyeliner', 'mascara', 'eyeshadow', 'lipstick', 'foundation',
    'concealer', 'blush', 'bronzer', 'highlighter', 'eye makeup',
    'face makeup', 'body makeup', 'makeup kit', 'shadow paint',
    'face paint', 'paint'
}

# Negative keywords - loại trực tiếp (face mask, face covering, etc.)
# Rule này cần thiết để loại bỏ false positive: các sản phẩm y tế/phòng hộ
# (face mask, gaiter, bandana) không phải makeup/beauty products
NEGATIVE_KEYWORDS = {
    'mask',  # face mask, surgical mask, etc.
    'face mask',  # protective face mask
    'gaiter',  # neck gaiter
    'bandana',  # face covering bandana
    'scarf',  # face covering scarf (loại bỏ khỏi EMBED_KEYWORDS)
    'breathing cover',  # breathing face cover
    'face covering',  # generic face covering
    'face cover',  # variant
    'surgical mask',  # medical mask
    'n95',  # N95 mask
    'respirator'  # respirator mask
}

# Makeup/skincare-related words để disambiguate "face"
# Nếu title có "face" nhưng không có các từ này → có thể là face mask/covering
# Bao gồm cả makeup và skincare products hợp lệ
FACE_MAKEUP_KEYWORDS = {
    'makeup', 'make up',
    'paint',  # face paint
    'foundation',  # face foundation
    'concealer',  # face concealer
    # Skincare products hợp lệ
    'wash',  # face wash
    'cleanser',  # face cleanser
    'serum',  # face serum
    'cream',  # face cream
    'lotion',  # face lotion
    'moisturizer', 'moisturiser',  # face moisturizer
    'toner',  # face toner
    'mask',  # face mask (skincare, nhưng cần cẩn thận với negative keywords)
    'scrub',  # face scrub
    'exfoliant', 'exfoliating',  # face exfoliant
    'sunscreen', 'sun screen'  # face sunscreen
}


# ============================================================================
# FILTERING LOGIC
# ============================================================================

def has_minimum_tokens(title: str, min_tokens: int = 3) -> bool:
    """
    Kiểm tra title có đủ số token tối thiểu không.
    
    Args:
        title: Title đã clean
        min_tokens: Số token tối thiểu (mặc định: 3)
        
    Returns:
        True nếu có đủ token, False nếu không
    """
    if not title or not isinstance(title, str):
        return False
    
    tokens = title.strip().split()
    return len(tokens) >= min_tokens


def contains_semantic_keyword(title: str) -> bool:
    """
    Kiểm tra title có chứa ít nhất 1 keyword semantic không.
    Match theo token, không match substring.
    
    Args:
        title: Title đã clean (lowercase)
        
    Returns:
        True nếu có keyword semantic, False nếu không
    """
    if not title or not isinstance(title, str):
        return False
    
    title_lower = title.lower()
    tokens = set(title_lower.split())
    
    # Kiểm tra từng keyword
    for keyword in EMBED_KEYWORDS:
        keyword_lower = keyword.lower()
        
        # Match exact token
        if keyword_lower in tokens:
            return True
        
        # Match multi-word keyword (như "hair extension")
        if ' ' in keyword_lower:
            if keyword_lower in title_lower:
                return True
    
    return False


def contains_negative_keyword(title: str) -> bool:
    """
    Kiểm tra title có chứa negative keyword không.
    Negative keywords là các từ chỉ sản phẩm y tế/phòng hộ (face mask, gaiter, etc.)
    không phải makeup/beauty products.
    
    Match theo token hoặc cụm từ, không match substring bừa.
    
    Args:
        title: Title đã clean (lowercase)
        
    Returns:
        True nếu có negative keyword, False nếu không
    """
    if not title or not isinstance(title, str):
        return False
    
    title_lower = title.lower()
    tokens = set(title_lower.split())
    
    # Kiểm tra từng negative keyword
    for keyword in NEGATIVE_KEYWORDS:
        keyword_lower = keyword.lower()
        
        # Match exact token
        if keyword_lower in tokens:
            return True
        
        # Match multi-word keyword (như "face mask", "face covering")
        if ' ' in keyword_lower:
            if keyword_lower in title_lower:
                return True
    
    return False


def has_face_without_makeup_context(title: str) -> bool:
    """
    Kiểm tra title có từ "face" nhưng không có context makeup không.
    
    Rule: Nếu title chứa "face" nhưng không chứa makeup-related words
    (makeup, paint, foundation, concealer) → có thể là face mask/covering
    không phải makeup product.
    
    Args:
        title: Title đã clean (lowercase)
        
    Returns:
        True nếu có "face" nhưng không có makeup context, False nếu không
    """
    if not title or not isinstance(title, str):
        return False
    
    title_lower = title.lower()
    tokens = set(title_lower.split())
    
    # Kiểm tra có từ "face" không
    if 'face' not in tokens and 'face' not in title_lower:
        return False
    
    # Kiểm tra có makeup-related words không
    has_makeup_context = False
    for keyword in FACE_MAKEUP_KEYWORDS:
        keyword_lower = keyword.lower()
        
        # Match exact token
        if keyword_lower in tokens:
            has_makeup_context = True
            break
        
        # Match multi-word (như "make up")
        if ' ' in keyword_lower:
            if keyword_lower in title_lower:
                has_makeup_context = True
                break
    
    # Nếu có "face" nhưng không có makeup context → có thể là face mask
    return not has_makeup_context


def has_specific_object(title: str) -> bool:
    """
    Kiểm tra title có object cụ thể khi có từ chung chung.
    Ví dụ: "hair" chỉ chấp nhận nếu có "wig", "extension", "clip", etc.
    
    Args:
        title: Title đã clean (lowercase)
        
    Returns:
        True nếu có object cụ thể hoặc không có từ chung chung, False nếu không
    """
    if not title or not isinstance(title, str):
        return False
    
    title_lower = title.lower()
    tokens = set(title_lower.split())
    
    # Kiểm tra "hair" - phải có object cụ thể
    if 'hair' in tokens:
        # Kiểm tra có object cụ thể không
        has_specific = False
        for obj in SPECIFIC_HAIR_OBJECTS:
            obj_lower = obj.lower()
            if obj_lower in tokens or obj_lower in title_lower:
                has_specific = True
                break
        
        if not has_specific:
            return False
    
    # Kiểm tra "makeup" - phải có object cụ thể hoặc context rõ ràng
    if 'makeup' in tokens or 'make up' in title_lower:
        has_specific = False
        for obj in SPECIFIC_MAKEUP_OBJECTS:
            obj_lower = obj.lower()
            if obj_lower in tokens or obj_lower in title_lower:
                has_specific = True
                break
        
        # Chấp nhận nếu có context như "eye makeup", "face makeup"
        if not has_specific:
            if 'eye makeup' in title_lower or 'face makeup' in title_lower or 'body makeup' in title_lower:
                has_specific = True
        
        if not has_specific:
            return False
    
    # Kiểm tra các từ chung chung khác
    for generic in ['beauty', 'cosmetic', 'product', 'accessory', 'item']:
        if generic in tokens:
            # Nếu có từ chung chung, phải có keyword cụ thể
            if not contains_semantic_keyword(title):
                return False
    
    return True


def is_embedding_eligible(title: str) -> bool:
    """
    Xác định item có đủ điều kiện cho embedding không.
    
    Điều kiện:
    1. Title có ít nhất 3 token
    2. Title chứa ít nhất 1 keyword semantic
    3. Title có object cụ thể (nếu có từ chung chung)
    4. Title KHÔNG chứa negative keywords (face mask, gaiter, etc.)
    5. Title có "face" phải có makeup context (makeup, paint, foundation, concealer)
    
    Args:
        title: Title đã clean
        
    Returns:
        True nếu eligible, False nếu không
    """
    if not title or not isinstance(title, str):
        return False
    
    # Rule 0: Loại bỏ negative keywords (face mask, gaiter, etc.)
    # Rule này cần thiết để loại bỏ false positive: các sản phẩm y tế/phòng hộ
    # không phải makeup/beauty products
    if contains_negative_keyword(title):
        return False
    
    # Rule 0.5: Ràng buộc keyword "face"
    # Nếu có "face" nhưng không có makeup context → có thể là face mask/covering
    if has_face_without_makeup_context(title):
        return False
    
    # Điều kiện 1: Có ít nhất 3 token
    if not has_minimum_tokens(title, min_tokens=3):
        return False
    
    # Điều kiện 2: Có keyword semantic
    if not contains_semantic_keyword(title):
        return False
    
    # Điều kiện 3: Có object cụ thể (nếu có từ chung chung)
    if not has_specific_object(title):
        return False
    
    return True


# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def filter_embedding_eligible(input_path: Path, output_path: Path):
    """
    Xử lý file metadata để thêm cột embedding_eligible.
    
    Args:
        input_path: Đường dẫn file input
        output_path: Đường dẫn file output
    """
    print("=" * 80)
    print("FILTER EMBEDDING ELIGIBLE ITEMS")
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
    print(f"\nThống kê ban đầu:")
    print(f"  Tổng số items: {len(df):,}")
    
    # Thêm cột embedding_eligible
    print(f"\nĐang xác định embedding_eligible cho từng item...")
    
    # Apply function để xác định eligible
    df = df.with_columns([
        pl.col('title').map_elements(
            lambda x: is_embedding_eligible(str(x)) if x else False,
            return_dtype=pl.Boolean
        ).alias('embedding_eligible')
    ])
    
    # Thống kê kết quả
    eligible_count = df.filter(pl.col('embedding_eligible') == True).height
    not_eligible_count = df.filter(pl.col('embedding_eligible') == False).height
    
    print(f"\n[OK] Đã xác định embedding_eligible cho tất cả items")
    print(f"\nThống kê kết quả:")
    print(f"  Items eligible: {eligible_count:,} ({eligible_count/len(df)*100:.1f}%)")
    print(f"  Items không eligible: {not_eligible_count:,} ({not_eligible_count/len(df)*100:.1f}%)")
    
    # Phân tích lý do không eligible (sample)
    print(f"\nPhân tích items không eligible (sample 10 items):")
    not_eligible_df = df.filter(pl.col('embedding_eligible') == False).head(10)
    
    for i, row in enumerate(not_eligible_df.to_dicts(), 1):
        title = row.get('title', '')
        tokens = title.split() if title else []
        has_keyword = contains_semantic_keyword(title) if title else False
        has_specific = has_specific_object(title) if title else False
        
        reasons = []
        if len(tokens) < 3:
            reasons.append(f"< 3 tokens ({len(tokens)})")
        if not has_keyword:
            reasons.append("no semantic keyword")
        if not has_specific:
            reasons.append("no specific object")
        
        print(f"\n  [{i}] {row.get('parent_asin', 'N/A')}")
        print(f"      Title: {title[:80]}...")
        print(f"      Lý do: {', '.join(reasons) if reasons else 'unknown'}")
    
    # Lưu file output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nĐang lưu file: {output_path}")
    df.write_parquet(str(output_path))
    
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"[OK] Đã lưu {len(df):,} items vào {output_path}")
    print(f"  File size: {file_size_mb:.2f} MB")
    print(f"  Cột mới: embedding_eligible (Boolean)")
    
    # In một vài mẫu eligible và không eligible
    print("\n" + "=" * 80)
    print("MẪU ITEMS ELIGIBLE (5 items đầu tiên):")
    print("=" * 80)
    eligible_sample = df.filter(pl.col('embedding_eligible') == True).head(5)
    for i, row in enumerate(eligible_sample.to_dicts(), 1):
        print(f"\n[Item {i}] {row.get('parent_asin', 'N/A')}")
        print(f"  Title: {row.get('title', '')[:100]}")
        print(f"  Tokens: {len(row.get('title', '').split())}")
        print(f"  Eligible: {row.get('embedding_eligible', False)}")
    
    print("\n" + "=" * 80)
    print("MẪU ITEMS KHÔNG ELIGIBLE (5 items đầu tiên):")
    print("=" * 80)
    not_eligible_sample = df.filter(pl.col('embedding_eligible') == False).head(5)
    for i, row in enumerate(not_eligible_sample.to_dicts(), 1):
        print(f"\n[Item {i}] {row.get('parent_asin', 'N/A')}")
        print(f"  Title: {row.get('title', '')[:100]}")
        print(f"  Tokens: {len(row.get('title', '').split())}")
        print(f"  Eligible: {row.get('embedding_eligible', False)}")
    
    print("\n" + "=" * 80)
    print("[OK] HOÀN TẤT: File metadata_for_embedding_final.parquet đã được tạo thành công!")
    print("=" * 80)
    print(f"\nLưu ý:")
    print(f"  - Tất cả items đều được giữ lại trong file")
    print(f"  - Chỉ items có embedding_eligible = True mới nên đưa vào embedding pipeline")
    print(f"  - Items có embedding_eligible = False vẫn có thể dùng cho mục đích khác")


def main():
    """
    Hàm chính để chạy filter embedding eligible.
    """
    try:
        filter_embedding_eligible(INPUT_FILE, OUTPUT_FILE)
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

