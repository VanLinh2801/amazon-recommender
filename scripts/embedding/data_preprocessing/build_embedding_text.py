"""
Build Embedding Text v2
=======================
Xây dựng embedding_text ổn định cho content-based recommendation,
chịu được trường hợp product_type = default, details rỗng, metadata thiếu.

Input: data/embedding/semantic_attributes.parquet
Output: data/embedding/embedding_text.parquet
"""

import polars as pl
from pathlib import Path
import sys
import io
from typing import Dict, List


# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


# ============================================================================
# PRODUCT TYPE DESCRIPTION MAPPING
# ============================================================================

PRODUCT_TYPE_DESCRIPTIONS = {
    'beard straightener': 'Heated beard straightener for hair styling.',
    'hair straightener': 'Hair straightener for styling.',
    'heated hair brush': 'Heated hair brush for styling.',
    'hair dryer': 'Hair dryer for styling.',
    'hair curler': 'Hair curler for styling.',
    'wig': 'Wig for hair styling.',
    'lace front wig': 'Lace front wig for natural hairline.',
    'hair extension': 'Hair extension for adding length and volume.',
    'tape-in hair extension': 'Tape-in hair extension.',
    'clip-in hair extension': 'Clip-in hair extension.',
    'scrunchie': 'Scrunchie hair accessory.',
    'headband': 'Headband hair accessory.',
    'hair clip': 'Hair clip accessory.',
    'hair claw clip': 'Hair claw clip accessory.',
    'hair barrette': 'Hair barrette accessory.',
    'foundation': 'Foundation makeup product.',
    'concealer': 'Concealer makeup product.',
    'lipstick': 'Lipstick makeup product.',
    'lip gloss': 'Lip gloss makeup product.',
    'lip balm': 'Lip balm for lip care.',
    'eyeliner': 'Eyeliner makeup product.',
    'automatic eyeliner': 'Automatic eyeliner for precise application.',
    'eyeliner pencil': 'Eyeliner pencil for eye makeup.',
    'mascara': 'Mascara for eyelash enhancement.',
    'eyeshadow': 'Eyeshadow for eye makeup.',
    'eyebrow pencil': 'Eyebrow pencil for eyebrow definition.',
    'eyebrow product': 'Eyebrow makeup product.',
    'face paint': 'Face paint for creative makeup.',
    'nail polish': 'Nail polish for nail decoration.',
    'gel nail': 'Gel nail polish.',
    'acrylic nail': 'Acrylic nail product.',
    'fake nail': 'Fake nail product.',
    'cream': 'Cream skincare product.',
    'gel': 'Gel skincare product.',
    'serum': 'Serum skincare product.',
    'face serum': 'Face serum for skincare.',
    'lotion': 'Lotion skincare product.',
    'cleanser': 'Cleanser for skincare.',
    'face cleanser': 'Face cleanser for skincare.',
    'toner': 'Toner for skincare.',
    'mask': 'Face mask for skincare.',
    'face mask': 'Face mask for skincare.',
    'hair mask': 'Hair mask for hair care.',
    'moisturizer': 'Moisturizer for skincare.',
    'shampoo': 'Shampoo for hair care.',
    'conditioner': 'Conditioner for hair care.',
    'hair treatment': 'Hair treatment product.',
}


def get_product_type_description(product_type: str) -> str:
    """
    Lấy câu mô tả ngắn cho product_type.
    
    Args:
        product_type: Product type string
        
    Returns:
        Câu mô tả hoặc empty string nếu không có mapping
    """
    if not product_type or product_type == 'default':
        return ""
    
    return PRODUCT_TYPE_DESCRIPTIONS.get(product_type, f"{product_type.capitalize()} product.")


# ============================================================================
# ATTRIBUTES TO SENTENCE
# ============================================================================

def attributes_to_sentence(attributes: Dict) -> str:
    """
    Chuyển attributes dict thành 1 câu ngắn.
    Không liệt kê dài, chỉ lấy các attributes quan trọng nhất.
    
    Args:
        attributes: Dict các attributes (chỉ chứa các key có giá trị)
        
    Returns:
        Câu mô tả attributes hoặc empty string
    """
    if not attributes or not isinstance(attributes, dict):
        return ""
    
    # Lọc bỏ None values
    valid_attrs = {k: v for k, v in attributes.items() if v is not None and str(v).strip()}
    
    if not valid_attrs:
        return ""
    
    # Ưu tiên các attributes quan trọng
    priority_fields = ['Item Form', 'Material', 'Finish Type', 'Color', 'Coverage', 'Skin Type']
    
    # Lấy tối đa 3 attributes quan trọng nhất
    selected_attrs = []
    for field in priority_fields:
        if field in valid_attrs:
            selected_attrs.append(valid_attrs[field])
            if len(selected_attrs) >= 3:
                break
    
    # Nếu chưa đủ, lấy thêm từ các field khác
    if len(selected_attrs) < 3:
        for field, value in valid_attrs.items():
            if field not in priority_fields:
                selected_attrs.append(value)
                if len(selected_attrs) >= 3:
                    break
    
    if not selected_attrs:
        return ""
    
    # Tạo câu mô tả
    if len(selected_attrs) == 1:
        return f"{selected_attrs[0].capitalize()} product."
    elif len(selected_attrs) == 2:
        return f"{selected_attrs[0].capitalize()} {selected_attrs[1].lower()} product."
    else:
        return f"{selected_attrs[0].capitalize()} {selected_attrs[1].lower()} with {selected_attrs[2].lower()}."


# ============================================================================
# BUILD EMBEDDING TEXT
# ============================================================================

def build_embedding_text(
    clean_title: str,
    product_type: str,
    attributes: Dict,
    details_has_semantic: bool,
    semantic_detail_count: int,
    usage_features: List[str],
    description_fallback: str
) -> str:
    """
    Xây dựng embedding_text theo quy tắc:
    1. Luôn bắt đầu bằng clean_title (neo semantic chính)
    2. Nếu product_type != "default": thêm 1 câu mô tả product_type
    3. Nếu attributes có semantic (>= 2 key): chuyển thành 1 câu ngắn
    4. Nếu usage_features không rỗng: thêm 1 câu từ usage_features
    5. Nếu không có usage_features và attributes semantic thấp: dùng description_fallback
    
    Args:
        clean_title: Clean title của sản phẩm
        product_type: Product type
        attributes: Dict các attributes
        details_has_semantic: True nếu attributes có >= 2 key có giá trị
        semantic_detail_count: Số key có giá trị trong attributes
        usage_features: List các usage features
        description_fallback: Description fallback string
        
    Returns:
        Embedding text string (không bao giờ rỗng)
    """
    parts = []
    
    # 1. Luôn bắt đầu bằng clean_title
    # Title luôn đứng đầu vì nó là neo semantic chính, chứa thông tin quan trọng nhất
    # và giúp embedding model hiểu ngữ cảnh ngay từ đầu
    if clean_title and str(clean_title).strip():
        parts.append(str(clean_title).strip())
    else:
        # Fallback: nếu không có title, dùng "Product"
        parts.append("Product")
    
    # 2. Nếu product_type != "default": thêm 1 câu mô tả product_type
    if product_type and product_type != "default":
        product_desc = get_product_type_description(product_type)
        if product_desc:
            parts.append(product_desc)
    
    # 3. Nếu attributes có semantic (>= 2 key): chuyển thành 1 câu ngắn
    if details_has_semantic and semantic_detail_count >= 2:
        attr_sentence = attributes_to_sentence(attributes)
        if attr_sentence:
            parts.append(attr_sentence)
    
    # 4. Nếu usage_features không rỗng: thêm 1 câu từ usage_features
    if usage_features and isinstance(usage_features, list) and len(usage_features) > 0:
        # Lấy feature đầu tiên và tạo câu
        first_feature = str(usage_features[0]).strip()
        if first_feature:
            # Đảm bảo câu có dấu chấm
            if not first_feature.endswith('.'):
                first_feature += "."
            parts.append(first_feature)
    
    # 5. Nếu không có usage_features và attributes semantic thấp: dùng description_fallback
    elif not usage_features or (isinstance(usage_features, list) and len(usage_features) == 0):
        if not details_has_semantic or semantic_detail_count < 2:
            if description_fallback and str(description_fallback).strip():
                desc = str(description_fallback).strip()
                # Đảm bảo câu có dấu chấm
                if not desc.endswith('.'):
                    desc += "."
                parts.append(desc)
    
    # Kết hợp các phần thành embedding_text
    embedding_text = " ".join(parts)
    
    # Đảm bảo không rỗng
    if not embedding_text or not embedding_text.strip():
        embedding_text = parts[0] if parts else "Product"
    
    return embedding_text.strip()


# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def process_embedding_text(
    input_file: Path,
    output_file: Path
):
    """
    Xử lý và build embedding_text từ semantic_attributes.
    """
    print("=" * 80)
    print("BUILD EMBEDDING TEXT v2")
    print("=" * 80)
    
    # Đọc file input
    print(f"\nĐang đọc file: {input_file}")
    if not input_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {input_file}")
    
    df = pl.read_parquet(str(input_file))
    print(f"[OK] Đã đọc {len(df):,} items")
    
    if len(df) == 0:
        print("[WARNING] Không có items nào để xử lý!")
        return
    
    # Xử lý từng item
    print(f"\nĐang build embedding_text...")
    
    rows = df.to_dicts()
    results = []
    
    for i, row in enumerate(rows):
        if (i + 1) % 10000 == 0:
            print(f"  Đã xử lý: {i + 1:,}/{len(rows):,} items")
        
        parent_asin = row.get('parent_asin')
        if not parent_asin:
            continue
        
        # Lấy các trường cần thiết
        clean_title = row.get('clean_title', '')
        product_type = row.get('product_type', 'default')
        attributes = row.get('attributes', {})
        details_has_semantic = row.get('details_has_semantic', False)
        semantic_detail_count = row.get('semantic_detail_count', 0)
        usage_features = row.get('usage_features', [])
        description_fallback = row.get('description_fallback', '')
        
        # Chuyển attributes từ Polars Struct sang dict nếu cần
        if attributes and not isinstance(attributes, dict):
            try:
                if hasattr(attributes, 'to_dict'):
                    attributes = attributes.to_dict()
                elif hasattr(attributes, '__dict__'):
                    attributes = attributes.__dict__
                else:
                    attributes = {}
            except:
                attributes = {}
        
        # Build embedding_text
        embedding_text = build_embedding_text(
            clean_title=clean_title,
            product_type=product_type,
            attributes=attributes,
            details_has_semantic=details_has_semantic,
            semantic_detail_count=semantic_detail_count,
            usage_features=usage_features,
            description_fallback=description_fallback
        )
        
        # Đảm bảo embedding_text không rỗng
        if not embedding_text or not embedding_text.strip():
            embedding_text = clean_title if clean_title else "Product"
        
        results.append({
            "parent_asin": str(parent_asin),
            "embedding_text": embedding_text.strip()
        })
    
    print(f"\n[OK] Đã xử lý xong {len(rows):,} items")
    print(f"  Items có embedding_text: {len(results):,}")
    
    # Tạo DataFrame mới
    result_df = pl.DataFrame(results)
    
    # Thống kê
    if len(result_df) > 0:
        print(f"\nThống kê:")
        print(f"  Số items: {len(result_df):,}")
        
        # Thống kê độ dài embedding_text
        text_lengths = result_df["embedding_text"].str.len_chars()
        print(f"\nĐộ dài embedding_text:")
        print(f"  Trung bình: {text_lengths.mean():.1f} ký tự")
        print(f"  Min: {text_lengths.min()} ký tự")
        print(f"  Max: {text_lengths.max()} ký tự")
        print(f"  Median: {text_lengths.median():.1f} ký tự")
        
        # Đếm items có embedding_text rỗng
        empty_count = result_df.filter(pl.col("embedding_text").str.len_chars() == 0).height
        print(f"\n  Items có embedding_text rỗng: {empty_count:,}")
    
    # Lưu ra file
    output_file.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nĐang lưu file: {output_file}")
    result_df.write_parquet(str(output_file))
    print(f"[OK] Đã lưu {len(result_df):,} items vào {output_file}")
    
    # In một vài mẫu
    print("\n" + "=" * 80)
    print("MẪU DỮ LIỆU (5 items đầu tiên):")
    print("=" * 80)
    for i, row in enumerate(result_df.head(5).to_dicts()):
        print(f"\n[Item {i+1}]")
        print(f"parent_asin: {row['parent_asin']}")
        print(f"embedding_text ({len(row['embedding_text'])} ký tự):")
        print(f"  {row['embedding_text']}")
    
    print("\n" + "=" * 80)
    print("[OK] HOÀN TẤT: File embedding_text.parquet đã được tạo thành công!")
    print("=" * 80)
    
    return result_df


def main():
    """Hàm chính để chạy build embedding text."""
    script_path = Path(__file__).resolve()
    # Tìm project root: từ app/embedding/data_preprocessing/ lên 3 cấp
    project_root = script_path.parent.parent.parent.parent
    
    # Đường dẫn file
    input_file = project_root / "data" / "embedding" / "semantic_attributes.parquet"
    output_file = project_root / "data" / "embedding" / "embedding_text.parquet"
    
    process_embedding_text(
        input_file=input_file,
        output_file=output_file
    )


if __name__ == "__main__":
    main()

