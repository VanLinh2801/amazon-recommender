"""
Extract Semantic Attributes
============================
Trích xuất semantic attributes từ title, details, features, description
phục vụ embedding.

Input: data/embedding/metadata_for_embedding_final.parquet
Output: data/embedding/semantic_attributes.parquet
"""

import polars as pl
from pathlib import Path
import sys
import io
import json
import re
from typing import Optional, Dict, List, Tuple


# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


# ============================================================================
# PRODUCT TYPE DETECTION - Rule-based với priority (specific -> general)
# ============================================================================

PRODUCT_TYPE_PATTERNS = [
    # Beard / Hair Tools - Most specific first
    ('beard straightener', 'beard straightener'),
    ('hair straightener', 'hair straightener'),
    ('heated hair brush', 'heated hair brush'),
    ('hair dryer', 'hair dryer'),
    ('hair curler', 'hair curler'),
    
    # Hair Extensions - Most specific first
    ('lace front wig', 'lace front wig'),
    ('lace front wigs', 'lace front wig'),
    ('lace wig', 'lace front wig'),
    ('13x1 lace front', 'lace front wig'),
    ('13x1 lace', 'lace front wig'),
    ('deep wave', 'deep wave hair extension'),
    ('brazilian curly', 'brazilian curly hair extension'),
    ('tape-in', 'tape-in hair extension'),
    ('clip-in', 'clip-in hair extension'),
    ('hair weave', 'hair weave'),
    ('hair extension', 'hair extension'),
    ('wig', 'wig'),
    ('wigs', 'wig'),
    
    # Hair Products & Accessories
    ('scrunchie', 'scrunchie'),
    ('headband', 'headband'),
    ('hair clip', 'hair clip'),
    ('hair clips', 'hair clip'),
    ('hair claw', 'hair claw clip'),
    ('hair barrette', 'hair barrette'),
    ('hair accessory', 'hair accessory'),
    ('hair accessories', 'hair accessory'),
    
    # Hair Styling Products
    ('hairspray', 'hairspray'),
    ('hair spray', 'hairspray'),
    ('hair gel', 'hair gel'),
    ('styling gel', 'hair gel'),
    ('hair mousse', 'hair mousse'),
    ('hair styling', 'hair styling product'),
    
    # Makeup - Foundation & Concealer
    ('foundation', 'foundation'),
    ('concealer', 'concealer'),
    
    # Makeup - Lip
    ('lipstick', 'lipstick'),
    ('lip stick', 'lipstick'),
    ('lip gloss', 'lip gloss'),
    ('lip balm', 'lip balm'),
    
    # Makeup - Eye
    ('automatic eyeliner', 'automatic eyeliner'),
    ('auto eyeliner', 'automatic eyeliner'),
    ('eyeliner pencil', 'eyeliner pencil'),
    ('eyeliner', 'eyeliner'),
    ('mascara', 'mascara'),
    ('eyeshadow', 'eyeshadow'),
    ('eye shadow', 'eyeshadow'),
    ('eyebrow pencil', 'eyebrow pencil'),
    ('eyebrow', 'eyebrow product'),
    
    # Makeup - Face Paint
    ('face paint', 'face paint'),
    
    # Makeup - Other Face
    ('blush', 'blush'),
    ('bronzer', 'bronzer'),
    ('face powder', 'face powder'),
    
    # Makeup Kits
    ('makeup kit', 'makeup kit'),
    ('cosmetic kit', 'cosmetic kit'),
    ('beauty kit', 'beauty kit'),
    ('makeup set', 'makeup kit'),
    
    # Makeup Tools
    ('makeup brush', 'makeup brush'),
    ('makeup brushes', 'makeup brush set'),
    ('makeup sponge', 'makeup sponge'),
    ('beauty sponge', 'makeup sponge'),
    ('makeup tool', 'makeup tool'),
    
    # Nail Care
    ('gel nail', 'gel nail'),
    ('acrylic nail', 'acrylic nail'),
    ('fake nail', 'fake nail'),
    ('nail polish', 'nail polish'),
    ('nail varnish', 'nail polish'),
    ('nail lacquer', 'nail polish'),
    
    # Skincare - Cream / Gel / Serum / Lotion
    ('cream', 'cream'),
    ('gel', 'gel'),
    ('serum', 'serum'),
    ('face serum', 'face serum'),
    ('lotion', 'lotion'),
    
    # Skincare - Cleanser / Toner / Mask
    ('cleanser', 'cleanser'),
    ('face cleanser', 'face cleanser'),
    ('toner', 'toner'),
    ('mask', 'mask'),
    ('face mask', 'face mask'),
    ('hair mask', 'hair mask'),
    
    # Skincare - Moisturizer
    ('moisturizer', 'moisturizer'),
    ('moisturiser', 'moisturizer'),
    
    # Hair Care
    ('shampoo', 'shampoo'),
    ('conditioner', 'conditioner'),
    ('hair treatment', 'hair treatment'),
    
    # Fragrance
    ('perfume', 'perfume'),
    ('cologne', 'cologne'),
    ('fragrance', 'fragrance'),
    
    # Personal Care
    ('deodorant', 'deodorant'),
    ('deodarant', 'deodorant'),  # Common typo
    ('antiperspirant', 'antiperspirant'),
    ('anti-perspirant', 'antiperspirant'),
    
    # Fashion Accessories
    ('knitted hat', 'knitted hat'),
    ('knitted cap', 'knitted cap'),
    ('rabbit fur hat', 'rabbit fur hat'),
    ('fur hat', 'fur hat'),
    ('hat', 'hat'),
    ('cap', 'cap'),
    ('scarf', 'scarf'),
    ('fashion accessory', 'fashion accessory'),
    
    # Beauty Accessories
    ('tweezers', 'tweezers'),
    ('beauty mirror', 'beauty mirror'),
    ('makeup mirror', 'beauty mirror'),
    ('beauty accessory', 'beauty accessory'),
]


def detect_product_type(title: str) -> str:
    """
    Xác định product_type từ title bằng rule-based matching.
    Ưu tiên match cụ thể nhất trước.
    
    Args:
        title: Title của sản phẩm
        
    Returns:
        Product type string hoặc 'default'
    """
    if not title:
        return 'default'
    
    title_lower = title.lower()
    
    # Kiểm tra theo thứ tự từ cụ thể đến chung
    for pattern, product_type in PRODUCT_TYPE_PATTERNS:
        if pattern in title_lower:
            return product_type
    
    return 'default'


# ============================================================================
# TITLE CLEANING
# ============================================================================

def clean_title(title: str) -> str:
    """
    Làm sạch title: loại bỏ ký tự đặc biệt, normalize khoảng trắng.
    
    Args:
        title: Title gốc
        
    Returns:
        Title đã được làm sạch
    """
    if not title:
        return ""
    
    # Chuyển về lowercase và strip
    cleaned = str(title).strip().lower()
    
    # Loại bỏ ký tự đặc biệt không cần thiết (giữ lại dấu gạch ngang, dấu cách)
    cleaned = re.sub(r'[^\w\s\-]', ' ', cleaned)
    
    # Normalize khoảng trắng (nhiều khoảng trắng thành một)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned.strip()


# ============================================================================
# SEMANTIC DENSITY FOR DETAILS
# ============================================================================

# Whitelist key cho semantic density check
# Dùng semantic density thay vì null check vì details có thể tồn tại
# nhưng chứa toàn None/empty, không có giá trị semantic thực sự
SEMANTIC_DETAIL_WHITELIST = [
    'Item Form',
    'Material',
    'Hair Type',
    'Finish Type',
    'Coverage',
    'Skin Type',
    'Scent',
    'Color',
    'Installation Type'
]


def check_semantic_density(details: Dict) -> Tuple[bool, int, Dict]:
    """
    Kiểm tra semantic density của details.
    Dùng semantic density thay vì null check vì details có thể tồn tại
    nhưng chứa toàn None/empty, không có giá trị semantic thực sự.
    
    Args:
        details: Dict chứa details từ raw_metadata
        
    Returns:
        Tuple (details_has_semantic, semantic_detail_count, attributes)
        - details_has_semantic: True nếu semantic_detail_count >= 2
        - semantic_detail_count: Số key trong whitelist có value hợp lệ
        - attributes: Dict các attributes đã được lọc
    """
    if not details or not isinstance(details, dict):
        return False, 0, {}
    
    attributes = {}
    semantic_detail_count = 0
    
    for field in SEMANTIC_DETAIL_WHITELIST:
        value = details.get(field)
        # Kiểm tra value hợp lệ: không None, không empty, không phải "N/A"
        if value is not None:
            value_str = str(value).strip()
            value_lower = value_str.lower()
            if value_str and value_lower not in ['null', 'none', 'n/a', 'unknown', '']:
                # Làm sạch giá trị
                # Giới hạn độ dài để tránh quá dài
                if len(value_str) <= 100:
                    attributes[field] = value_str
                    semantic_detail_count += 1
    
    details_has_semantic = semantic_detail_count >= 2
    
    return details_has_semantic, semantic_detail_count, attributes


# ============================================================================
# USAGE FEATURES EXTRACTION
# ============================================================================

def extract_usage_features(features: List[str]) -> List[str]:
    """
    Trích xuất usage features từ features.
    
    Args:
        features: List các features
        
    Returns:
        List ngắn các usage features (tối đa 5)
    """
    usage_features = []
    
    if features and isinstance(features, list):
        for feat in features:
            if feat and str(feat).strip():
                feat_str = str(feat).strip()
                # Chỉ lấy features ngắn và có ý nghĩa
                if 5 <= len(feat_str) <= 100:
                    usage_features.append(feat_str)
                    if len(usage_features) >= 5:
                        break
    
    return usage_features[:5]  # Giới hạn tối đa 5


# ============================================================================
# DESCRIPTION FALLBACK EXTRACTION
# ============================================================================

# Từ khóa marketing cần loại bỏ
MARKETING_KEYWORDS = ['gift', 'perfect', 'women', 'girls']


def extract_description_fallback(description: List[str]) -> str:
    """
    Trích xuất description fallback khi details_has_semantic == False.
    Chỉ lấy 1 câu đầu, không lấy câu mang tính marketing.
    
    Args:
        description: List các description
        
    Returns:
        String description fallback hoặc empty string
    """
    if not description or not isinstance(description, list):
        return ""
    
    for desc in description:
        if desc and str(desc).strip():
            desc_str = str(desc).strip()
            
            # Lấy câu đầu tiên (trước dấu chấm)
            first_sentence = desc_str.split('.')[0].strip()
            if not first_sentence:
                first_sentence = desc_str.split('!')[0].strip()
            if not first_sentence:
                first_sentence = desc_str.split('?')[0].strip()
            if not first_sentence:
                # Nếu không có dấu câu, lấy 100 ký tự đầu
                first_sentence = desc_str[:100].strip()
            
            # Kiểm tra không chứa từ khóa marketing
            desc_lower = first_sentence.lower()
            has_marketing = any(keyword in desc_lower for keyword in MARKETING_KEYWORDS)
            
            if not has_marketing and len(first_sentence) >= 10:
                # Giới hạn độ dài
                if len(first_sentence) > 150:
                    first_sentence = first_sentence[:147] + "..."
                return first_sentence
    
    return ""


# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def process_semantic_attributes(
    input_file: Path,
    output_file: Path
):
    """
    Xử lý và trích xuất semantic attributes từ metadata.
    """
    print("=" * 80)
    print("EXTRACT SEMANTIC ATTRIBUTES")
    print("=" * 80)
    
    # Đọc file input
    print(f"\nĐang đọc file: {input_file}")
    if not input_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {input_file}")
    
    df = pl.read_parquet(str(input_file))
    print(f"[OK] Đã đọc {len(df):,} items")
    
    # Lọc chỉ các item có embedding_eligible == True
    print(f"\nĐang lọc items có embedding_eligible == True...")
    df_filtered = df.filter(pl.col("embedding_eligible") == True)
    print(f"[OK] Còn lại {len(df_filtered):,} items sau khi lọc")
    
    if len(df_filtered) == 0:
        print("[WARNING] Không có items nào để xử lý!")
        return
    
    # Xử lý từng item
    print(f"\nĐang trích xuất semantic attributes...")
    
    rows = df_filtered.to_dicts()
    results = []
    
    for i, row in enumerate(rows):
        if (i + 1) % 10000 == 0:
            print(f"  Đã xử lý: {i + 1:,}/{len(rows):,} items")
        
        parent_asin = row.get('parent_asin')
        if not parent_asin:
            continue
        
        # Lấy các trường cần thiết
        title = row.get('title', '')
        details = row.get('details')
        features = row.get('features', [])
        description = row.get('description', [])
        
        # 1. Clean title
        clean_title_str = clean_title(title)
        
        # 2. Detect product type từ clean_title
        product_type = detect_product_type(clean_title_str)
        
        # 3. Check semantic density và extract attributes từ details
        # Polars Struct đã được convert sang dict khi dùng to_dicts()
        details_dict = {}
        if details:
            if isinstance(details, dict):
                details_dict = details
            else:
                # Fallback: thử convert sang dict
                try:
                    if hasattr(details, 'to_dict'):
                        details_dict = details.to_dict()
                    elif hasattr(details, '__dict__'):
                        details_dict = details.__dict__
                    elif isinstance(details, (list, tuple)) and len(details) > 0:
                        # Có thể là list of tuples
                        details_dict = dict(details)
                    else:
                        details_dict = {}
                except:
                    details_dict = {}
        
        # Kiểm tra semantic density (không kiểm tra details is None)
        details_has_semantic, semantic_detail_count, attributes = check_semantic_density(details_dict)
        
        # 4. Extract usage features
        usage_features = extract_usage_features(features)
        
        # 5. Extract description fallback nếu details_has_semantic == False
        description_fallback = ""
        if not details_has_semantic:
            description_fallback = extract_description_fallback(description)
        
        # 6. Kiểm tra điều kiện semantic tối thiểu
        # Một item được coi là semantic hợp lệ nếu ít nhất 1 trong các điều kiện sau đúng:
        # - product_type != "default"
        # - details_has_semantic == True
        # - usage_features hoặc description_fallback không rỗng
        has_valid_semantic = (
            product_type != "default" or
            details_has_semantic or
            len(usage_features) > 0 or
            len(description_fallback) > 0
        )
        
        # Tạo kết quả
        results.append({
            "parent_asin": str(parent_asin),
            "clean_title": clean_title_str,
            "product_type": product_type,
            "attributes": attributes,
            "usage_features": usage_features,
            "description_fallback": description_fallback,
            "details_has_semantic": details_has_semantic,
            "semantic_detail_count": semantic_detail_count,
            "has_valid_semantic": has_valid_semantic
        })
    
    print(f"\n[OK] Đã xử lý xong {len(rows):,} items")
    print(f"  Items có kết quả: {len(results):,}")
    
    # Tạo DataFrame mới
    result_df = pl.DataFrame(results)
    
    # Thống kê
    if len(result_df) > 0:
        print(f"\nThống kê:")
        print(f"  Số items: {len(result_df):,}")
        
        # Thống kê product types
        product_type_counts = result_df.group_by("product_type").agg(pl.len().alias("count")).sort("count", descending=True)
        print(f"\nTop 10 product types:")
        for row in product_type_counts.head(10).to_dicts():
            print(f"  {row['product_type']}: {row['count']:,}")
        
        # Thống kê attributes
        def has_attributes(attrs):
            return isinstance(attrs, dict) and len(attrs) > 0
        
        items_with_attrs = result_df.filter(pl.col("attributes").map_elements(has_attributes, return_dtype=pl.Boolean)).height
        print(f"\n  Items có attributes: {items_with_attrs:,} ({items_with_attrs/len(result_df)*100:.1f}%)")
        
        # Thống kê usage_features
        items_with_features = result_df.filter(pl.col("usage_features").list.len() > 0).height
        print(f"  Items có usage_features: {items_with_features:,} ({items_with_features/len(result_df)*100:.1f}%)")
        
        # Thống kê description_fallback
        items_with_desc_fallback = result_df.filter(pl.col("description_fallback").str.len_chars() > 0).height
        print(f"  Items có description_fallback: {items_with_desc_fallback:,} ({items_with_desc_fallback/len(result_df)*100:.1f}%)")
        
        # Thống kê semantic density
        items_with_semantic_details = result_df.filter(pl.col("details_has_semantic") == True).height
        print(f"  Items có details_has_semantic: {items_with_semantic_details:,} ({items_with_semantic_details/len(result_df)*100:.1f}%)")
        
        # Thống kê valid semantic
        items_valid_semantic = result_df.filter(pl.col("has_valid_semantic") == True).height
        print(f"  Items có valid semantic: {items_valid_semantic:,} ({items_valid_semantic/len(result_df)*100:.1f}%)")
        
        # Thống kê product_type = default
        items_default_type = result_df.filter(pl.col("product_type") == "default").height
        print(f"  Items có product_type = default: {items_default_type:,} ({items_default_type/len(result_df)*100:.1f}%)")
    
    # Lưu ra file
    output_file.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nĐang lưu file: {output_file}")
    result_df.write_parquet(str(output_file))
    print(f"[OK] Đã lưu {len(result_df):,} items vào {output_file}")
    
    # In một vài mẫu
    print("\n" + "=" * 80)
    print("MẪU DỮ LIỆU (3 items đầu tiên):")
    print("=" * 80)
    for i, row in enumerate(result_df.head(3).to_dicts()):
        print(f"\n[Item {i+1}]")
        print(f"parent_asin: {row['parent_asin']}")
        print(f"clean_title: {row['clean_title']}")
        print(f"product_type: {row['product_type']}")
        print(f"details_has_semantic: {row['details_has_semantic']} (count: {row['semantic_detail_count']})")
        print(f"attributes: {row['attributes']}")
        print(f"usage_features: {row['usage_features']}")
        print(f"description_fallback: {row['description_fallback']}")
        print(f"has_valid_semantic: {row['has_valid_semantic']}")
    
    print("\n" + "=" * 80)
    print("[OK] HOÀN TẤT: File semantic_attributes.parquet đã được tạo thành công!")
    print("=" * 80)
    
    return result_df


def main():
    """Hàm chính để chạy extract semantic attributes."""
    script_path = Path(__file__).resolve()
    # Tìm project root: từ app/embedding/data_preprocessing/ lên 3 cấp
    project_root = script_path.parent.parent.parent.parent
    
    # Đường dẫn file
    input_file = project_root / "data" / "embedding" / "metadata_for_embedding_final.parquet"
    output_file = project_root / "data" / "embedding" / "semantic_attributes.parquet"
    
    process_semantic_attributes(
        input_file=input_file,
        output_file=output_file
    )


if __name__ == "__main__":
    main()

