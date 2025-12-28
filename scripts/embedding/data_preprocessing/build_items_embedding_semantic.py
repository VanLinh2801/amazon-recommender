"""
Build Items Embedding Text (Semantic Scaffolding Version)
==========================================================
Mục tiêu: Tạo embedding_text bằng semantic scaffolding với template cố định
và domain knowledge mapping để tối ưu semantic separation trong vector space.

Approach: Rule-based + Semantic Map + Fixed Template
- Không dùng LLM để rewrite
- Không viết văn tự nhiên
- Tập trung vào semantic slots: product_type, primary_use, functional_role, category

Input: 
  - data/embedding/items_for_embedding_en_final.parquet (item_id, embedding_text)
  - data/processed/items_for_rs.parquet (metadata gốc)

Output: data/embedding/items_for_embedding_semantic.parquet

Chạy độc lập: python app/data_preprocessing/build_items_embedding_semantic.py
"""

import polars as pl
from pathlib import Path
import sys
import io
import json
import re
from typing import Optional, Dict, Tuple

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


# ============================================================================
# STEP 1: PRODUCT TYPE DETECTION - Rule-based với priority (specific -> general)
# ============================================================================

# Product type keywords theo độ đặc thù (từ cao đến thấp)
# Mỗi tuple: (keyword_pattern, product_type)
# Keywords được kiểm tra theo thứ tự, match đầu tiên sẽ được dùng
PRODUCT_TYPE_PATTERNS = [
    # Hair Extensions - Most specific first
    ('lace front wig', 'lace front wig'),
    ('lace front wigs', 'lace front wig'),
    ('13x1 lace front', 'lace front wig'),
    ('13x1 lace', 'lace front wig'),
    ('deep wave', 'deep wave hair extension'),
    ('brazilian curly', 'brazilian curly hair extension'),
    ('hair weave', 'hair weave'),
    ('hair extension', 'hair extension'),
    ('wig', 'wig'),
    ('wigs', 'wig'),
    
    # Hair Styling Products
    ('hairspray', 'hairspray'),
    ('hair spray', 'hairspray'),
    ('hair gel', 'hair gel'),
    ('styling gel', 'hair gel'),
    ('hair mousse', 'hair mousse'),
    ('hair styling', 'hair styling product'),
    
    # Hair Accessories
    ('hair clip', 'hair clip'),
    ('hair clips', 'hair clip'),
    ('hair claw', 'hair claw clip'),
    ('hair barrette', 'hair barrette'),
    ('hair accessory', 'hair accessory'),
    ('hair accessories', 'hair accessory'),
    
    # Eye Makeup - Specific forms first
    ('automatic eyeliner', 'automatic eyeliner'),
    ('auto eyeliner', 'automatic eyeliner'),
    ('eyeliner pencil', 'eyeliner pencil'),
    ('eyeliner', 'eyeliner'),
    ('mascara', 'mascara'),
    ('eyeshadow', 'eyeshadow'),
    ('eye shadow', 'eyeshadow'),
    ('eyebrow pencil', 'eyebrow pencil'),
    
    # Lip Makeup
    ('lipstick', 'lipstick'),
    ('lip stick', 'lipstick'),
    ('lip gloss', 'lip gloss'),
    ('lip balm', 'lip balm'),
    
    # Face Makeup
    ('foundation', 'foundation'),
    ('concealer', 'concealer'),
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
    ('nail polish', 'nail polish'),
    ('nail varnish', 'nail polish'),
    ('nail lacquer', 'nail polish'),
    
    # Fragrance
    ('perfume', 'perfume'),
    ('cologne', 'cologne'),
    ('fragrance', 'fragrance'),
    
    # Personal Care
    ('deodorant', 'deodorant'),
    ('deodarant', 'deodorant'),  # Common typo
    ('antiperspirant', 'antiperspirant'),
    ('anti-perspirant', 'antiperspirant'),
    
    # Hair Care
    ('shampoo', 'shampoo'),
    ('conditioner', 'conditioner'),
    ('hair mask', 'hair mask'),
    ('hair treatment', 'hair treatment'),
    
    # Skincare
    ('moisturizer', 'moisturizer'),
    ('moisturiser', 'moisturizer'),
    ('cleanser', 'cleanser'),
    ('face cleanser', 'face cleanser'),
    ('serum', 'serum'),
    ('face serum', 'face serum'),
    ('toner', 'toner'),
    
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


# ============================================================================
# STEP 2: SEMANTIC MAP - Domain knowledge mapping
# ============================================================================

SEMANTIC_MAP: Dict[str, Dict[str, str]] = {
    # Eye Makeup
    'eyeliner': {
        'primary_use': 'eye makeup application',
        'functional_role': 'defining and enhancing eye appearance',
        'category': 'Beauty and Eye Makeup'
    },
    'automatic eyeliner': {
        'primary_use': 'eye makeup application',
        'functional_role': 'defining and enhancing eye appearance with precision',
        'category': 'Beauty and Eye Makeup'
    },
    'eyeliner pencil': {
        'primary_use': 'eye makeup application',
        'functional_role': 'defining and enhancing eye appearance',
        'category': 'Beauty and Eye Makeup'
    },
    'mascara': {
        'primary_use': 'eye makeup application',
        'functional_role': 'enhancing eyelash length and volume',
        'category': 'Beauty and Eye Makeup'
    },
    'eyeshadow': {
        'primary_use': 'eye makeup application',
        'functional_role': 'adding color and dimension to eyelids',
        'category': 'Beauty and Eye Makeup'
    },
    'eyebrow pencil': {
        'primary_use': 'eye makeup application',
        'functional_role': 'defining and shaping eyebrow appearance',
        'category': 'Beauty and Eye Makeup'
    },
    
    # Lip Makeup
    'lipstick': {
        'primary_use': 'lip makeup application',
        'functional_role': 'adding color and definition to lips',
        'category': 'Beauty and Lip Makeup'
    },
    'lip gloss': {
        'primary_use': 'lip makeup application',
        'functional_role': 'adding shine and color to lips',
        'category': 'Beauty and Lip Makeup'
    },
    'lip balm': {
        'primary_use': 'lip care and protection',
        'functional_role': 'moisturizing and protecting lips',
        'category': 'Beauty and Lip Makeup'
    },
    
    # Face Makeup
    'foundation': {
        'primary_use': 'face makeup application',
        'functional_role': 'creating even skin tone and coverage',
        'category': 'Beauty and Face Makeup'
    },
    'concealer': {
        'primary_use': 'face makeup application',
        'functional_role': 'covering imperfections and blemishes',
        'category': 'Beauty and Face Makeup'
    },
    'blush': {
        'primary_use': 'face makeup application',
        'functional_role': 'adding color and dimension to cheeks',
        'category': 'Beauty and Face Makeup'
    },
    'bronzer': {
        'primary_use': 'face makeup application',
        'functional_role': 'adding warmth and contour to face',
        'category': 'Beauty and Face Makeup'
    },
    'face powder': {
        'primary_use': 'face makeup application',
        'functional_role': 'setting makeup and reducing shine',
        'category': 'Beauty and Face Makeup'
    },
    
    # Hair Extensions
    'wig': {
        'primary_use': 'hair extension and cosmetic styling',
        'functional_role': 'creating a natural hairstyle for beauty and fashion',
        'category': 'Beauty and Hair Extensions'
    },
    'lace front wig': {
        'primary_use': 'hair extension and cosmetic styling',
        'functional_role': 'creating a natural hairstyle with realistic hairline',
        'category': 'Beauty and Hair Extensions'
    },
    'hair extension': {
        'primary_use': 'hair extension and cosmetic styling',
        'functional_role': 'adding length and volume to natural hair',
        'category': 'Beauty and Hair Extensions'
    },
    'deep wave hair extension': {
        'primary_use': 'hair extension and cosmetic styling',
        'functional_role': 'creating deep wave hairstyle with extensions',
        'category': 'Beauty and Hair Extensions'
    },
    'brazilian curly hair extension': {
        'primary_use': 'hair extension and cosmetic styling',
        'functional_role': 'creating curly hairstyle with Brazilian hair extensions',
        'category': 'Beauty and Hair Extensions'
    },
    'hair weave': {
        'primary_use': 'hair extension and cosmetic styling',
        'functional_role': 'adding length and volume through weaving technique',
        'category': 'Beauty and Hair Extensions'
    },
    
    # Hair Styling
    'hairspray': {
        'primary_use': 'hair styling and hold',
        'functional_role': 'maintaining hairstyle shape and preventing frizz',
        'category': 'Beauty and Hair Styling'
    },
    'hair gel': {
        'primary_use': 'hair styling and hold',
        'functional_role': 'shaping and holding hair in desired style',
        'category': 'Beauty and Hair Styling'
    },
    'hair mousse': {
        'primary_use': 'hair styling and hold',
        'functional_role': 'adding volume and texture to hair',
        'category': 'Beauty and Hair Styling'
    },
    'hair styling product': {
        'primary_use': 'hair styling and hold',
        'functional_role': 'shaping and maintaining hairstyle',
        'category': 'Beauty and Hair Styling'
    },
    
    # Hair Accessories
    'hair clip': {
        'primary_use': 'hair styling and accessories',
        'functional_role': 'securing and styling hair in place',
        'category': 'Beauty and Hair Accessories'
    },
    'hair claw clip': {
        'primary_use': 'hair styling and accessories',
        'functional_role': 'securing hair with claw-style grip',
        'category': 'Beauty and Hair Accessories'
    },
    'hair barrette': {
        'primary_use': 'hair styling and accessories',
        'functional_role': 'decorative hair securing accessory',
        'category': 'Beauty and Hair Accessories'
    },
    'hair accessory': {
        'primary_use': 'hair styling and accessories',
        'functional_role': 'decorating and securing hair',
        'category': 'Beauty and Hair Accessories'
    },
    
    # Makeup Kits
    'makeup kit': {
        'primary_use': 'cosmetic application',
        'functional_role': 'providing multiple makeup products for complete application',
        'category': 'Beauty and Makeup Kits'
    },
    'cosmetic kit': {
        'primary_use': 'cosmetic application',
        'functional_role': 'providing multiple cosmetic products for complete application',
        'category': 'Beauty and Makeup Kits'
    },
    'beauty kit': {
        'primary_use': 'cosmetic application',
        'functional_role': 'providing multiple beauty products for complete application',
        'category': 'Beauty and Makeup Kits'
    },
    
    # Makeup Tools
    'makeup brush': {
        'primary_use': 'makeup application tools',
        'functional_role': 'applying and blending makeup products',
        'category': 'Beauty and Makeup Tools'
    },
    'makeup brush set': {
        'primary_use': 'makeup application tools',
        'functional_role': 'providing multiple brushes for various makeup applications',
        'category': 'Beauty and Makeup Tools'
    },
    'makeup sponge': {
        'primary_use': 'makeup application tools',
        'functional_role': 'blending and applying liquid and cream makeup',
        'category': 'Beauty and Makeup Tools'
    },
    'makeup tool': {
        'primary_use': 'makeup application tools',
        'functional_role': 'aiding in makeup application process',
        'category': 'Beauty and Makeup Tools'
    },
    
    # Nail Care
    'nail polish': {
        'primary_use': 'nail care and decoration',
        'functional_role': 'adding color and protection to nails',
        'category': 'Beauty and Nail Care'
    },
    'nail varnish': {
        'primary_use': 'nail care and decoration',
        'functional_role': 'adding color and protection to nails',
        'category': 'Beauty and Nail Care'
    },
    
    # Fragrance
    'perfume': {
        'primary_use': 'fragrance and personal scent',
        'functional_role': 'providing pleasant scent for personal use',
        'category': 'Beauty and Fragrance'
    },
    'cologne': {
        'primary_use': 'fragrance and personal scent',
        'functional_role': 'providing pleasant scent for personal use',
        'category': 'Beauty and Fragrance'
    },
    'fragrance': {
        'primary_use': 'fragrance and personal scent',
        'functional_role': 'providing pleasant scent for personal use',
        'category': 'Beauty and Fragrance'
    },
    
    # Personal Care
    'deodorant': {
        'primary_use': 'personal hygiene and odor control',
        'functional_role': 'preventing body odor and maintaining freshness',
        'category': 'Beauty and Personal Care'
    },
    'antiperspirant': {
        'primary_use': 'personal hygiene and sweat control',
        'functional_role': 'reducing perspiration and preventing body odor',
        'category': 'Beauty and Personal Care'
    },
    
    # Hair Care
    'shampoo': {
        'primary_use': 'hair care and cleansing',
        'functional_role': 'cleaning and maintaining hair health',
        'category': 'Beauty and Hair Care'
    },
    'conditioner': {
        'primary_use': 'hair care and conditioning',
        'functional_role': 'moisturizing and detangling hair',
        'category': 'Beauty and Hair Care'
    },
    'hair mask': {
        'primary_use': 'hair care and treatment',
        'functional_role': 'deep conditioning and repairing hair',
        'category': 'Beauty and Hair Care'
    },
    'hair treatment': {
        'primary_use': 'hair care and treatment',
        'functional_role': 'repairing and improving hair condition',
        'category': 'Beauty and Hair Care'
    },
    
    # Skincare
    'moisturizer': {
        'primary_use': 'skincare and hydration',
        'functional_role': 'moisturizing and protecting skin',
        'category': 'Beauty and Skincare'
    },
    'cleanser': {
        'primary_use': 'skincare and cleansing',
        'functional_role': 'removing dirt and impurities from skin',
        'category': 'Beauty and Skincare'
    },
    'face cleanser': {
        'primary_use': 'skincare and cleansing',
        'functional_role': 'removing dirt and impurities from facial skin',
        'category': 'Beauty and Skincare'
    },
    'serum': {
        'primary_use': 'skincare treatment',
        'functional_role': 'targeted treatment for specific skin concerns',
        'category': 'Beauty and Skincare'
    },
    'face serum': {
        'primary_use': 'skincare treatment',
        'functional_role': 'targeted treatment for facial skin concerns',
        'category': 'Beauty and Skincare'
    },
    'toner': {
        'primary_use': 'skincare treatment',
        'functional_role': 'balancing skin pH and preparing for other products',
        'category': 'Beauty and Skincare'
    },
    
    # Fashion Accessories
    'hat': {
        'primary_use': 'fashion accessory for everyday wear',
        'functional_role': 'appearance enhancement and casual fashion',
        'category': 'Beauty and Fashion Accessories'
    },
    'knitted hat': {
        'primary_use': 'fashion accessory for everyday wear',
        'functional_role': 'warmth and appearance enhancement',
        'category': 'Beauty and Fashion Accessories'
    },
    'knitted cap': {
        'primary_use': 'fashion accessory for everyday wear',
        'functional_role': 'warmth and appearance enhancement',
        'category': 'Beauty and Fashion Accessories'
    },
    'rabbit fur hat': {
        'primary_use': 'fashion accessory for everyday wear',
        'functional_role': 'luxury appearance enhancement and warmth',
        'category': 'Beauty and Fashion Accessories'
    },
    'fur hat': {
        'primary_use': 'fashion accessory for everyday wear',
        'functional_role': 'luxury appearance enhancement and warmth',
        'category': 'Beauty and Fashion Accessories'
    },
    'cap': {
        'primary_use': 'fashion accessory for everyday wear',
        'functional_role': 'appearance enhancement and casual fashion',
        'category': 'Beauty and Fashion Accessories'
    },
    'scarf': {
        'primary_use': 'fashion accessory for everyday wear',
        'functional_role': 'appearance enhancement and fashion styling',
        'category': 'Beauty and Fashion Accessories'
    },
    'fashion accessory': {
        'primary_use': 'fashion accessory for everyday wear',
        'functional_role': 'appearance enhancement and fashion styling',
        'category': 'Beauty and Fashion Accessories'
    },
    
    # Beauty Accessories
    'tweezers': {
        'primary_use': 'beauty and grooming tools',
        'functional_role': 'precise hair removal and grooming',
        'category': 'Beauty and Makeup Tools'
    },
    'beauty mirror': {
        'primary_use': 'beauty and grooming tools',
        'functional_role': 'aiding in makeup application and grooming',
        'category': 'Beauty and Makeup Tools'
    },
    'makeup mirror': {
        'primary_use': 'beauty and grooming tools',
        'functional_role': 'aiding in makeup application and grooming',
        'category': 'Beauty and Makeup Tools'
    },
    'beauty accessory': {
        'primary_use': 'beauty and grooming tools',
        'functional_role': 'aiding in beauty and grooming routines',
        'category': 'Beauty and Makeup Tools'
    },
    
    # Default fallback
    'default': {
        'primary_use': 'beauty and personal care',
        'functional_role': 'enhancing personal appearance and care',
        'category': 'Beauty Products'
    }
}


# ============================================================================
# STEP 3: STRONG ATTRIBUTE EXTRACTION
# ============================================================================

# Attributes giúp phân biệt ngữ nghĩa tốt hơn
STRONG_ATTRIBUTES = [
    'automatic', 'auto', 'pencil', 'gel', 'liquid', 'cream', 'powder',
    'lace front', '13x1', 'deep wave', 'brazilian', 'curly', 'straight',
    'knitted', 'rabbit fur', 'human hair', 'synthetic', 'real fur',
    'unscented', 'scented', 'waterproof', 'long lasting', 'matte', 'glossy',
    'pre plucked', 'baby hair', 'density', 'pack of'
]


def extract_strong_attributes(title: str, details: dict = None) -> list:
    """
    Trích xuất các attribute mạnh giúp phân biệt ngữ nghĩa.
    
    Args:
        title: Title của sản phẩm
        details: Dict chứa details từ raw_metadata
        
    Returns:
        List các attribute strings
    """
    attributes = []
    title_lower = title.lower() if title else ""
    
    # Extract từ title
    for attr in STRONG_ATTRIBUTES:
        if attr in title_lower:
            # Format attribute để dễ đọc
            if attr == 'pack of':
                # Tìm số sau "pack of"
                match = re.search(r'pack of (\d+)', title_lower)
                if match:
                    attributes.append(f"pack of {match.group(1)}")
            elif attr == '13x1':
                attributes.append('13x1 lace front')
            elif attr == 'lace front':
                if '13x1' not in title_lower:
                    attributes.append('lace front')
            elif attr not in ['auto', 'human hair']:  # Tránh trùng với automatic, human hair
                attributes.append(attr)
    
    # Extract từ details nếu có
    if details and isinstance(details, dict):
        details_lower = {k.lower(): v for k, v in details.items()}
        
        # Material
        if 'material' in details_lower or 'material type' in details_lower:
            material = details_lower.get('material') or details_lower.get('material type')
            if material and str(material).strip().lower() in ['human hair', 'synthetic', 'rabbit fur', 'real fur']:
                attributes.append(str(material).strip().lower())
        
        # Form
        if 'form' in details_lower or 'product form' in details_lower or 'item form' in details_lower:
            form = details_lower.get('form') or details_lower.get('product form') or details_lower.get('item form')
            if form and str(form).strip().lower() in ['pencil', 'gel', 'liquid', 'cream', 'powder']:
                attributes.append(str(form).strip().lower())
    
    # Loại bỏ duplicates và giữ tối đa 3 attributes quan trọng nhất
    unique_attrs = []
    seen = set()
    for attr in attributes:
        attr_lower = attr.lower()
        if attr_lower not in seen:
            seen.add(attr_lower)
            unique_attrs.append(attr)
            if len(unique_attrs) >= 3:
                break
    
    return unique_attrs


# ============================================================================
# STEP 4: PRODUCT TYPE DETECTION FUNCTION
# ============================================================================

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
# STEP 5: BUILD EMBEDDING TEXT WITH FIXED TEMPLATE
# ============================================================================

def build_semantic_embedding_text(
    title: str,
    raw_metadata: dict = None,
    details: dict = None
) -> str:
    """
    Xây dựng embedding_text bằng semantic scaffolding với template cố định.
    
    Template: "Product type: {product_type}. Primary use: {primary_use}. 
               Functional role: {functional_role}. Key characteristics: {attributes}. 
               Category: {category}."
    
    Args:
        title: Title của sản phẩm
        raw_metadata: Raw metadata dict
        details: Details dict từ raw_metadata
        
    Returns:
        Embedding text string
    """
    # Step 1: Detect product type
    product_type = detect_product_type(title)
    
    # Step 2: Get semantic mapping
    semantic_info = SEMANTIC_MAP.get(product_type, SEMANTIC_MAP['default'])
    primary_use = semantic_info['primary_use']
    functional_role = semantic_info['functional_role']
    category = semantic_info['category']
    
    # Step 3: Extract strong attributes
    attributes = extract_strong_attributes(title, details)
    
    # Step 4: Build embedding text với template cố định
    # Template được thiết kế để đảm bảo độ dài ổn định và semantic separation tốt
    parts = [
        f"Product type: {product_type}",
        f"Primary use: {primary_use}",
        f"Functional role: {functional_role}"
    ]
    
    # Thêm key characteristics nếu có
    if attributes:
        attributes_str = ", ".join(attributes)
        parts.append(f"Key characteristics: {attributes_str}")
    
    # Category luôn có
    parts.append(f"Category: {category}")
    
    # Kết hợp thành một đoạn văn
    embedding_text = ". ".join(parts) + "."
    
    # Đảm bảo độ dài tối thiểu để có đủ semantic information
    # Nếu quá ngắn (< 150), thêm thông tin bổ sung
    if len(embedding_text) < 150:
        # Thêm mô tả về application context nếu thiếu
        if 'beauty' not in embedding_text.lower() and 'cosmetic' not in embedding_text.lower():
            parts.insert(-1, f"Application context: beauty and personal care product")
            embedding_text = ". ".join(parts) + "."
    
    # Đảm bảo không vượt quá 500 ký tự
    if len(embedding_text) > 500:
        # Giữ các phần quan trọng nhất
        essential_parts = [
            f"Product type: {product_type}",
            f"Primary use: {primary_use}",
            f"Category: {category}"
        ]
        embedding_text = ". ".join(essential_parts) + "."
    
    return embedding_text


# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def process_items_for_semantic_embedding(
    input_embedding_file: Path,
    input_metadata_file: Path,
    output_file: Path
):
    """
    Xử lý items để tạo embedding_text bằng semantic scaffolding.
    """
    print("=" * 80)
    print("BUILD ITEMS EMBEDDING TEXT (SEMANTIC SCAFFOLDING VERSION)")
    print("=" * 80)
    
    # Đọc file embedding hiện tại (chỉ để lấy item_id)
    print(f"\nĐang đọc file embedding: {input_embedding_file}")
    if not input_embedding_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {input_embedding_file}")
    
    embedding_df = pl.read_parquet(str(input_embedding_file))
    print(f"[OK] Đã đọc {len(embedding_df):,} items từ embedding file")
    
    # Đọc file metadata
    print(f"\nĐang đọc file metadata: {input_metadata_file}")
    if not input_metadata_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {input_metadata_file}")
    
    metadata_df = pl.read_parquet(str(input_metadata_file))
    print(f"[OK] Đã đọc {len(metadata_df):,} items từ metadata file")
    
    # Join theo item_id
    print("\nĐang join dữ liệu theo item_id...")
    if 'item_id' not in embedding_df.columns:
        raise ValueError("File embedding phải có cột 'item_id'")
    
    if 'item_id' not in metadata_df.columns:
        if 'parent_asin' in metadata_df.columns:
            metadata_df = metadata_df.rename({'parent_asin': 'item_id'})
        else:
            raise ValueError("File metadata phải có cột 'item_id' hoặc 'parent_asin'")
    
    joined_df = embedding_df.select(['item_id']).join(
        metadata_df,
        on='item_id',
        how='left'
    )
    print(f"[OK] Đã join {len(joined_df):,} items")
    
    # Xử lý từng item
    print("\nĐang sinh embedding_text bằng semantic scaffolding...")
    
    rows = joined_df.to_dicts()
    results = []
    empty_count = 0
    
    for i, row in enumerate(rows):
        if (i + 1) % 10000 == 0:
            print(f"  Đã xử lý: {i + 1:,}/{len(rows):,} items")
        
        item_id = row.get('item_id')
        if not item_id:
            continue
        
        # Lấy title và metadata
        title = row.get('title', '')
        raw_metadata = row.get('raw_metadata')
        
        # Parse details từ raw_metadata
        details = None
        if raw_metadata:
            if isinstance(raw_metadata, str):
                try:
                    metadata = json.loads(raw_metadata)
                    details = metadata.get('details', {})
                except:
                    pass
            elif isinstance(raw_metadata, dict):
                details = raw_metadata.get('details', {})
        
        # Build semantic embedding text
        embedding_text = build_semantic_embedding_text(
            title=title,
            raw_metadata=raw_metadata,
            details=details
        )
        
        if not embedding_text or embedding_text.strip() == "":
            empty_count += 1
            continue
        
        results.append({
            "item_id": str(item_id),
            "embedding_text": embedding_text
        })
    
    print(f"\n[OK] Đã xử lý xong {len(rows):,} items")
    print(f"  Items có embedding_text: {len(results):,}")
    print(f"  Items không có embedding_text: {empty_count:,}")
    
    # Tạo DataFrame mới
    result_df = pl.DataFrame(results)
    
    # Thống kê độ dài
    if len(result_df) > 0:
        text_lengths = result_df["embedding_text"].str.len_chars()
        print(f"\nThống kê độ dài embedding_text:")
        print(f"  Trung bình: {text_lengths.mean():.1f} ký tự")
        print(f"  Min: {text_lengths.min()} ký tự")
        print(f"  Max: {text_lengths.max()} ký tự")
        print(f"  Median: {text_lengths.median():.1f} ký tự")
        print(f"  Std: {text_lengths.std():.1f} ký tự")
        
        # Thống kê product types
        print(f"\nThống kê product types (sample):")
        product_types = {}
        for row in result_df.head(1000).to_dicts():
            text = row['embedding_text']
            match = re.search(r'Product type: ([^.]+)', text)
            if match:
                pt = match.group(1).strip()
                product_types[pt] = product_types.get(pt, 0) + 1
        
        for pt, count in sorted(product_types.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {pt}: {count}")
    
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
        print(f"item_id: {row['item_id']}")
        print(f"embedding_text ({len(row['embedding_text'])} ký tự):")
        print(f"  {row['embedding_text']}")
    
    print("\n" + "=" * 80)
    print("[OK] HOÀN TẤT: File items_for_embedding_semantic.parquet đã được tạo thành công!")
    print("=" * 80)
    
    return result_df


def main():
    """Hàm chính để chạy build items embedding semantic."""
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
    
    # Kiểm tra file input có tồn tại không
    input_embedding_file = project_root / "data" / "embedding" / "items_for_embedding_en_final.parquet"
    if not input_embedding_file.exists():
        # Fallback: dùng file en_v2 nếu không có final
        input_embedding_file = project_root / "data" / "embedding" / "items_for_embedding_en_v2.parquet"
        if not input_embedding_file.exists():
            # Fallback tiếp: dùng file en
            input_embedding_file = project_root / "data" / "embedding" / "items_for_embedding_en.parquet"
    
    input_metadata_file = project_root / "data" / "processed" / "items_for_rs.parquet"
    output_file = project_root / "data" / "embedding" / "items_for_embedding_semantic.parquet"
    
    process_items_for_semantic_embedding(
        input_embedding_file=input_embedding_file,
        input_metadata_file=input_metadata_file,
        output_file=output_file
    )


if __name__ == "__main__":
    main()

