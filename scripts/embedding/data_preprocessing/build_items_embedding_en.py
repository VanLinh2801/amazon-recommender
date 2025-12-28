"""
Build Items Embedding Text (English Version)
=============================================
Mục tiêu: Tạo lại embedding_text bằng tiếng Anh để tối ưu cho model BAAI/bge-large-en-v1.5.
Tập trung vào product type, usage, và category cụ thể để cải thiện semantic similarity.

Input: 
  - data/embedding/items_for_embedding.parquet (item_id, embedding_text tiếng Việt)
  - data/processed/items_for_rs.parquet (metadata gốc)

Output: data/embedding/items_for_embedding_en.parquet (item_id, embedding_text tiếng Anh)

Chạy độc lập: python app/data_preprocessing/build_items_embedding_en.py
"""

import polars as pl
from pathlib import Path
import sys
import io
import json
import re

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


# Mapping từ tiếng Việt sang tiếng Anh cho các từ khóa phổ biến
CATEGORY_MAPPING = {
    "All Beauty": "Beauty Products",
    "Beauty": "Beauty Products"
}

# Product type keywords để nhận diện từ title
PRODUCT_TYPE_KEYWORDS = {
    'eyeliner': 'eyeliner',
    'mascara': 'mascara',
    'lipstick': 'lipstick',
    'foundation': 'foundation',
    'concealer': 'concealer',
    'blush': 'blush',
    'eyeshadow': 'eyeshadow',
    'makeup': 'makeup',
    'cosmetic': 'cosmetic',
    'wig': 'wig',
    'hair extension': 'hair extension',
    'hair clip': 'hair clip',
    'hair accessory': 'hair accessory',
    'hair spray': 'hairspray',
    'hairspray': 'hairspray',
    'hair gel': 'hair gel',
    'hair brush': 'hair brush',
    'comb': 'comb',
    'shampoo': 'shampoo',
    'conditioner': 'conditioner',
    'perfume': 'perfume',
    'cologne': 'cologne',
    'deodorant': 'deodorant',
    'antiperspirant': 'antiperspirant',
    'skincare': 'skincare',
    'moisturizer': 'moisturizer',
    'sunscreen': 'sunscreen',
    'cleanser': 'cleanser',
    'serum': 'serum',
    'nail polish': 'nail polish',
    'nail': 'nail polish',
    'makeup kit': 'makeup kit',
    'beauty kit': 'beauty kit',
    'cosmetic kit': 'cosmetic kit',
    'brush': 'makeup brush',
    'sponge': 'makeup sponge',
    'mirror': 'beauty mirror',
    'tweezers': 'tweezers',
    'razor': 'razor',
    'hat': 'hat',
    'cap': 'cap',
    'scarf': 'scarf'
}

# Usage keywords
USAGE_KEYWORDS = {
    'eye makeup': ['eye', 'eyeliner', 'mascara', 'eyeshadow', 'eyebrow'],
    'lip makeup': ['lip', 'lipstick', 'lip gloss', 'lip balm'],
    'face makeup': ['foundation', 'concealer', 'blush', 'powder', 'bronzer'],
    'hair styling': ['hair spray', 'hairspray', 'hair gel', 'hair mousse', 'styling'],
    'hair extension': ['wig', 'hair extension', 'weave', 'lace front'],
    'hair care': ['shampoo', 'conditioner', 'hair mask', 'hair treatment'],
    'skincare': ['moisturizer', 'cleanser', 'serum', 'toner', 'skincare'],
    'fragrance': ['perfume', 'cologne', 'fragrance', 'body spray'],
    'personal care': ['deodorant', 'antiperspirant', 'body wash', 'soap'],
    'nail care': ['nail polish', 'nail', 'manicure', 'pedicure'],
    'beauty accessories': ['brush', 'sponge', 'mirror', 'tweezers', 'accessory']
}


def extract_product_type_from_title(title: str) -> str:
    """
    Trích xuất product type từ title bằng cách tìm keywords.
    
    Args:
        title: Title của sản phẩm
        
    Returns:
        Product type string hoặc None
    """
    if not title:
        return None
    
    title_lower = title.lower()
    
    # Tìm product type keywords
    for keyword, product_type in PRODUCT_TYPE_KEYWORDS.items():
        if keyword in title_lower:
            return product_type
    
    return None


def extract_usage_from_title(title: str, product_type: str = None) -> str:
    """
    Trích xuất usage/intended use từ title và product type.
    
    Args:
        title: Title của sản phẩm
        product_type: Product type đã xác định
        
    Returns:
        Usage string hoặc None
    """
    if not title:
        return None
    
    title_lower = title.lower()
    
    # Nếu có product_type, ưu tiên suy luận từ đó
    if product_type:
        product_lower = product_type.lower()
        if 'hair' in product_lower and ('clip' in product_lower or 'accessory' in product_lower):
            return 'beauty accessories'
        elif 'hair' in product_lower and ('extension' in product_lower or 'wig' in product_lower):
            return 'hair extension'
        elif 'hair' in product_lower and ('spray' in product_lower or 'gel' in product_lower):
            return 'hair styling'
        elif 'eye' in product_lower or 'eyeliner' in product_lower or 'mascara' in product_lower:
            return 'eye makeup'
        elif 'lip' in product_lower or 'lipstick' in product_lower:
            return 'lip makeup'
        elif 'makeup' in product_lower:
            return 'face makeup'
        elif 'nail' in product_lower:
            return 'nail care'
        elif 'skincare' in product_lower:
            return 'skincare'
        elif 'perfume' in product_lower or 'cologne' in product_lower:
            return 'fragrance'
        elif 'deodorant' in product_lower or 'antiperspirant' in product_lower:
            return 'personal care'
    
    # Tìm usage keywords trong title
    for usage, keywords in USAGE_KEYWORDS.items():
        for keyword in keywords:
            if keyword in title_lower:
                return usage
    
    return None


def infer_category_from_product_type(product_type: str, usage: str) -> str:
    """
    Suy luận category cụ thể từ product type và usage.
    
    Args:
        product_type: Product type
        usage: Usage/intended use
        
    Returns:
        Category string
    """
    if not product_type and not usage:
        return "Beauty Products"
    
    # Category mapping dựa trên product type và usage
    category_map = {
        'eyeliner': 'Beauty and Eye Makeup',
        'mascara': 'Beauty and Eye Makeup',
        'eyeshadow': 'Beauty and Eye Makeup',
        'eye makeup': 'Beauty and Eye Makeup',
        'lipstick': 'Beauty and Lip Makeup',
        'lip makeup': 'Beauty and Lip Makeup',
        'foundation': 'Beauty and Face Makeup',
        'concealer': 'Beauty and Face Makeup',
        'blush': 'Beauty and Face Makeup',
        'face makeup': 'Beauty and Face Makeup',
        'wig': 'Beauty and Hair Extensions',
        'hair extension': 'Beauty and Hair Extensions',
        'hair extension': 'Beauty and Hair Extensions',
        'hair clip': 'Beauty and Hair Accessories',
        'hair accessory': 'Beauty and Hair Accessories',
        'hair spray': 'Beauty and Hair Styling',
        'hairspray': 'Beauty and Hair Styling',
        'hair styling': 'Beauty and Hair Styling',
        'makeup kit': 'Beauty and Makeup Kits',
        'beauty kit': 'Beauty and Makeup Kits',
        'cosmetic kit': 'Beauty and Makeup Kits',
        'makeup brush': 'Beauty and Makeup Tools',
        'makeup sponge': 'Beauty and Makeup Tools',
        'beauty accessories': 'Beauty and Makeup Tools',
        'skincare': 'Beauty and Skincare',
        'fragrance': 'Beauty and Fragrance',
        'perfume': 'Beauty and Fragrance',
        'cologne': 'Beauty and Fragrance',
        'nail polish': 'Beauty and Nail Care',
        'nail care': 'Beauty and Nail Care',
        'deodorant': 'Beauty and Personal Care',
        'antiperspirant': 'Beauty and Personal Care',
        'personal care': 'Beauty and Personal Care'
    }
    
    # Thử tìm category từ product type trước
    if product_type:
        for key, category in category_map.items():
            if key in product_type.lower():
                return category
    
    # Thử tìm từ usage
    if usage:
        for key, category in category_map.items():
            if key in usage.lower():
                return category
    
    return "Beauty Products"


def extract_key_attributes(details: dict) -> dict:
    """
    Trích xuất các thuộc tính quan trọng từ raw_metadata.details.
    
    Args:
        details: Dict chứa details từ raw_metadata
        
    Returns:
        Dict chứa các thuộc tính quan trọng
    """
    if not details or not isinstance(details, dict):
        return {}
    
    key_attrs = {}
    
    # Các key quan trọng cần trích xuất
    important_keys = [
        'material', 'material type', 'form', 'product form', 'item form',
        'style', 'design', 'type', 'color', 'colour', 'size',
        'skin type', 'hair type', 'age range', 'target audience'
    ]
    
    details_lower = {k.lower(): v for k, v in details.items()}
    
    for key_pattern in important_keys:
        if key_pattern in details_lower:
            value = details_lower[key_pattern]
            if value and str(value).strip() and str(value).lower() not in ['null', 'none', 'n/a', '']:
                value_str = str(value).strip()
                # Loại bỏ URL, số thuần (có thể là giá), giá trị quá dài
                if not (value_str.startswith('http') or 
                       value_str.startswith('www.') or
                       value_str.replace('.', '').replace(',', '').replace('-', '').isdigit() or
                       len(value_str) > 50):
                    key_attrs[key_pattern] = value_str
    
    return key_attrs


def build_english_embedding_text(row: dict) -> str:
    """
    Xây dựng embedding_text bằng tiếng Anh từ metadata.
    
    Args:
        row: Dict chứa thông tin item (title, main_category, store, raw_metadata)
        
    Returns:
        String embedding_text bằng tiếng Anh
    """
    parts = []
    
    # 1. Title (giữ nguyên, thường đã là tiếng Anh)
    title = row.get('title', '')
    if title and str(title).strip():
        title_str = str(title).strip()
        parts.append(title_str)
    
    # 2. Extract product type từ title
    product_type = extract_product_type_from_title(title)
    
    # 3. Extract usage từ title và product type
    usage = extract_usage_from_title(title, product_type)
    
    # 4. Infer category
    main_category = row.get('main_category', '')
    if main_category and main_category.lower() in CATEGORY_MAPPING:
        category = CATEGORY_MAPPING[main_category.lower()]
    else:
        category = infer_category_from_product_type(product_type, usage)
    
    # 5. Extract key attributes từ raw_metadata
    raw_metadata = row.get('raw_metadata')
    key_attrs = {}
    if raw_metadata:
        if isinstance(raw_metadata, str):
            try:
                metadata = json.loads(raw_metadata)
                details = metadata.get('details', {})
                key_attrs = extract_key_attributes(details)
            except:
                pass
        elif isinstance(raw_metadata, dict):
            details = raw_metadata.get('details', {})
            key_attrs = extract_key_attributes(details)
    
    # 6. Brand/store (chỉ thêm nếu có giá trị phân biệt)
    store = row.get('store')
    brand = None
    if raw_metadata:
        if isinstance(raw_metadata, str):
            try:
                metadata = json.loads(raw_metadata)
                brand = metadata.get('brand') or metadata.get('Brand')
            except:
                pass
        elif isinstance(raw_metadata, dict):
            brand = raw_metadata.get('brand') or raw_metadata.get('Brand')
    
    brand_or_store = brand or store
    if brand_or_store and str(brand_or_store).strip().lower() not in ['null', 'none', 'n/a', 'unknown', '']:
        brand_str = str(brand_or_store).strip()
        # Chỉ thêm brand nếu không đã có trong title
        if brand_str.lower() not in title.lower():
            parts.append(f"by {brand_str}")
    
    # 7. Xây dựng câu mô tả tự nhiên
    description_parts = []
    
    # Xây dựng câu mô tả tự nhiên hơn
    # Bắt đầu với product type và usage nếu có
    if product_type and usage:
        description_parts.append(f"A {product_type} designed for {usage}")
    elif product_type:
        description_parts.append(f"A {product_type} product")
    elif usage:
        description_parts.append(f"A product for {usage}")
    
    # Key attributes thành câu tự nhiên
    if key_attrs:
        attr_descriptions = []
        if 'form' in key_attrs or 'product form' in key_attrs or 'item form' in key_attrs:
            form = key_attrs.get('form') or key_attrs.get('product form') or key_attrs.get('item form')
            if form and form.lower() not in ['various', 'assorted']:
                attr_descriptions.append(f"available in {form} form")
        
        if 'material' in key_attrs or 'material type' in key_attrs:
            material = key_attrs.get('material') or key_attrs.get('material type')
            if material and material.lower() not in ['various', 'assorted']:
                attr_descriptions.append(f"made from {material}")
        
        if 'style' in key_attrs:
            style = key_attrs['style']
            if style and style.lower() not in ['various', 'assorted']:
                attr_descriptions.append(f"featuring {style} style")
        
        if 'color' in key_attrs or 'colour' in key_attrs:
            color = key_attrs.get('color') or key_attrs.get('colour')
            if color and color.lower() not in ['various', 'assorted', 'multi', 'multicolor']:
                attr_descriptions.append(f"available in {color}")
        
        if attr_descriptions:
            description_parts.extend(attr_descriptions)
    
    # Category - chỉ thêm nếu khác "Beauty Products" và chưa có trong description
    if category and category != "Beauty Products":
        if not any(category.lower() in part.lower() for part in description_parts):
            description_parts.append(f"categorized as {category}")
    
    # Kết hợp tất cả thành một đoạn văn tự nhiên
    if description_parts:
        description = ". ".join(description_parts)
        parts.append(description)
    
    # Gộp tất cả thành một đoạn văn
    embedding_text = ". ".join(parts)
    
    # Làm sạch và chuẩn hóa
    embedding_text = re.sub(r'\s+', ' ', embedding_text)  # Loại bỏ khoảng trắng dư
    embedding_text = re.sub(r'\.{2,}', '.', embedding_text)  # Loại bỏ dấu chấm lặp
    embedding_text = embedding_text.strip()
    
    # Đảm bảo độ dài hợp lý (200-400 ký tự, tối đa 500)
    # Nếu quá ngắn (< 100), cố gắng làm phong phú hơn
    if len(embedding_text) < 100:
        # Thêm thông tin cơ bản nếu thiếu
        if not product_type:
            product_type = extract_product_type_from_title(title)
        if not usage:
            usage = extract_usage_from_title(title, product_type)
        if not category or category == "Beauty Products":
            category = infer_category_from_product_type(product_type, usage)
        
        # Xây dựng lại description nếu thiếu
        if not description_parts:
            if product_type and usage:
                description_parts.append(f"A {product_type} designed for {usage}")
            elif product_type:
                description_parts.append(f"A {product_type} product")
            elif usage:
                description_parts.append(f"A product for {usage}")
            else:
                description_parts.append("A beauty product")
            
            if category and category != "Beauty Products":
                description_parts.append(f"categorized as {category}")
        
        # Rebuild embedding_text
        parts = [title] if title else []
        if description_parts:
            parts.append(". ".join(description_parts))
        if brand_or_store and str(brand_or_store).strip().lower() not in ['null', 'none', 'n/a', 'unknown', '']:
            brand_str = str(brand_or_store).strip()
            if brand_str.lower() not in title.lower():
                parts.append(f"by {brand_str}")
        
        embedding_text = ". ".join(parts)
        embedding_text = re.sub(r'\s+', ' ', embedding_text)
        embedding_text = re.sub(r'\.{2,}', '.', embedding_text)
        embedding_text = embedding_text.strip()
    
    # Nếu quá dài (> 500), cắt ngắn
    if len(embedding_text) > 500:
        # Ưu tiên giữ title và phần đầu của description
        if len(parts) > 1:
            result_parts = [parts[0]]  # Giữ title
            remaining_length = 500 - len(parts[0]) - 10  # 10 cho dấu câu và khoảng trắng
            
            if len(parts) > 1:
                desc = parts[1]
                if len(desc) <= remaining_length:
                    result_parts.append(desc)
                else:
                    # Cắt description nhưng giữ phần đầu quan trọng
                    desc_words = desc.split()
                    current_length = 0
                    kept_words = []
                    for word in desc_words:
                        if current_length + len(word) + 1 <= remaining_length - 3:
                            kept_words.append(word)
                            current_length += len(word) + 1
                        else:
                            break
                    if kept_words:
                        result_parts.append(" ".join(kept_words) + "...")
            
            embedding_text = ". ".join(result_parts)
        else:
            embedding_text = embedding_text[:497] + "..."
    
    return embedding_text if embedding_text else ""


def process_items_for_embedding_en(
    input_embedding_file: Path,
    input_metadata_file: Path,
    output_file: Path
):
    """
    Xử lý items để tạo embedding_text tiếng Anh mới.
    
    Args:
        input_embedding_file: File items_for_embedding.parquet (để lấy item_id)
        input_metadata_file: File items_for_rs.parquet (để lấy metadata)
        output_file: File output items_for_embedding_en.parquet
    """
    print("=" * 80)
    print("BUILD ITEMS EMBEDDING TEXT (ENGLISH VERSION)")
    print("=" * 80)
    
    # Đọc file embedding hiện tại để lấy item_id
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
    # Đảm bảo cột item_id tồn tại trong cả hai dataframe
    if 'item_id' not in embedding_df.columns:
        raise ValueError("File embedding phải có cột 'item_id'")
    
    if 'item_id' not in metadata_df.columns:
        # Có thể là parent_asin
        if 'parent_asin' in metadata_df.columns:
            metadata_df = metadata_df.rename({'parent_asin': 'item_id'})
        else:
            raise ValueError("File metadata phải có cột 'item_id' hoặc 'parent_asin'")
    
    # Join
    joined_df = embedding_df.select(['item_id']).join(
        metadata_df,
        on='item_id',
        how='left'
    )
    print(f"[OK] Đã join {len(joined_df):,} items")
    
    # Xử lý từng item để tạo embedding_text tiếng Anh
    print("\nĐang xây dựng embedding_text tiếng Anh cho từng item...")
    
    rows = joined_df.to_dicts()
    results = []
    empty_count = 0
    
    for i, row in enumerate(rows):
        if (i + 1) % 10000 == 0:
            print(f"  Đã xử lý: {i + 1:,}/{len(rows):,} items")
        
        item_id = row.get('item_id')
        if not item_id:
            continue
        
        # Xây dựng embedding_text tiếng Anh
        embedding_text = build_english_embedding_text(row)
        
        if not embedding_text or embedding_text.strip() == "":
            empty_count += 1
            # Nếu không tạo được, giữ nguyên embedding_text cũ
            old_text = embedding_df.filter(pl.col('item_id') == item_id)['embedding_text'].to_list()
            if old_text:
                embedding_text = old_text[0]
            else:
                continue
        
        results.append({
            "item_id": str(item_id),
            "embedding_text": embedding_text
        })
    
    print(f"\n[OK] Đã xử lý xong {len(rows):,} items")
    print(f"  Items có embedding_text mới: {len(results):,}")
    print(f"  Items không có embedding_text: {empty_count:,}")
    
    # Tạo DataFrame mới
    result_df = pl.DataFrame(results)
    
    # Thống kê độ dài embedding_text
    if len(result_df) > 0:
        text_lengths = result_df["embedding_text"].str.len_chars()
        print(f"\nThống kê độ dài embedding_text:")
        print(f"  Trung bình: {text_lengths.mean():.1f} ký tự")
        print(f"  Min: {text_lengths.min()} ký tự")
        print(f"  Max: {text_lengths.max()} ký tự")
        print(f"  Median: {text_lengths.median():.1f} ký tự")
    
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
    print("[OK] HOÀN TẤT: File items_for_embedding_en.parquet đã được tạo thành công!")
    print("=" * 80)
    
    return result_df


def main():
    """Hàm chính để chạy build items embedding English."""
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
    
    input_embedding_file = project_root / "data" / "embedding" / "items_for_embedding.parquet"
    input_metadata_file = project_root / "data" / "processed" / "items_for_rs.parquet"
    output_file = project_root / "data" / "embedding" / "items_for_embedding_en.parquet"
    
    process_items_for_embedding_en(
        input_embedding_file=input_embedding_file,
        input_metadata_file=input_metadata_file,
        output_file=output_file
    )


if __name__ == "__main__":
    main()

