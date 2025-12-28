"""
Build Items Embedding Text (English Version V2 - Natural Language)
===================================================================
Mục tiêu: Tinh chỉnh embedding_text để tự nhiên hơn, tập trung vào công dụng
và vai trò của sản phẩm thay vì liệt kê kỹ thuật, nhằm cải thiện semantic similarity.

Input: 
  - data/embedding/items_for_embedding_en.parquet (item_id, embedding_text hiện tại)
  - data/processed/items_for_rs.parquet (metadata gốc)

Output: data/embedding/items_for_embedding_en_v2.parquet (item_id, embedding_text tự nhiên)

Chạy độc lập: python app/data_preprocessing/build_items_embedding_en_v2.py
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


# Product type mapping để nhận diện từ title
PRODUCT_TYPE_PATTERNS = {
    'eyeliner': ['eyeliner'],
    'mascara': ['mascara'],
    'lipstick': ['lipstick', 'lip stick'],
    'foundation': ['foundation'],
    'concealer': ['concealer'],
    'blush': ['blush'],
    'eyeshadow': ['eyeshadow', 'eye shadow'],
    'wig': ['wig', 'wigs'],
    'hair extension': ['hair extension', 'hair extensions', 'weave', 'lace front'],
    'hair clip': ['hair clip', 'hair clips', 'hair claw', 'hair barrette'],
    'hairspray': ['hairspray', 'hair spray', 'hairspray'],
    'hair gel': ['hair gel', 'styling gel'],
    'makeup kit': ['makeup kit', 'cosmetic kit', 'beauty kit'],
    'makeup brush': ['makeup brush', 'brush set'],
    'makeup sponge': ['makeup sponge', 'beauty sponge'],
    'nail polish': ['nail polish', 'nail varnish'],
    'perfume': ['perfume'],
    'cologne': ['cologne'],
    'deodorant': ['deodorant', 'deodarant'],
    'antiperspirant': ['antiperspirant', 'anti-perspirant'],
    'shampoo': ['shampoo'],
    'conditioner': ['conditioner'],
    'moisturizer': ['moisturizer', 'moisturiser'],
    'cleanser': ['cleanser'],
    'serum': ['serum'],
    'hat': ['hat', 'cap'],
    'scarf': ['scarf'],
    'beauty accessory': ['accessory', 'accessories', 'tweezers', 'mirror']
}

# Usage mapping
USAGE_MAPPING = {
    'eyeliner': 'eye makeup application',
    'mascara': 'eye makeup application',
    'eyeshadow': 'eye makeup application',
    'lipstick': 'lip makeup application',
    'foundation': 'face makeup application',
    'concealer': 'face makeup application',
    'blush': 'face makeup application',
    'wig': 'hair extension and styling',
    'hair extension': 'hair extension and styling',
    'hair clip': 'hair styling and accessories',
    'hairspray': 'hair styling and hold',
    'hair gel': 'hair styling and hold',
    'makeup kit': 'cosmetic application',
    'makeup brush': 'makeup application tools',
    'makeup sponge': 'makeup application tools',
    'nail polish': 'nail care and decoration',
    'perfume': 'fragrance and personal scent',
    'cologne': 'fragrance and personal scent',
    'deodorant': 'personal hygiene and odor control',
    'antiperspirant': 'personal hygiene and sweat control',
    'shampoo': 'hair care and cleansing',
    'conditioner': 'hair care and conditioning',
    'moisturizer': 'skincare and hydration',
    'cleanser': 'skincare and cleansing',
    'serum': 'skincare treatment',
    'hat': 'fashion and sun protection',
    'scarf': 'fashion accessory',
    'beauty accessory': 'beauty and grooming tools'
}

# Category mapping
CATEGORY_MAPPING = {
    'eye makeup': 'Beauty and Eye Makeup',
    'lip makeup': 'Beauty and Lip Makeup',
    'face makeup': 'Beauty and Face Makeup',
    'hair extension': 'Beauty and Hair Extensions',
    'hair styling': 'Beauty and Hair Styling',
    'hair accessories': 'Beauty and Hair Accessories',
    'makeup kits': 'Beauty and Makeup Kits',
    'makeup tools': 'Beauty and Makeup Tools',
    'nail care': 'Beauty and Nail Care',
    'fragrance': 'Beauty and Fragrance',
    'skincare': 'Beauty and Skincare',
    'hair care': 'Beauty and Hair Care',
    'personal care': 'Beauty and Personal Care',
    'fashion accessories': 'Beauty and Fashion Accessories'
}


def identify_product_type(title: str) -> tuple:
    """
    Xác định product type và usage từ title.
    
    Returns:
        Tuple (product_type, usage)
    """
    if not title:
        return None, None
    
    title_lower = title.lower()
    
    # Tìm product type
    product_type = None
    for pt, patterns in PRODUCT_TYPE_PATTERNS.items():
        for pattern in patterns:
            if pattern in title_lower:
                product_type = pt
                break
        if product_type:
            break
    
    # Xác định usage từ product type
    usage = None
    if product_type:
        usage = USAGE_MAPPING.get(product_type)
    
    # Nếu không tìm được từ product type, thử suy luận từ title
    if not usage:
        if any(word in title_lower for word in ['eye', 'eyeliner', 'mascara', 'eyeshadow']):
            usage = 'eye makeup application'
        elif any(word in title_lower for word in ['lip', 'lipstick']):
            usage = 'lip makeup application'
        elif any(word in title_lower for word in ['hair', 'wig', 'extension']):
            if 'wig' in title_lower or 'extension' in title_lower:
                usage = 'hair extension and styling'
            elif 'clip' in title_lower or 'accessory' in title_lower:
                usage = 'hair styling and accessories'
            else:
                usage = 'hair styling and hold'
        elif any(word in title_lower for word in ['makeup', 'cosmetic']):
            usage = 'cosmetic application'
        elif any(word in title_lower for word in ['perfume', 'cologne', 'fragrance']):
            usage = 'fragrance and personal scent'
        elif any(word in title_lower for word in ['deodorant', 'antiperspirant']):
            usage = 'personal hygiene and odor control'
        elif any(word in title_lower for word in ['shampoo', 'conditioner']):
            usage = 'hair care and cleansing'
        elif any(word in title_lower for word in ['moisturizer', 'cleanser', 'serum']):
            usage = 'skincare and hydration'
    
    return product_type, usage


def infer_category(product_type: str, usage: str) -> str:
    """
    Suy luận category từ product type và usage.
    """
    if not product_type and not usage:
        return 'Beauty Products'
    
    # Mapping từ usage
    if usage:
        if 'eye makeup' in usage:
            return CATEGORY_MAPPING['eye makeup']
        elif 'lip makeup' in usage:
            return CATEGORY_MAPPING['lip makeup']
        elif 'face makeup' in usage or 'cosmetic application' in usage:
            return CATEGORY_MAPPING['face makeup']
        elif 'hair extension' in usage:
            return CATEGORY_MAPPING['hair extension']
        elif 'hair styling' in usage:
            if 'accessories' in usage:
                return CATEGORY_MAPPING['hair accessories']
            return CATEGORY_MAPPING['hair styling']
        elif 'makeup application tools' in usage:
            return CATEGORY_MAPPING['makeup tools']
        elif 'nail care' in usage:
            return CATEGORY_MAPPING['nail care']
        elif 'fragrance' in usage:
            return CATEGORY_MAPPING['fragrance']
        elif 'skincare' in usage:
            return CATEGORY_MAPPING['skincare']
        elif 'hair care' in usage:
            return CATEGORY_MAPPING['hair care']
        elif 'personal hygiene' in usage:
            return CATEGORY_MAPPING['personal care']
        elif 'fashion' in usage:
            return CATEGORY_MAPPING['fashion accessories']
    
    # Mapping từ product type
    if product_type:
        if product_type in ['eyeliner', 'mascara', 'eyeshadow']:
            return CATEGORY_MAPPING['eye makeup']
        elif product_type == 'lipstick':
            return CATEGORY_MAPPING['lip makeup']
        elif product_type in ['foundation', 'concealer', 'blush']:
            return CATEGORY_MAPPING['face makeup']
        elif product_type in ['wig', 'hair extension']:
            return CATEGORY_MAPPING['hair extension']
        elif product_type in ['hair clip', 'hairspray', 'hair gel']:
            return CATEGORY_MAPPING['hair styling']
        elif product_type == 'makeup kit':
            return CATEGORY_MAPPING['makeup kits']
        elif product_type in ['makeup brush', 'makeup sponge']:
            return CATEGORY_MAPPING['makeup tools']
        elif product_type == 'nail polish':
            return CATEGORY_MAPPING['nail care']
        elif product_type in ['perfume', 'cologne']:
            return CATEGORY_MAPPING['fragrance']
        elif product_type in ['moisturizer', 'cleanser', 'serum']:
            return CATEGORY_MAPPING['skincare']
        elif product_type in ['shampoo', 'conditioner']:
            return CATEGORY_MAPPING['hair care']
        elif product_type in ['deodorant', 'antiperspirant']:
            return CATEGORY_MAPPING['personal care']
    
    return 'Beauty Products'


def extract_relevant_attributes(details: dict) -> dict:
    """
    Trích xuất các thuộc tính quan trọng và có giá trị phân biệt.
    """
    if not details or not isinstance(details, dict):
        return {}
    
    relevant_attrs = {}
    details_lower = {k.lower(): v for k, v in details.items()}
    
    # Chỉ lấy các thuộc tính giúp phân biệt sản phẩm
    important_keys = ['material', 'material type', 'form', 'product form', 'color', 'colour', 'style']
    
    for key in important_keys:
        if key in details_lower:
            value = details_lower[key]
            if value and str(value).strip():
                value_str = str(value).strip()
                # Loại bỏ giá trị không có ý nghĩa
                if (value_str.lower() not in ['null', 'none', 'n/a', '', 'various', 'assorted', 'multi'] and
                    not value_str.startswith('http') and
                    not value_str.replace('.', '').replace(',', '').replace('-', '').isdigit() and
                    len(value_str) <= 30):
                    relevant_attrs[key] = value_str
    
    return relevant_attrs


def build_natural_embedding_text(row: dict) -> str:
    """
    Xây dựng embedding_text tự nhiên, tập trung vào công dụng và vai trò.
    """
    title = row.get('title', '')
    if not title or not str(title).strip():
        return ""
    
    title_str = str(title).strip()
    
    # Xác định product type và usage
    product_type, usage = identify_product_type(title_str)
    
    # Suy luận category
    category = infer_category(product_type, usage)
    
    # Extract attributes từ metadata
    raw_metadata = row.get('raw_metadata')
    relevant_attrs = {}
    if raw_metadata:
        if isinstance(raw_metadata, str):
            try:
                metadata = json.loads(raw_metadata)
                details = metadata.get('details', {})
                relevant_attrs = extract_relevant_attributes(details)
            except:
                pass
        elif isinstance(raw_metadata, dict):
            details = raw_metadata.get('details', {})
            relevant_attrs = extract_relevant_attributes(details)
    
    # Brand/store
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
    
    # Xây dựng câu văn tự nhiên
    sentences = []
    
    # Câu đầu: Giới thiệu sản phẩm với product type và usage
    if product_type and usage:
        # Viết tự nhiên hơn
        if 'makeup' in usage or 'cosmetic' in usage:
            intro = f"This {product_type} is designed for {usage},"
        elif 'hair' in usage:
            intro = f"This {product_type} is used for {usage},"
        elif 'fragrance' in usage or 'personal' in usage:
            intro = f"This {product_type} is intended for {usage},"
        else:
            intro = f"This {product_type} serves for {usage},"
        sentences.append(intro)
    elif product_type:
        sentences.append(f"This {product_type} product")
    elif usage:
        sentences.append(f"A product designed for {usage}")
    else:
        sentences.append("A beauty product")
    
    # Thêm các thuộc tính quan trọng một cách tự nhiên
    if relevant_attrs:
        attr_sentences = []
        
        if 'material' in relevant_attrs or 'material type' in relevant_attrs:
            material = relevant_attrs.get('material') or relevant_attrs.get('material type')
            if material:
                attr_sentences.append(f"made from {material}")
        
        if 'form' in relevant_attrs or 'product form' in relevant_attrs:
            form = relevant_attrs.get('form') or relevant_attrs.get('product form')
            if form:
                attr_sentences.append(f"available as {form}")
        
        if 'style' in relevant_attrs:
            style = relevant_attrs['style']
            if style:
                attr_sentences.append(f"featuring {style} styling")
        
        if 'color' in relevant_attrs or 'colour' in relevant_attrs:
            color = relevant_attrs.get('color') or relevant_attrs.get('colour')
            if color:
                attr_sentences.append(f"in {color}")
        
        if attr_sentences:
            sentences.append(" ".join(attr_sentences))
    
    # Thêm category nếu khác "Beauty Products"
    if category and category != "Beauty Products":
        sentences.append(f"categorized under {category}")
    
    # Thêm brand nếu có giá trị nhận diện và chưa có trong title
    if brand_or_store and str(brand_or_store).strip():
        brand_str = str(brand_or_store).strip()
        if brand_str.lower() not in ['null', 'none', 'n/a', 'unknown', '']:
            if brand_str.lower() not in title_str.lower():
                sentences.append(f"from {brand_str}")
    
    # Kết hợp thành một đoạn văn tự nhiên
    embedding_text = " ".join(sentences)
    
    # Đảm bảo bắt đầu với title nếu có
    if title_str and not embedding_text.startswith(title_str):
        embedding_text = f"{title_str}. {embedding_text}"
    
    # Làm sạch
    embedding_text = re.sub(r'\s+', ' ', embedding_text)
    embedding_text = re.sub(r'\.{2,}', '.', embedding_text)
    embedding_text = re.sub(r',\s*,', ',', embedding_text)
    embedding_text = embedding_text.strip()
    
    # Đảm bảo độ dài hợp lý (200-400 ký tự, tối đa 500)
    if len(embedding_text) < 100:
        # Nếu quá ngắn, thêm thông tin cơ bản
        if not product_type:
            product_type, usage = identify_product_type(title_str)
        if product_type and usage:
            additional = f" This {product_type} is designed for {usage}."
            embedding_text = embedding_text + additional
        elif product_type:
            additional = f" This is a {product_type} product."
            embedding_text = embedding_text + additional
        
        if len(embedding_text) < 100:
            if category and category != "Beauty Products":
                embedding_text = embedding_text + f" It belongs to {category}."
            else:
                embedding_text = embedding_text + " It is a beauty product."
    
    if len(embedding_text) > 500:
        # Cắt ngắn nhưng giữ phần quan trọng
        if len(sentences) > 1:
            # Giữ title và câu đầu tiên
            result = [sentences[0]]
            remaining = 500 - len(sentences[0]) - len(title_str) - 20
            
            for sent in sentences[1:]:
                if len(sent) + len(result[-1]) <= remaining:
                    result.append(sent)
                else:
                    break
            
            if title_str and not result[0].startswith(title_str):
                embedding_text = f"{title_str}. {' '.join(result)}"
            else:
                embedding_text = " ".join(result)
            
            if len(embedding_text) > 500:
                embedding_text = embedding_text[:497] + "..."
        else:
            embedding_text = embedding_text[:497] + "..."
    
    return embedding_text if embedding_text else ""


def process_items_for_embedding_en_v2(
    input_embedding_file: Path,
    input_metadata_file: Path,
    output_file: Path
):
    """
    Xử lý items để tạo embedding_text tự nhiên hơn.
    """
    print("=" * 80)
    print("BUILD ITEMS EMBEDDING TEXT (ENGLISH VERSION V2 - NATURAL LANGUAGE)")
    print("=" * 80)
    
    # Đọc file embedding hiện tại
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
    print("\nĐang viết lại embedding_text tự nhiên cho từng item...")
    
    rows = joined_df.to_dicts()
    results = []
    empty_count = 0
    
    for i, row in enumerate(rows):
        if (i + 1) % 10000 == 0:
            print(f"  Đã xử lý: {i + 1:,}/{len(rows):,} items")
        
        item_id = row.get('item_id')
        if not item_id:
            continue
        
        # Xây dựng embedding_text tự nhiên
        embedding_text = build_natural_embedding_text(row)
        
        if not embedding_text or embedding_text.strip() == "":
            empty_count += 1
            # Fallback: giữ nguyên embedding_text cũ
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
    
    # Thống kê độ dài
    if len(result_df) > 0:
        text_lengths = result_df["embedding_text"].str.len_chars()
        print(f"\nThống kê độ dài embedding_text:")
        print(f"  Trung bình: {text_lengths.mean():.1f} ký tự")
        print(f"  Min: {text_lengths.min()} ký tự")
        print(f"  Max: {text_lengths.max()} ký tự")
        print(f"  Median: {text_lengths.median():.1f} ký tự")
        
        # Thống kê số items trong khoảng 200-400
        in_range = ((text_lengths >= 200) & (text_lengths <= 400)).sum()
        print(f"  Items trong khoảng 200-400 ký tự: {in_range:,} ({in_range/len(result_df)*100:.1f}%)")
    
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
    print("[OK] HOÀN TẤT: File items_for_embedding_en_v2.parquet đã được tạo thành công!")
    print("=" * 80)
    
    return result_df


def main():
    """Hàm chính để chạy build items embedding English V2."""
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
    
    input_embedding_file = project_root / "data" / "embedding" / "items_for_embedding_en.parquet"
    input_metadata_file = project_root / "data" / "processed" / "items_for_rs.parquet"
    output_file = project_root / "data" / "embedding" / "items_for_embedding_en_v2.parquet"
    
    process_items_for_embedding_en_v2(
        input_embedding_file=input_embedding_file,
        input_metadata_file=input_metadata_file,
        output_file=output_file
    )


if __name__ == "__main__":
    main()

