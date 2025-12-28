"""
Build Items Embedding Text
===========================
Mục tiêu: Tạo file items_for_embedding.parquet với item_id và embedding_text
cho mô hình sentence embedding.

Chạy độc lập: python app/data_preprocessing/build_items_embedding.py
"""

import polars as pl
from pathlib import Path
import sys
import io
import json

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


def extract_important_attributes(raw_metadata, exclude_brand=None):
    """
    Trích xuất các thuộc tính quan trọng từ raw_metadata.details.
    
    Args:
        raw_metadata: Dict hoặc JSON string chứa metadata
        exclude_brand: Brand name để loại bỏ khỏi attributes (tránh trùng lặp)
        
    Returns:
        String mô tả các thuộc tính quan trọng bằng tiếng Việt
    """
    if raw_metadata is None:
        return ""
    
    # Parse JSON nếu là string
    if isinstance(raw_metadata, str):
        try:
            metadata = json.loads(raw_metadata)
        except:
            return ""
    elif isinstance(raw_metadata, dict):
        metadata = raw_metadata
    else:
        return ""
    
    # Lấy details từ raw_metadata
    details = metadata.get("details", {})
    if not details or not isinstance(details, dict):
        return ""
    
    # Các key quan trọng cần trích xuất (ưu tiên theo thứ tự)
    # Sử dụng set để tránh trùng lặp (case-insensitive)
    important_key_patterns = [
        "material", "material_type", "material type",
        "product_form", "product form", "form",
        "style", "design",
        "use", "usage", "purpose",
        "target_audience", "target audience", "audience",
        "type", "category",
        "size", "color", "colour",
        "features", "feature",
        "benefits", "benefit"
    ]
    
    # Các key cần loại bỏ (không có ý nghĩa cho embedding)
    exclude_patterns = [
        "asin", "url", "image", "video", "price", "rating", "review", "id",
        "is_discontinued", "discontinued", "manufacturer", "dimensions",
        "weight", "package", "shipping", "warranty", "date", "time",
        "product_dimensions", "item_model_number", "model_number"
    ]
    
    attributes = []
    processed_keys = set()  # Track các key đã xử lý (lowercase)
    
    # Tạo mapping lowercase -> original key để giữ nguyên format
    details_lower_map = {k.lower(): k for k in details.keys()}
    details_lower = {k.lower(): v for k, v in details.items()}
    
    # Ưu tiên các key quan trọng
    for pattern in important_key_patterns:
        if pattern in details_lower and pattern not in processed_keys:
            value = details_lower[pattern]
            original_key = details_lower_map[pattern]
            
            # Loại bỏ Brand nếu đã có trong Thương hiệu
            if pattern == "brand" and exclude_brand:
                value_str = str(value).strip()
                if value_str.lower() == exclude_brand.lower():
                    processed_keys.add(pattern)
                    continue
            
            if _is_valid_attribute_value(value):
                attributes.append(f"{original_key}: {value}")
                processed_keys.add(pattern)
    
    # Nếu chưa đủ, lấy thêm các key khác (trừ các key không cần thiết)
    if len(attributes) < 5:
        for key, value in details.items():
            key_lower = key.lower()
            
            # Bỏ qua nếu đã xử lý hoặc trong exclude list
            if key_lower in processed_keys:
                continue
            if any(exclude in key_lower for exclude in exclude_patterns):
                continue
            
            # Loại bỏ Brand nếu đã có trong Thương hiệu
            if (key_lower == "brand" or "brand" in key_lower) and exclude_brand:
                value_str = str(value).strip()
                if value_str.lower() == exclude_brand.lower():
                    processed_keys.add(key_lower)
                    continue
            
            # Bỏ qua các giá trị boolean không có ý nghĩa
            if isinstance(value, bool) and not value:
                continue
            if isinstance(value, str) and value.lower() in ["no", "false", "none", "n/a"]:
                continue
            
            if _is_valid_attribute_value(value):
                attributes.append(f"{key}: {value}")
                processed_keys.add(key_lower)
                if len(attributes) >= 8:  # Giới hạn số lượng thuộc tính
                    break
    
    if attributes:
        # Giới hạn số lượng và độ dài (tối đa 200 ký tự cho phần attributes)
        max_attrs = 6
        max_length = 200
        
        if len(attributes) > max_attrs:
            attributes = attributes[:max_attrs]
        
        attr_text = ". ".join(attributes)
        
        # Cắt nếu quá dài
        if len(attr_text) > max_length:
            # Cắt từng attribute cho đến khi đủ độ dài
            result = []
            current_length = 0
            for attr in attributes:
                if current_length + len(attr) + 2 <= max_length - 3:  # 2 cho ". ", 3 cho "..."
                    result.append(attr)
                    current_length += len(attr) + 2
                else:
                    break
            if result:
                attr_text = ". ".join(result) + "..."
            else:
                attr_text = attributes[0][:max_length-3] + "..."
        
        return attr_text
    return ""


def _is_valid_attribute_value(value):
    """
    Kiểm tra xem giá trị có hợp lệ để đưa vào embedding_text không.
    
    Args:
        value: Giá trị cần kiểm tra
        
    Returns:
        True nếu hợp lệ, False nếu không
    """
    if value is None:
        return False
    
    value_str = str(value).strip()
    
    # Bỏ qua các giá trị rỗng hoặc không có ý nghĩa
    if not value_str or value_str.lower() in ["null", "none", "n/a", "", "unknown"]:
        return False
    
    # Bỏ qua URL
    if value_str.startswith("http") or value_str.startswith("www."):
        return False
    
    # Bỏ qua các chuỗi chỉ chứa số (có thể là giá tiền hoặc ID)
    if value_str.replace(".", "").replace(",", "").replace("-", "").replace(" ", "").isdigit():
        return False
    
    # Bỏ qua các giá trị quá dài (có thể là mô tả dài hoặc JSON)
    if len(value_str) > 100:
        return False
    
    return True


def build_embedding_text(row):
    """
    Xây dựng embedding_text từ các thông tin của item.
    
    Args:
        row: Row từ DataFrame
        
    Returns:
        String embedding_text
    """
    parts = []
    
    # 1. Tên sản phẩm (title)
    title = row.get("title")
    if title and str(title).strip() and str(title).lower() not in ["null", "none", "n/a", "unknown"]:
        title_str = str(title).strip()
        parts.append(f"Tên sản phẩm: {title_str}")
    
    # 2. Danh mục chính (main_category)
    main_category = row.get("main_category")
    if main_category and str(main_category).strip() and str(main_category).lower() not in ["null", "none", "n/a", "unknown"]:
        category_str = str(main_category).strip()
        parts.append(f"Danh mục: {category_str}")
    
    # 3. Thương hiệu hoặc store
    store = row.get("store")
    brand = None
    
    # Thử lấy brand từ raw_metadata nếu có
    raw_metadata = row.get("raw_metadata")
    if raw_metadata:
        if isinstance(raw_metadata, str):
            try:
                metadata = json.loads(raw_metadata)
                brand = metadata.get("brand") or metadata.get("Brand")
            except:
                pass
        elif isinstance(raw_metadata, dict):
            brand = raw_metadata.get("brand") or raw_metadata.get("Brand")
    
    brand_or_store = brand or store
    brand_str = None
    if brand_or_store and str(brand_or_store).strip() and str(brand_or_store).lower() not in ["null", "none", "n/a", "unknown"]:
        brand_str = str(brand_or_store).strip()
        parts.append(f"Thương hiệu: {brand_str}")
    
    # 4. Thuộc tính chính từ raw_metadata.details (loại bỏ Brand nếu đã có trong Thương hiệu)
    attributes = extract_important_attributes(raw_metadata, exclude_brand=brand_str)
    if attributes:
        parts.append(f"Thuộc tính chính: {attributes}")
    
    # Kết hợp các phần thành một đoạn văn bản
    if not parts:
        return ""
    
    embedding_text = ". ".join(parts)
    
    # Đảm bảo độ dài hợp lý (200-400 ký tự, tối đa 500)
    if len(embedding_text) > 500:
        # Cắt bớt từ phần cuối (ưu tiên giữ title và category)
        if len(parts) > 3:
            # Tính độ dài của các phần trước attributes
            prefix = ". ".join(parts[:-1]) + ". "
            max_attr_len = 500 - len(prefix) - 3  # 3 cho "..."
            
            if max_attr_len > 20:  # Đảm bảo còn đủ chỗ cho attributes
                parts[-1] = parts[-1][:max_attr_len] + "..."
                embedding_text = ". ".join(parts)
            else:
                # Nếu không đủ chỗ, bỏ phần attributes
                embedding_text = ". ".join(parts[:-1])
        else:
            # Nếu chỉ có ít phần, cắt trực tiếp
            embedding_text = embedding_text[:497] + "..."
    
    # Đảm bảo không vượt quá 500 ký tự
    if len(embedding_text) > 500:
        embedding_text = embedding_text[:497] + "..."
    
    return embedding_text


def process_items_for_embedding(input_path: Path, output_path: Path):
    """
    Xử lý items_for_rs.parquet để tạo items_for_embedding.parquet.
    
    Args:
        input_path: Đường dẫn đến file items_for_rs.parquet
        output_path: Đường dẫn đến file output items_for_embedding.parquet
    """
    print("=" * 80)
    print("BUILD ITEMS EMBEDDING TEXT")
    print("=" * 80)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {input_path}")
    
    print(f"\nĐang đọc file: {input_path}")
    df = pl.read_parquet(str(input_path))
    print(f"[OK] Đã đọc {len(df):,} items")
    print(f"Columns: {df.columns}")
    
    # Xử lý từng row để tạo embedding_text
    print("\nĐang xây dựng embedding_text cho từng item...")
    
    # Convert sang dict để xử lý
    rows = df.to_dicts()
    
    results = []
    empty_count = 0
    
    for i, row in enumerate(rows):
        if (i + 1) % 10000 == 0:
            print(f"  Đã xử lý: {i + 1:,}/{len(rows):,} items")
        
        # Lấy item_id từ parent_asin hoặc item_id
        item_id = row.get("item_id") or row.get("parent_asin")
        if not item_id:
            continue
        
        # Xây dựng embedding_text
        embedding_text = build_embedding_text(row)
        
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
    
    # Thống kê độ dài embedding_text
    if len(result_df) > 0:
        text_lengths = result_df["embedding_text"].str.len_chars()
        print(f"\nThống kê độ dài embedding_text:")
        print(f"  Trung bình: {text_lengths.mean():.1f} ký tự")
        print(f"  Min: {text_lengths.min()} ký tự")
        print(f"  Max: {text_lengths.max()} ký tự")
        print(f"  Median: {text_lengths.median():.1f} ký tự")
    
    # Lưu ra file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nĐang lưu file: {output_path}")
    result_df.write_parquet(str(output_path))
    print(f"[OK] Đã lưu {len(result_df):,} items vào {output_path}")
    
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
    print("[OK] HOÀN TẤT: File items_for_embedding.parquet đã được tạo thành công!")
    print("=" * 80)
    
    return result_df


def main():
    """Hàm chính để chạy build items embedding."""
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
    
    input_path = project_root / "data" / "processed" / "items_for_rs.parquet"
    output_path = project_root / "data" / "embedding" / "items_for_embedding.parquet"
    
    process_items_for_embedding(input_path, output_path)


if __name__ == "__main__":
    main()

