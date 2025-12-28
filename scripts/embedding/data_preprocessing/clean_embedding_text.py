"""
Clean and Normalize Embedding Text
===================================
Mục tiêu: Làm sạch và chuẩn hóa embedding_text trong items_for_embedding.parquet
để đảm bảo văn bản gọn, đúng ngữ nghĩa, ít nhiễu.

Chạy độc lập: python app/data_preprocessing/clean_embedding_text.py
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


def remove_redundant_punctuation(text):
    """
    Loại bỏ dấu câu lặp liên tiếp và ký tự đặc biệt dư thừa.
    """
    # Loại bỏ dấu chấm lặp (giữ lại tối đa 1)
    text = re.sub(r'\.{2,}', '.', text)
    
    # Loại bỏ dấu phẩy lặp (giữ lại tối đa 1)
    text = re.sub(r',{2,}', ',', text)
    
    # Loại bỏ dấu hai chấm lặp
    text = re.sub(r':{2,}', ':', text)
    
    # Loại bỏ dấu chấm phẩy lặp
    text = re.sub(r';{2,}', ';', text)
    
    # Loại bỏ khoảng trắng trước dấu câu
    text = re.sub(r'\s+([.,:;])', r'\1', text)
    
    # Đảm bảo có khoảng trắng sau dấu chấm (trừ khi là số như "3.14" hoặc kết thúc câu)
    # Xử lý trường hợp: "text.text" -> "text. text"
    # Không áp dụng cho số thập phân (ví dụ: "3.14", "oz")
    # Pattern: dấu chấm không theo sau bởi số hoặc khoảng trắng, và theo sau bởi chữ cái
    text = re.sub(r'\.([A-Za-zÀ-ỹ])', r'. \1', text)
    
    # Đảm bảo có khoảng trắng sau dấu hai chấm (nếu thiếu)
    text = re.sub(r':([A-Za-zÀ-ỹ])', r': \1', text)
    
    return text


def normalize_whitespace(text):
    """
    Chuẩn hóa khoảng trắng: loại bỏ khoảng trắng lặp, trim đầu cuối.
    """
    # Loại bỏ khoảng trắng lặp (giữ lại 1)
    text = re.sub(r'\s+', ' ', text)
    
    # Loại bỏ khoảng trắng đầu cuối
    text = text.strip()
    
    return text


def capitalize_sentences(text):
    """
    Viết hoa đầu câu và sau dấu chấm.
    """
    if not text:
        return text
    
    # Viết hoa ký tự đầu tiên
    if len(text) > 0:
        text = text[0].upper() + text[1:] if len(text) > 1 else text.upper()
    
    # Viết hoa sau dấu chấm (nếu có khoảng trắng)
    # Bao gồm cả chữ cái tiếng Việt và tiếng Anh
    def capitalize_after_dot(match):
        return '. ' + match.group(1).upper()
    
    # Pattern cho chữ cái thường (tiếng Anh và tiếng Việt)
    text = re.sub(r'\.\s+([a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ])', 
                  capitalize_after_dot, text)
    
    # Đảm bảo các nhãn như "Tên sản phẩm:", "Danh mục:", "Thương hiệu:" được viết hoa đúng
    text = re.sub(r'(tên sản phẩm:)', 'Tên sản phẩm:', text, flags=re.IGNORECASE)
    text = re.sub(r'(danh mục:)', 'Danh mục:', text, flags=re.IGNORECASE)
    text = re.sub(r'(thương hiệu:)', 'Thương hiệu:', text, flags=re.IGNORECASE)
    text = re.sub(r'(thuộc tính chính:)', 'Thuộc tính chính:', text, flags=re.IGNORECASE)
    
    return text


def remove_generic_phrases(text):
    """
    Loại bỏ các cụm từ quá chung chung hoặc mang tính quảng cáo.
    """
    # Danh sách các cụm từ cần loại bỏ
    generic_phrases = [
        r'chất lượng cao',
        r'sản phẩm tuyệt vời',
        r'uy tín',
        r'đáng tin cậy',
        r'giá rẻ',
        r'giá tốt',
        r'khuyến mãi',
        r'giảm giá',
        r'hot deal',
        r'best seller',
        r'bán chạy',
        r'phổ biến',
        r'được yêu thích',
        r'được đánh giá cao',
        r'5 sao',
        r'★★★★★',
    ]
    
    for phrase in generic_phrases:
        # Loại bỏ cụm từ và dấu câu xung quanh
        pattern = r'[.,:;]?\s*' + phrase + r'\s*[.,:;]?'
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    
    return text


def merge_redundant_info(text):
    """
    Gộp hoặc rút gọn các cụm từ trùng nghĩa.
    """
    # Gộp các thuộc tính trùng lặp (ví dụ: "Brand: X. brand: X" -> "Brand: X")
    # Đã xử lý ở bước trước, nhưng có thể còn một số trường hợp
    
    # Loại bỏ các phần trùng lặp gần nhau
    sentences = text.split('. ')
    seen = set()
    unique_sentences = []
    
    for sentence in sentences:
        sentence_lower = sentence.lower().strip()
        # Kiểm tra xem có trùng với câu đã có không (kiểm tra phần key)
        if ':' in sentence:
            key = sentence.split(':')[0].strip().lower()
            if key not in seen:
                seen.add(key)
                unique_sentences.append(sentence)
        else:
            # Nếu không có dấu hai chấm, kiểm tra toàn bộ câu
            if sentence_lower not in seen:
                seen.add(sentence_lower)
                unique_sentences.append(sentence)
    
    return '. '.join(unique_sentences)


def clean_attributes_section(text):
    """
    Làm sạch phần thuộc tính chính, gộp các thuộc tính rời rạc thành câu tự nhiên hơn.
    """
    if 'Thuộc tính chính:' not in text:
        return text
    
    # Tách phần thuộc tính
    parts = text.split('Thuộc tính chính:')
    if len(parts) < 2:
        return text
    
    prefix = parts[0]
    attributes_part = parts[1]
    
    # Tách các thuộc tính
    attributes = [attr.strip() for attr in attributes_part.split('.') if attr.strip()]
    
    if not attributes:
        return prefix.rstrip()
    
    # Làm sạch từng thuộc tính
    cleaned_attrs = []
    seen_keys = set()
    
    for attr in attributes:
        if ':' in attr:
            key, value = attr.split(':', 1)
            key = key.strip()
            value = value.strip()
            
            # Loại bỏ thuộc tính trùng lặp (theo key)
            key_lower = key.lower()
            if key_lower in seen_keys:
                continue
            
            # Loại bỏ giá trị rỗng hoặc không có ý nghĩa
            if not value or value.lower() in ['null', 'none', 'n/a', 'unknown', '']:
                continue
            
            # Loại bỏ các thuộc tính không quan trọng
            if any(exclude in key_lower for exclude in ['unit', 'count', 'dimension', 'weight', 'package']):
                continue
            
            seen_keys.add(key_lower)
            cleaned_attrs.append(f"{key}: {value}")
        else:
            # Nếu không có dấu hai chấm, giữ nguyên nếu có ý nghĩa
            if attr and len(attr) > 3:
                cleaned_attrs.append(attr)
    
    # Gộp lại thành câu tự nhiên
    if cleaned_attrs:
        # Giới hạn số lượng thuộc tính (tối đa 5)
        if len(cleaned_attrs) > 5:
            cleaned_attrs = cleaned_attrs[:5]
        
        attributes_text = '. '.join(cleaned_attrs)
        
        # Đảm bảo không quá dài (tối đa 150 ký tự cho phần attributes)
        if len(attributes_text) > 150:
            # Cắt từng thuộc tính cho đến khi đủ độ dài
            result = []
            current_length = 0
            for attr in cleaned_attrs:
                if current_length + len(attr) + 2 <= 147:  # 2 cho ". ", 3 cho "..."
                    result.append(attr)
                    current_length += len(attr) + 2
                else:
                    break
            if result:
                attributes_text = '. '.join(result) + '...'
            else:
                attributes_text = cleaned_attrs[0][:147] + '...'
        
        return prefix.rstrip() + f'Thuộc tính chính: {attributes_text}'
    else:
        # Nếu không còn thuộc tính nào, bỏ phần này
        return prefix.rstrip()


def truncate_if_needed(text, max_length=500):
    """
    Cắt ngắn văn bản nếu quá dài, ưu tiên giữ lại thông tin quan trọng.
    """
    if len(text) <= max_length:
        return text
    
    # Ưu tiên giữ lại các phần quan trọng
    parts = text.split('. ')
    important_parts = []
    other_parts = []
    
    for part in parts:
        part_lower = part.lower()
        if any(keyword in part_lower for keyword in ['tên sản phẩm:', 'danh mục:', 'thương hiệu:', 'thuộc tính chính:']):
            important_parts.append(part)
        else:
            other_parts.append(part)
    
    # Gộp lại theo thứ tự ưu tiên
    result_parts = important_parts + other_parts
    
    # Cắt từng phần cho đến khi đủ độ dài
    final_parts = []
    current_length = 0
    
    for part in result_parts:
        part_with_sep = part + '. '
        if current_length + len(part_with_sep) <= max_length - 3:  # 3 cho "..."
            final_parts.append(part)
            current_length += len(part_with_sep)
        else:
            # Nếu là phần quan trọng và chưa có phần nào, cắt phần đó
            if not final_parts:
                final_parts.append(part[:max_length-3] + '...')
            else:
                break
    
    result = '. '.join(final_parts)
    
    # Đảm bảo không vượt quá max_length
    if len(result) > max_length:
        result = result[:max_length-3] + '...'
    
    return result


def clean_embedding_text(text):
    """
    Hàm chính để làm sạch và chuẩn hóa embedding_text.
    """
    if not text or not isinstance(text, str):
        return ""
    
    # Bước 1: Loại bỏ khoảng trắng dư thừa
    text = normalize_whitespace(text)
    
    # Bước 1.5: Sửa các trường hợp thiếu khoảng trắng trước các nhãn quan trọng
    # Xử lý trường hợp: "text.Nhãn" -> "text. Nhãn"
    text = re.sub(r'\.([Tt]ên sản phẩm|[Dd]anh mục|[Tt]hương hiệu|[Tt]huộc tính chính)', r'. \1', text)
    text = re.sub(r':([Tt]ên sản phẩm|[Dd]anh mục|[Tt]hương hiệu|[Tt]huộc tính chính)', r': \1', text)
    
    # Bước 2: Loại bỏ dấu câu lặp và chuẩn hóa khoảng trắng quanh dấu câu
    text = remove_redundant_punctuation(text)
    
    # Bước 3: Chuẩn hóa khoảng trắng lại sau bước 2
    text = normalize_whitespace(text)
    
    # Bước 3.5: Đảm bảo có khoảng trắng sau dấu chấm trước chữ cái (lần cuối)
    text = re.sub(r'\.([A-Za-zÀ-ỹ])', r'. \1', text)
    
    # Bước 4: Loại bỏ các cụm từ quá chung chung
    text = remove_generic_phrases(text)
    
    # Bước 5: Làm sạch phần thuộc tính
    text = clean_attributes_section(text)
    
    # Bước 6: Gộp thông tin trùng lặp
    text = merge_redundant_info(text)
    
    # Bước 7: Chuẩn hóa khoảng trắng lại (sau các bước trên)
    text = normalize_whitespace(text)
    
    # Bước 8: Viết hoa đầu câu
    text = capitalize_sentences(text)
    
    # Bước 9: Cắt ngắn nếu quá dài
    text = truncate_if_needed(text, max_length=500)
    
    # Bước 10: Chuẩn hóa lại lần cuối
    text = normalize_whitespace(text)
    
    return text


def process_embedding_file(input_path: Path, output_path: Path = None):
    """
    Xử lý file items_for_embedding.parquet để làm sạch embedding_text.
    
    Args:
        input_path: Đường dẫn đến file input
        output_path: Đường dẫn đến file output (nếu None thì ghi đè file input)
    """
    print("=" * 80)
    print("CLEAN AND NORMALIZE EMBEDDING TEXT")
    print("=" * 80)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {input_path}")
    
    if output_path is None:
        output_path = input_path
    
    print(f"\nĐang đọc file: {input_path}")
    df = pl.read_parquet(str(input_path))
    print(f"[OK] Đã đọc {len(df):,} items")
    print(f"Columns: {df.columns}")
    
    # Thống kê trước khi làm sạch
    text_lengths_before = df["embedding_text"].str.len_chars()
    print(f"\nThống kê TRƯỚC khi làm sạch:")
    print(f"  Trung bình: {text_lengths_before.mean():.1f} ký tự")
    print(f"  Min: {text_lengths_before.min()} ký tự")
    print(f"  Max: {text_lengths_before.max()} ký tự")
    print(f"  Median: {text_lengths_before.median():.1f} ký tự")
    
    # Làm sạch embedding_text
    print("\nĐang làm sạch embedding_text...")
    
    # Lưu dữ liệu gốc để so sánh sau này
    original_rows = df.to_dicts()
    
    # Convert sang dict để xử lý
    rows = df.to_dicts()
    
    cleaned_texts = []
    empty_count = 0
    
    for i, row in enumerate(rows):
        if (i + 1) % 10000 == 0:
            print(f"  Đã xử lý: {i + 1:,}/{len(rows):,} items")
        
        original_text = row.get("embedding_text", "")
        cleaned_text = clean_embedding_text(original_text)
        
        if not cleaned_text or cleaned_text.strip() == "":
            empty_count += 1
            cleaned_texts.append(original_text)  # Giữ nguyên nếu làm sạch xong thành rỗng
        else:
            cleaned_texts.append(cleaned_text)
    
    print(f"\n[OK] Đã xử lý xong {len(rows):,} items")
    print(f"  Items có embedding_text hợp lệ: {len(rows) - empty_count:,}")
    print(f"  Items không có embedding_text: {empty_count:,}")
    
    # Cập nhật DataFrame
    df = df.with_columns([
        pl.Series("embedding_text", cleaned_texts)
    ])
    
    # Thống kê sau khi làm sạch
    text_lengths_after = df["embedding_text"].str.len_chars()
    print(f"\nThống kê SAU khi làm sạch:")
    print(f"  Trung bình: {text_lengths_after.mean():.1f} ký tự")
    print(f"  Min: {text_lengths_after.min()} ký tự")
    print(f"  Max: {text_lengths_after.max()} ký tự")
    print(f"  Median: {text_lengths_after.median():.1f} ký tự")
    
    # Lưu file
    print(f"\nĐang lưu file: {output_path}")
    df.write_parquet(str(output_path))
    print(f"[OK] Đã lưu {len(df):,} items vào {output_path}")
    
    # In một vài mẫu để so sánh
    print("\n" + "=" * 80)
    print("MẪU SO SÁNH (5 items đầu tiên):")
    print("=" * 80)
    
    cleaned_rows = df.to_dicts()
    for i in range(min(5, len(df))):
        original_row = original_rows[i]
        cleaned_row = cleaned_rows[i]
        
        print(f"\n[Item {i+1}] item_id: {cleaned_row['item_id']}")
        print(f"TRƯỚC ({len(original_row['embedding_text'])} ký tự):")
        print(f"  {original_row['embedding_text']}")
        print(f"SAU ({len(cleaned_row['embedding_text'])} ký tự):")
        print(f"  {cleaned_row['embedding_text']}")
    
    print("\n" + "=" * 80)
    print("[OK] HOÀN TẤT: File đã được làm sạch và chuẩn hóa thành công!")
    print("=" * 80)
    
    return df


def main():
    """Hàm chính để chạy clean embedding text."""
    # Xác định đường dẫn project root
    script_path = Path(__file__).resolve()
    current = script_path.parent
    while current != current.parent:
        data_dir = current / "data"
        if data_dir.exists() and (data_dir / "embedding").exists():
            project_root = current
            break
        current = current.parent
    else:
        project_root = script_path.parent.parent.parent
    
    input_path = project_root / "data" / "embedding" / "items_for_embedding.parquet"
    
    # Ghi đè file gốc
    process_embedding_file(input_path, output_path=None)


if __name__ == "__main__":
    main()

