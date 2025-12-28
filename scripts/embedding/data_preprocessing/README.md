# Embedding Data Preprocessing

Thư mục này chứa các script xử lý dữ liệu cho embedding pipeline trong hệ thống recommendation.

## Cấu trúc

```
app/embedding/data_preprocessing/
├── __init__.py
├── build_embedding_view.py              # Tạo embedding view từ raw metadata
├── build_items_embedding.py             # Tạo embedding text tiếng Việt
├── build_items_embedding_en.py          # Tạo embedding text tiếng Anh (v1)
├── build_items_embedding_en_v2.py       # Tạo embedding text tiếng Anh tự nhiên (v2)
├── build_items_embedding_semantic.py    # Tạo embedding text bằng semantic scaffolding
├── clean_embedding_text.py              # Làm sạch và chuẩn hóa embedding text
└── README.md                             # File này
```

## Các file và mục đích

### 1. `build_embedding_view.py`
- **Mục đích**: Tạo embedding view từ raw metadata Amazon All Beauty
- **Input**: `data/raw/meta_All_Beauty.jsonl`
- **Output**: `data/embedding/metadata_for_embedding.parquet`
- **Chức năng**: Extract và làm sạch tối thiểu các cột cần thiết (parent_asin, title, store, main_category, details, features, description)

### 2. `build_items_embedding.py`
- **Mục đích**: Tạo embedding text tiếng Việt cho items
- **Input**: `data/processed/items_for_rs.parquet`
- **Output**: `data/embedding/items_for_embedding.parquet`
- **Chức năng**: Xây dựng embedding_text từ title, category, store, và attributes từ raw_metadata

### 3. `build_items_embedding_en.py`
- **Mục đích**: Tạo embedding text tiếng Anh (phiên bản đầu)
- **Input**: `data/embedding/items_for_embedding.parquet`, `data/processed/items_for_rs.parquet`
- **Output**: `data/embedding/items_for_embedding_en.parquet`
- **Chức năng**: Chuyển embedding_text sang tiếng Anh với product type và usage

### 4. `build_items_embedding_en_v2.py`
- **Mục đích**: Tạo embedding text tiếng Anh tự nhiên hơn
- **Input**: `data/embedding/items_for_embedding_en.parquet`, `data/processed/items_for_rs.parquet`
- **Output**: `data/embedding/items_for_embedding_en_v2.parquet`
- **Chức năng**: Viết lại embedding_text với văn phong tự nhiên, tập trung vào công dụng

### 5. `build_items_embedding_semantic.py`
- **Mục đích**: Tạo embedding text bằng semantic scaffolding (production approach)
- **Input**: `data/embedding/items_for_embedding_en_final.parquet` (hoặc v2), `data/processed/items_for_rs.parquet`
- **Output**: `data/embedding/items_for_embedding_semantic.parquet`
- **Chức năng**: Sử dụng template cố định + domain knowledge mapping để tối ưu semantic separation

### 6. `clean_embedding_text.py`
- **Mục đích**: Làm sạch và chuẩn hóa embedding_text
- **Input**: `data/embedding/items_for_embedding.parquet`
- **Output**: Ghi đè file input (hoặc file mới)
- **Chức năng**: Loại bỏ dấu câu lặp, chuẩn hóa khoảng trắng, loại bỏ cụm từ quảng cáo

## Quy trình sử dụng

### Pipeline chuẩn:
1. `build_embedding_view.py` - Tạo view từ raw metadata
2. `build_items_embedding.py` - Tạo embedding text tiếng Việt
3. `build_items_embedding_en.py` - Chuyển sang tiếng Anh
4. `build_items_embedding_en_v2.py` - Cải thiện văn phong (tùy chọn)
5. `build_items_embedding_semantic.py` - Semantic scaffolding (khuyến nghị cho production)
6. `clean_embedding_text.py` - Làm sạch (nếu cần)

### Cho production (khuyến nghị):
- Sử dụng `build_items_embedding_semantic.py` vì:
  - Template cố định, nhất quán
  - Semantic separation tốt
  - Dễ debug và mở rộng
  - Không phụ thuộc văn phong tự nhiên

## Lưu ý

- Tất cả các file đều có thể chạy độc lập
- Mỗi file có hàm `main()` riêng
- Không sửa đổi dữ liệu gốc, chỉ tạo file mới
- Các file output có thể được sử dụng cho training embeddings trên Google Colab

