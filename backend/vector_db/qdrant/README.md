# Qdrant Vector Database Manager

Module quản lý Qdrant vector database cho item text embeddings trong hệ thống recommendation.

## Cấu trúc

```
vector_db/qdrant/
├── __init__.py           # Module initialization
├── qdrant_manager.py     # Class QdrantManager - quản lý Qdrant
├── main.py              # Script chính để khởi tạo và load embeddings
└── README.md            # File này
```

## Yêu cầu

1. **Qdrant Server**: Phải chạy Qdrant local tại `http://localhost:6333`
   - Cài đặt: https://qdrant.tech/documentation/quick-start/
   - Hoặc dùng Docker: `docker run -p 6333:6333 qdrant/qdrant`

2. **Dependencies**:
   ```bash
   pip install qdrant-client numpy
   ```

3. **Dữ liệu**: Hai file embeddings đã được train offline:
   - `artifacts/embeddings/item_embeddings.npy`
   - `artifacts/embeddings/item_ids.json`

## Sử dụng

### 1. Khởi động Qdrant

```bash
# Option 1: Docker
docker run -p 6333:6333 qdrant/qdrant

# Option 2: Binary (nếu đã cài đặt)
qdrant
```

### 2. Chạy script khởi tạo

```bash
python vector_db/qdrant/main.py
```

Script sẽ:
- Kết nối tới Qdrant local
- Kiểm tra/tạo collection `item_text_embeddings`
- Load embeddings từ file
- Upsert items vào Qdrant
- Test search để kiểm tra hoạt động

### 3. Sử dụng trong code

```python
from vector_db.qdrant import QdrantManager
import numpy as np

# Khởi tạo manager
manager = QdrantManager(
    url="http://localhost:6333",
    collection_name="item_text_embeddings"
)

# Kết nối
manager.connect()

# Tìm items tương tự
query_vector = np.array([...])  # Vector embedding của query
similar_items = manager.search_similar_items(
    query_vector=query_vector,
    top_k=10
)

# Lấy vector của một item
item_vector = manager.get_item_vector("item_id_123")
```

## Collection Configuration

- **Tên**: `item_text_embeddings`
- **Vector size**: 1024 (từ model BAAI/bge-large-en-v1.5)
- **Distance metric**: Cosine
- **Payload**: `{"type": "item"}`

## API Reference

### QdrantManager

#### Methods

- `connect()`: Kết nối tới Qdrant server
- `check_collection_exists()`: Kiểm tra collection tồn tại
- `create_collection(vector_size)`: Tạo collection mới
- `ensure_collection(vector_size)`: Đảm bảo collection tồn tại
- `load_embeddings(embeddings_file, item_ids_file)`: Load embeddings từ file
- `upsert_items(embeddings, item_ids, batch_size)`: Upsert items vào Qdrant
- `search_similar_items(query_vector, top_k)`: Tìm items tương tự
- `get_item_vector(item_id)`: Lấy vector của một item
- `test_search(item_ids, top_k)`: Test search functionality

## Troubleshooting

### Lỗi kết nối

```
[ERROR] Không thể kết nối tới Qdrant
```

**Giải pháp**: Đảm bảo Qdrant đang chạy tại `http://localhost:6333`

### Lỗi vector size không khớp

```
[WARNING] Vector size không khớp!
```

**Giải pháp**: Script sẽ tự động tạo lại collection với vector size đúng

### Lỗi không tìm thấy file

```
FileNotFoundError: Không tìm thấy file: ...
```

**Giải pháp**: Đảm bảo hai file embeddings đã được tải về và đặt đúng đường dẫn:
- `artifacts/embeddings/item_embeddings.npy`
- `artifacts/embeddings/item_ids.json`

## Mở rộng

Để mở rộng hệ thống:

1. Thêm payload metadata vào `upsert_items()` nếu cần
2. Thêm filtering trong `search_similar_items()` nếu cần
3. Thêm batch operations khác nếu cần

