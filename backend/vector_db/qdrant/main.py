"""
Main Script: Initialize Qdrant and Load Item Embeddings
========================================================
Script chính để khởi tạo Qdrant, tạo collection và load item embeddings
từ file đã được train offline.

Chạy độc lập: python vector_db/qdrant/main.py
"""

import sys
import io
from pathlib import Path

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Import trực tiếp từ file để tránh import các module khác trong project
# (có thể có module cố kết nối database khi import)
import importlib.util
qdrant_manager_path = Path(__file__).resolve().parent / 'qdrant_manager.py'
spec = importlib.util.spec_from_file_location("qdrant_manager", qdrant_manager_path)
qdrant_manager_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qdrant_manager_module)
QdrantManager = qdrant_manager_module.QdrantManager

import numpy as np


def main():
    """
    Hàm chính để chạy toàn bộ pipeline khởi tạo Qdrant.
    """
    print("=" * 80)
    print("INITIALIZE QDRANT AND LOAD ITEM EMBEDDINGS")
    print("=" * 80)
    
    # Đường dẫn các file
    project_root = Path(__file__).resolve().parent.parent.parent
    # Tìm embeddings ở backend/artifacts hoặc artifacts
    embeddings_file = project_root / 'backend' / 'artifacts' / 'embeddings' / 'item_embeddings.npy'
    item_ids_file = project_root / 'backend' / 'artifacts' / 'embeddings' / 'item_ids.json'
    
    # Fallback: thử tìm ở artifacts nếu không có ở backend/artifacts
    if not embeddings_file.exists():
        embeddings_file = project_root / 'artifacts' / 'embeddings' / 'item_embeddings.npy'
    if not item_ids_file.exists():
        item_ids_file = project_root / 'artifacts' / 'embeddings' / 'item_ids.json'
    
    # Bước 1: Khởi tạo QdrantManager
    print("\n" + "=" * 80)
    print("BƯỚC 1: KHỞI TẠO QDRANT MANAGER")
    print("=" * 80)
    
    qdrant_manager = QdrantManager(
        url="http://localhost:6333",
        collection_name="item_text_embeddings"
    )
    
    # Bước 2: Kết nối và kiểm tra Qdrant
    print("\n" + "=" * 80)
    print("BƯỚC 2: KẾT NỐI TỚI QDRANT")
    print("=" * 80)
    
    if not qdrant_manager.connect():
        print("\n[ERROR] Không thể kết nối tới Qdrant!")
        print("[INFO] Đảm bảo Qdrant đang chạy:")
        print("  - Kiểm tra Qdrant đã được cài đặt và khởi động")
        print("  - Chạy: docker run -p 6333:6333 qdrant/qdrant")
        print("  - Hoặc: qdrant")
        return False
    
    # Bước 3: Load embeddings và item_ids
    print("\n" + "=" * 80)
    print("BƯỚC 3: LOAD EMBEDDINGS VÀ ITEM_IDS")
    print("=" * 80)
    
    try:
        embeddings, item_ids = qdrant_manager.load_embeddings(
            embeddings_file=embeddings_file,
            item_ids_file=item_ids_file
        )
        
        # Lấy embedding dimension
        embedding_dim = embeddings.shape[1]
        print(f"\n[OK] Embedding dimension: {embedding_dim}")
        
    except Exception as e:
        print(f"\n[ERROR] Không thể load embeddings: {e}")
        return False
    
    # Bước 4: Đảm bảo collection tồn tại với đúng cấu hình
    print("\n" + "=" * 80)
    print("BƯỚC 4: KIỂM TRA VÀ TẠO COLLECTION")
    print("=" * 80)
    
    # Thử ensure collection, nếu lỗi validation thì bỏ qua (collection đã tồn tại)
    try:
        if not qdrant_manager.ensure_collection(vector_size=embedding_dim):
            print("\n[WARNING] Không thể kiểm tra collection (có thể do version compatibility)")
            print("[INFO] Tiếp tục với upsert...")
    except Exception as e:
        print(f"\n[WARNING] Lỗi khi kiểm tra collection: {e}")
        print("[INFO] Collection có thể đã tồn tại, tiếp tục với upsert...")
    
    # Bước 5: Upsert items vào Qdrant
    print("\n" + "=" * 80)
    print("BƯỚC 5: UPSERT ITEMS VÀO QDRANT")
    print("=" * 80)
    
    # Kiểm tra xem collection đã có dữ liệu chưa
    collection_info = qdrant_manager.get_collection_info()
    if collection_info and collection_info['points_count'] > 0:
        print(f"\n[INFO] Collection đã có {collection_info['points_count']:,} points")
        user_input = input("Bạn có muốn upsert lại không? (y/n): ")
        if user_input.lower() != 'y':
            print("[INFO] Bỏ qua upsert")
        else:
            if not qdrant_manager.upsert_items(
                embeddings=embeddings,
                item_ids=item_ids,
                batch_size=100  # Batch size để tránh quá tải bộ nhớ
            ):
                print("\n[ERROR] Không thể upsert items!")
                return False
    else:
        # Collection trống, upsert mới
        if not qdrant_manager.upsert_items(
            embeddings=embeddings,
            item_ids=item_ids,
            batch_size=100
        ):
            print("\n[ERROR] Không thể upsert items!")
            return False
    
    # Bước 6: Test search
    print("\n" + "=" * 80)
    print("BƯỚC 6: TEST SEARCH")
    print("=" * 80)
    
    if not qdrant_manager.test_search(item_ids=item_ids, top_k=5):
        print("\n[WARNING] Test search có vấn đề, nhưng pipeline đã hoàn thành")
    
    # Kết thúc
    print("\n" + "=" * 80)
    print("[OK] PIPELINE HOÀN TẤT THÀNH CÔNG!")
    print("=" * 80)
    
    # In thông tin tổng hợp
    collection_info = qdrant_manager.get_collection_info()
    if collection_info:
        print(f"\nThông tin collection:")
        print(f"  Tên: {collection_info['name']}")
        print(f"  Số points: {collection_info['points_count']:,}")
        print(f"  Vector size: {collection_info['config'].params.vectors.size}")
        print(f"  Distance metric: {collection_info['config'].params.vectors.distance}")
    
    print(f"\nCó thể sử dụng QdrantManager để:")
    print(f"  - Tìm items tương tự")
    print(f"  - Content-based recommendation")
    print(f"  - Semantic search")
    
    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n[INFO] Đã dừng bởi người dùng")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Lỗi không mong đợi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

