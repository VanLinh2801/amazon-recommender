"""
Upsert Embeddings to Qdrant
============================
Script để load embeddings từ file và upsert vào Qdrant.

Logic:
1. Load embeddings từ artifacts/embeddings/item_embeddings.npy
2. Load item_ids từ artifacts/embeddings/item_ids.json
3. Kết nối Qdrant
4. Upsert embeddings vào Qdrant collection

Chạy: python app/database/scripts/upsert_embeddings_to_qdrant.py
"""

import sys
import io
import json
from pathlib import Path
import numpy as np

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add project root to path
script_path = Path(__file__).resolve()
project_root = script_path.parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def load_embeddings(project_root: Path) -> tuple[np.ndarray, list[str]]:
    """
    Load embeddings và item_ids từ file.
    
    Args:
        project_root: Project root path
        
    Returns:
        Tuple of (embeddings array, item_ids list)
    """
    embeddings_file = project_root / "artifacts" / "embeddings" / "item_embeddings.npy"
    item_ids_file = project_root / "artifacts" / "embeddings" / "item_ids.json"
    
    if not embeddings_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {embeddings_file}")
    if not item_ids_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {item_ids_file}")
    
    print(f"Đang load embeddings từ {embeddings_file}...")
    embeddings = np.load(str(embeddings_file))
    print(f"  Shape: {embeddings.shape}")
    
    print(f"Đang load item_ids từ {item_ids_file}...")
    with open(item_ids_file, 'r', encoding='utf-8') as f:
        item_ids = json.load(f)
    print(f"  Số item_ids: {len(item_ids):,}")
    
    # Kiểm tra tính nhất quán
    if len(embeddings) != len(item_ids):
        print(f"⚠️  Cảnh báo: Số embeddings ({len(embeddings):,}) != số item_ids ({len(item_ids):,})")
        min_len = min(len(embeddings), len(item_ids))
        embeddings = embeddings[:min_len]
        item_ids = item_ids[:min_len]
        print(f"  Đã cắt về {min_len:,} items")
    
    return embeddings, item_ids


def upsert_to_qdrant(embeddings: np.ndarray, item_ids: list[str], project_root: Path):
    """
    Upsert embeddings vào Qdrant.
    
    Args:
        embeddings: Embeddings array
        item_ids: List of item IDs
        project_root: Project root path
    """
    print("\n" + "=" * 80)
    print("UPSERT EMBEDDINGS TO QDRANT")
    print("=" * 80)
    
    try:
        # Import QdrantManager
        from vector_db.qdrant.qdrant_manager import QdrantManager
        
        print("\nĐang kết nối Qdrant...")
        qdrant_manager = QdrantManager()
        
        if not qdrant_manager.connect():
            print("❌ Không thể kết nối Qdrant. Vui lòng kiểm tra:")
            print("  1. Qdrant server đang chạy (docker run -p 6333:6333 qdrant/qdrant)")
            print("  2. Qdrant URL đúng trong config")
            return False
        
        print("[OK] Đã kết nối Qdrant")
        
        # Kiểm tra collection
        vector_size = embeddings.shape[1]
        print(f"\nĐang đảm bảo collection tồn tại (vector_size={vector_size})...")
        if not qdrant_manager.ensure_collection(vector_size=vector_size):
            print("❌ Không thể tạo/kiểm tra collection")
            return False
        
        print("[OK] Collection đã sẵn sàng")
        
        # Upsert embeddings
        print(f"\nĐang upsert {len(item_ids):,} embeddings vào Qdrant...")
        success = qdrant_manager.upsert_items(embeddings, item_ids)
        
        if success:
            print(f"[OK] Đã upsert thành công {len(item_ids):,} embeddings")
            
            # Kiểm tra collection info
            collection_info = qdrant_manager.get_collection_info()
            if collection_info:
                print(f"\nCollection info:")
                print(f"  Points count: {collection_info.get('points_count', 'N/A'):,}")
                print(f"  Vector size: {collection_info.get('config', {}).get('params', {}).get('vectors', {}).get('size', 'N/A')}")
            
            return True
        else:
            print("❌ Upsert thất bại")
            return False
            
    except ImportError as e:
        print(f"❌ Không thể import QdrantManager: {e}")
        print("  Vui lòng kiểm tra xem vector_db module có tồn tại không")
        return False
    except Exception as e:
        print(f"❌ Lỗi khi upsert embeddings: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Hàm chính."""
    print("=" * 80)
    print("UPSERT EMBEDDINGS TO QDRANT")
    print("=" * 80)
    
    try:
        # Load embeddings
        embeddings, item_ids = load_embeddings(project_root)
        
        # Upsert to Qdrant
        success = upsert_to_qdrant(embeddings, item_ids, project_root)
        
        if success:
            print("\n" + "=" * 80)
            print("[OK] HOÀN TẤT: Embeddings đã được upsert vào Qdrant!")
            print("=" * 80)
        else:
            print("\n" + "=" * 80)
            print("[ERROR] Upsert embeddings thất bại")
            print("=" * 80)
            sys.exit(1)
            
    except FileNotFoundError as e:
        print(f"❌ Lỗi: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

