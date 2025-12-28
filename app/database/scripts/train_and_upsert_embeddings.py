"""
Train and Upsert Embeddings to Qdrant
======================================
Script để train embeddings từ embedding_text.parquet và upsert vào Qdrant.

Logic:
1. Load embedding_text.parquet
2. Train embeddings sử dụng SentenceTransformer
3. Lưu embeddings và item_ids
4. Upsert vào Qdrant

Chạy: python app/database/scripts/train_and_upsert_embeddings.py
"""

import sys
import io
import json
from pathlib import Path
import numpy as np
import polars as pl

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add project root to path
script_path = Path(__file__).resolve()
project_root = script_path.parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def load_embedding_text(project_root: Path) -> tuple[pl.DataFrame, list[str]]:
    """
    Load embedding_text.parquet.
    
    Args:
        project_root: Project root path
        
    Returns:
        Tuple of (DataFrame, item_ids list)
    """
    embedding_text_file = project_root / "data" / "embedding" / "embedding_text.parquet"
    
    if not embedding_text_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {embedding_text_file}")
    
    print(f"Đang load embedding_text từ {embedding_text_file}...")
    df = pl.read_parquet(str(embedding_text_file))
    print(f"  Số items: {len(df):,}")
    print(f"  Columns: {df.columns}")
    
    # Lấy item_ids
    item_id_col = "parent_asin" if "parent_asin" in df.columns else "item_id"
    item_ids = df[item_id_col].to_list()
    
    return df, item_ids


def train_embeddings(df: pl.DataFrame, batch_size: int = 32) -> np.ndarray:
    """
    Train embeddings từ embedding_text sử dụng SentenceTransformer.
    
    Args:
        df: DataFrame với cột embedding_text
        batch_size: Batch size cho encoding
        
    Returns:
        Embeddings array
    """
    print("\n" + "=" * 80)
    print("TRAIN EMBEDDINGS")
    print("=" * 80)
    
    try:
        from sentence_transformers import SentenceTransformer
        import torch
    except ImportError:
        print("❌ Cần cài đặt sentence-transformers và torch")
        print("  pip install sentence-transformers torch")
        sys.exit(1)
    
    # Kiểm tra GPU
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"  Device: {device}")
    
    # Load model
    model_name = "BAAI/bge-large-en-v1.5"
    print(f"\nĐang load model: {model_name}...")
    model = SentenceTransformer(model_name, device=str(device))
    embedding_dim = model.get_sentence_embedding_dimension()
    print(f"  Embedding dimension: {embedding_dim}")
    
    # Lấy texts
    texts = df["embedding_text"].to_list()
    print(f"\nĐang encode {len(texts):,} texts...")
    
    # Encode với batch processing
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=True,
        normalize_embeddings=True,  # Chuẩn hóa L2
        convert_to_numpy=True,
        device=str(device)
    )
    
    print(f"\n[OK] Đã encode xong {len(embeddings):,} embeddings")
    print(f"  Shape: {embeddings.shape}")
    
    # Kiểm tra chuẩn hóa L2
    norms = np.linalg.norm(embeddings, axis=1)
    print(f"\nKiểm tra chuẩn hóa L2:")
    print(f"  Norm trung bình: {norms.mean():.6f}")
    print(f"  Norm min: {norms.min():.6f}")
    print(f"  Norm max: {norms.max():.6f}")
    
    return embeddings


def save_embeddings(embeddings: np.ndarray, item_ids: list[str], project_root: Path):
    """
    Lưu embeddings và item_ids vào file.
    
    Args:
        embeddings: Embeddings array
        item_ids: List of item IDs
        project_root: Project root path
    """
    print("\n" + "=" * 80)
    print("SAVE EMBEDDINGS")
    print("=" * 80)
    
    output_dir = project_root / "artifacts" / "embeddings"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    embeddings_file = output_dir / "item_embeddings.npy"
    item_ids_file = output_dir / "item_ids.json"
    
    print(f"\nĐang lưu embeddings vào {embeddings_file}...")
    np.save(str(embeddings_file), embeddings)
    file_size_mb = embeddings_file.stat().st_size / (1024 * 1024)
    print(f"  [OK] Đã lưu embeddings")
    print(f"     Shape: {embeddings.shape}")
    print(f"     File size: {file_size_mb:.2f} MB")
    
    print(f"\nĐang lưu item_ids vào {item_ids_file}...")
    with open(item_ids_file, 'w', encoding='utf-8') as f:
        json.dump(item_ids, f, ensure_ascii=False, indent=2)
    print(f"  [OK] Đã lưu {len(item_ids):,} item_ids")
    
    # Kiểm tra tính nhất quán
    print(f"\nKiểm tra tính nhất quán:")
    print(f"  Số embeddings: {len(embeddings):,}")
    print(f"  Số item_ids: {len(item_ids):,}")
    if len(embeddings) == len(item_ids):
        print(f"  [OK] Số lượng khớp nhau")
    else:
        print(f"  [ERROR] Số lượng không khớp!")


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
    import argparse
    
    parser = argparse.ArgumentParser(description="Train embeddings và upsert vào Qdrant")
    parser.add_argument(
        "--skip-train",
        action="store_true",
        help="Skip training, chỉ upsert embeddings đã có"
    )
    parser.add_argument(
        "--skip-upsert",
        action="store_true",
        help="Skip upsert, chỉ train và lưu embeddings"
    )
    args = parser.parse_args()
    
    print("=" * 80)
    print("TRAIN AND UPSERT EMBEDDINGS TO QDRANT")
    print("=" * 80)
    
    try:
        # Load embedding_text
        df, item_ids = load_embedding_text(project_root)
        
        # Train embeddings (nếu không skip)
        if not args.skip_train:
            embeddings = train_embeddings(df, batch_size=32)
            
            # Save embeddings
            save_embeddings(embeddings, item_ids, project_root)
        else:
            # Load embeddings đã có
            embeddings_file = project_root / "artifacts" / "embeddings" / "item_embeddings.npy"
            item_ids_file = project_root / "artifacts" / "embeddings" / "item_ids.json"
            
            if not embeddings_file.exists() or not item_ids_file.exists():
                print("❌ Không tìm thấy embeddings đã train. Vui lòng train trước.")
                sys.exit(1)
            
            print("\nĐang load embeddings đã train...")
            embeddings = np.load(str(embeddings_file))
            with open(item_ids_file, 'r', encoding='utf-8') as f:
                item_ids = json.load(f)
            print(f"  [OK] Đã load {len(embeddings):,} embeddings")
        
        # Upsert to Qdrant (nếu không skip)
        if not args.skip_upsert:
            success = upsert_to_qdrant(embeddings, item_ids, project_root)
            
            if not success:
                print("\n[ERROR] Upsert embeddings thất bại")
                sys.exit(1)
        
        print("\n" + "=" * 80)
        print("[OK] HOÀN TẤT!")
        print("=" * 80)
            
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

