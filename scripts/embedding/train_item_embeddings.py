"""
Pipeline Training Item Embeddings for Recommendation System
==========================================================
Mục tiêu: Tạo vector embedding chất lượng cao cho các item sử dụng mô hình
pretrained BAAI/bge-large-en-v1.5 để phục vụ content-based recommendation
và vector search với Qdrant.

Môi trường: Google Colab Pro với GPU
Chạy offline, chỉ encode embedding, không fine-tune, không deploy online.

Input: data/embedding/embedding_text.parquet
Output: artifacts/embeddings/item_embeddings.npy và item_ids.json

Chạy trên Google Colab:
1. Upload file này lên Google Colab
2. Upload file embedding_text.parquet lên Google Drive
3. Chạy script này trong Colab notebook hoặc terminal
"""

import os
import json
import numpy as np
import pandas as pd
import torch
from pathlib import Path
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')


def setup_environment():
    """
    Bước 1: Mount Google Drive và thiết lập môi trường.
    """
    print("=" * 80)
    print("BƯỚC 1: THIẾT LẬP MÔI TRƯỜNG")
    print("=" * 80)
    
    # Mount Google Drive (chạy lần đầu sẽ yêu cầu xác thực)
    try:
        from google.colab import drive
        drive.mount('/content/drive')
        print("[OK] Đã mount Google Drive thành công")
        base_path = Path('/content/drive/MyDrive')
    except ImportError:
        print("[INFO] Không phải môi trường Colab, sử dụng đường dẫn local")
        base_path = Path('.')
    
    # Tạo thư mục output nếu chưa có
    output_dir = base_path / 'artifacts' / 'embeddings'
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[OK] Thư mục output: {output_dir}")
    
    return base_path, output_dir


def check_gpu():
    """
    Bước 2: Kiểm tra GPU có khả dụng không.
    """
    print("\n" + "=" * 80)
    print("BƯỚC 2: KIỂM TRA GPU")
    print("=" * 80)
    
    if torch.cuda.is_available():
        gpu_name = torch.cuda.get_device_name(0)
        gpu_memory = torch.cuda.get_device_properties(0).total_memory / 1024**3
        print(f"[OK] GPU khả dụng: {gpu_name}")
        print(f"[OK] GPU Memory: {gpu_memory:.2f} GB")
        print(f"[OK] CUDA Version: {torch.version.cuda}")
        device = torch.device('cuda')
    else:
        print("[WARNING] GPU không khả dụng, sẽ sử dụng CPU (chậm hơn)")
        device = torch.device('cpu')
    
    return device


def load_data(base_path):
    """
    Bước 3: Đọc file embedding_text.parquet và làm sạch dữ liệu.
    
    Returns:
        df: DataFrame đã được làm sạch
        item_ids: Danh sách item_id theo thứ tự
    """
    print("\n" + "=" * 80)
    print("BƯỚC 3: ĐỌC VÀ LÀM SẠCH DỮ LIỆU")
    print("=" * 80)
    
    # Đường dẫn file input - thử nhiều vị trí có thể
    possible_paths = [
        Path('/content/embedding_text.parquet'),  # Trong Colab content folder
        base_path / 'data' / 'embedding' / 'embedding_text.parquet',  # Trong Google Drive
        base_path / 'embedding_text.parquet',  # Thư mục gốc Google Drive
        Path('embedding_text.parquet'),  # Thư mục hiện tại
    ]
    
    input_file = None
    for path in possible_paths:
        if path.exists():
            input_file = path
            break
    
    if input_file is None:
        raise FileNotFoundError(
            f"Không tìm thấy file embedding_text.parquet ở bất kỳ vị trí nào.\n"
            f"Đã thử các đường dẫn:\n" + 
            "\n".join([f"  - {p}" for p in possible_paths]) +
            "\nVui lòng đảm bảo file đã được upload lên Google Drive hoặc Colab"
        )
    
    print(f"Đang đọc file: {input_file}")
    df = pd.read_parquet(str(input_file))
    print(f"[OK] Đã đọc {len(df):,} items")
    print(f"Columns: {list(df.columns)}")
    
    # Kiểm tra cấu trúc dữ liệu - chấp nhận cả 'item_id' hoặc 'parent_asin'
    if 'embedding_text' not in df.columns:
        raise ValueError(
            "File phải có cột 'embedding_text'"
        )
    
    # Normalize cột ID: chấp nhận cả 'parent_asin' hoặc 'item_id'
    if 'parent_asin' in df.columns:
        # Đổi tên parent_asin thành item_id để code phía sau nhất quán
        df = df.rename(columns={'parent_asin': 'item_id'})
        print("[INFO] Đã đổi tên cột 'parent_asin' thành 'item_id'")
    elif 'item_id' not in df.columns:
        raise ValueError(
            "File phải có cột 'item_id' hoặc 'parent_asin'"
        )
    
    # Thống kê ban đầu
    print(f"\nThống kê ban đầu:")
    print(f"  Tổng số items: {len(df):,}")
    print(f"  Items có embedding_text null: {df['embedding_text'].isna().sum():,}")
    print(f"  Items có embedding_text rỗng: {(df['embedding_text'].astype(str).str.strip() == '').sum():,}")
    
    # Loại bỏ các dòng có embedding_text rỗng hoặc null
    initial_count = len(df)
    df = df.dropna(subset=['embedding_text'])
    df = df[df['embedding_text'].astype(str).str.strip() != '']
    df = df.reset_index(drop=True)
    
    removed_count = initial_count - len(df)
    print(f"\n[OK] Đã loại bỏ {removed_count:,} items không hợp lệ")
    print(f"[OK] Số items còn lại: {len(df):,}")
    
    # Lưu danh sách item_id theo đúng thứ tự
    item_ids = df['item_id'].tolist()
    print(f"[OK] Đã lưu {len(item_ids):,} item_id theo thứ tự")
    
    # Thống kê độ dài embedding_text
    text_lengths = df['embedding_text'].astype(str).str.len()
    print(f"\nThống kê độ dài embedding_text:")
    print(f"  Trung bình: {text_lengths.mean():.1f} ký tự")
    print(f"  Min: {text_lengths.min()} ký tự")
    print(f"  Max: {text_lengths.max()} ký tự")
    print(f"  Median: {text_lengths.median():.1f} ký tự")
    
    return df, item_ids


def load_model(device):
    """
    Bước 4: Load mô hình BAAI/bge-large-en-v1.5.
    
    Args:
        device: torch.device (cuda hoặc cpu)
        
    Returns:
        model: SentenceTransformer model
    """
    print("\n" + "=" * 80)
    print("BƯỚC 4: LOAD MÔ HÌNH EMBEDDING")
    print("=" * 80)
    
    model_name = 'BAAI/bge-large-en-v1.5'
    print(f"Đang load mô hình: {model_name}")
    print("(Lần đầu tiên sẽ tải model từ HuggingFace, có thể mất vài phút...)")
    
    try:
        # Load model với device mapping tự động
        model = SentenceTransformer(model_name, device=str(device))
        print(f"[OK] Đã load mô hình thành công")
        print(f"[OK] Model device: {next(model.parameters()).device}")
        
        # Kiểm tra kích thước embedding
        # Encode một câu mẫu để lấy embedding dimension
        sample_text = "Sample text for dimension check"
        sample_embedding = model.encode(sample_text, normalize_embeddings=True)
        embedding_dim = len(sample_embedding)
        print(f"[OK] Embedding dimension: {embedding_dim}")
        
        return model, embedding_dim
        
    except Exception as e:
        print(f"[ERROR] Không thể load mô hình: {e}")
        raise


def encode_embeddings(model, df, device, batch_size=48, embedding_dim=1024):
    """
    Bước 5: Encode toàn bộ embedding_text thành vectors.
    
    Args:
        model: SentenceTransformer model
        df: DataFrame chứa embedding_text
        device: torch.device
        batch_size: Batch size cho encoding (32-64 phù hợp với Colab Pro)
        embedding_dim: Kích thước embedding vector
        
    Returns:
        embeddings: numpy array có shape (num_items, embedding_dim)
    """
    print("\n" + "=" * 80)
    print("BƯỚC 5: ENCODE EMBEDDINGS")
    print("=" * 80)
    
    num_items = len(df)
    print(f"Số items cần encode: {num_items:,}")
    print(f"Batch size: {batch_size}")
    print(f"Embedding dimension: {embedding_dim}")
    print(f"Device: {device}")
    
    # Lấy danh sách texts
    texts = df['embedding_text'].astype(str).tolist()
    
    # Encode với batch processing và progress bar
    print("\nĐang encode embeddings (có thể mất vài phút tùy vào số lượng items)...")
    
    # Sử dụng encode với normalize_embeddings=True để chuẩn hóa L2
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=True,
        normalize_embeddings=True,  # Bật chuẩn hóa L2
        convert_to_numpy=True,
        device=str(device)
    )
    
    print(f"\n[OK] Đã encode xong {len(embeddings):,} embeddings")
    print(f"[OK] Shape của embedding matrix: {embeddings.shape}")
    
    # Kiểm tra chuẩn hóa L2 (norm của mỗi vector phải xấp xỉ 1.0)
    norms = np.linalg.norm(embeddings, axis=1)
    print(f"\nKiểm tra chuẩn hóa L2:")
    print(f"  Norm trung bình: {norms.mean():.6f}")
    print(f"  Norm min: {norms.min():.6f}")
    print(f"  Norm max: {norms.max():.6f}")
    print(f"  (Lý tưởng: tất cả norm ≈ 1.0)")
    
    # In một vài vector mẫu
    print("\nMẫu embedding vectors (5 items đầu tiên):")
    for i in range(min(5, len(embeddings))):
        print(f"\n  Item {i+1}:")
        print(f"    Shape: {embeddings[i].shape}")
        print(f"    Norm: {norms[i]:.6f}")
        print(f"    Min value: {embeddings[i].min():.6f}")
        print(f"    Max value: {embeddings[i].max():.6f}")
        print(f"    Mean value: {embeddings[i].mean():.6f}")
        print(f"    First 10 values: {embeddings[i][:10]}")
    
    return embeddings


def save_artifacts(embeddings, item_ids, output_dir):
    """
    Bước 6: Lưu embeddings và item_ids vào Google Drive.
    
    Args:
        embeddings: numpy array có shape (num_items, embedding_dim)
        item_ids: danh sách item_id theo thứ tự
        output_dir: thư mục output
    """
    print("\n" + "=" * 80)
    print("BƯỚC 6: LƯU ARTIFACTS")
    print("=" * 80)
    
    # Lưu embedding matrix
    embeddings_file = output_dir / 'item_embeddings.npy'
    print(f"Đang lưu embedding matrix: {embeddings_file}")
    np.save(str(embeddings_file), embeddings)
    file_size_mb = embeddings_file.stat().st_size / (1024 * 1024)
    print(f"[OK] Đã lưu embedding matrix")
    print(f"     Shape: {embeddings.shape}")
    print(f"     File size: {file_size_mb:.2f} MB")
    
    # Lưu item_ids
    item_ids_file = output_dir / 'item_ids.json'
    print(f"\nĐang lưu item_ids: {item_ids_file}")
    with open(item_ids_file, 'w', encoding='utf-8') as f:
        json.dump(item_ids, f, ensure_ascii=False, indent=2)
    print(f"[OK] Đã lưu {len(item_ids):,} item_ids")
    
    # Kiểm tra tính nhất quán
    print(f"\nKiểm tra tính nhất quán:")
    print(f"  Số embeddings: {len(embeddings):,}")
    print(f"  Số item_ids: {len(item_ids):,}")
    if len(embeddings) == len(item_ids):
        print(f"  [OK] Số lượng khớp nhau")
    else:
        print(f"  [ERROR] Số lượng không khớp!")
    
    print(f"\n[OK] Tất cả artifacts đã được lưu vào: {output_dir}")
    print(f"     - item_embeddings.npy: Embedding matrix")
    print(f"     - item_ids.json: Danh sách item_id")


def main():
    """
    Hàm chính để chạy toàn bộ pipeline.
    """
    print("\n" + "=" * 80)
    print("PIPELINE TRAINING ITEM EMBEDDINGS")
    print("Mô hình: BAAI/bge-large-en-v1.5")
    print("=" * 80)
    
    try:
        # Bước 1: Thiết lập môi trường
        base_path, output_dir = setup_environment()
        
        # Bước 2: Kiểm tra GPU
        device = check_gpu()
        
        # Bước 3: Đọc và làm sạch dữ liệu
        df, item_ids = load_data(base_path)
        
        # Bước 4: Load mô hình
        model, embedding_dim = load_model(device)
        
        # Bước 5: Encode embeddings
        # Batch size phù hợp với Colab Pro GPU (có thể điều chỉnh 32-64)
        batch_size = 48
        embeddings = encode_embeddings(
            model, df, device, 
            batch_size=batch_size,
            embedding_dim=embedding_dim
        )
        
        # Bước 6: Lưu artifacts
        save_artifacts(embeddings, item_ids, output_dir)
        
        print("\n" + "=" * 80)
        print("[OK] PIPELINE HOÀN TẤT THÀNH CÔNG!")
        print("=" * 80)
        print(f"\nKết quả:")
        print(f"  - Số items đã encode: {len(embeddings):,}")
        print(f"  - Embedding dimension: {embedding_dim}")
        print(f"  - Output directory: {output_dir}")
        print(f"\nCó thể sử dụng embeddings này cho:")
        print(f"  - Content-based recommendation")
        print(f"  - Vector search với Qdrant")
        print(f"  - Semantic similarity search")
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("[ERROR] PIPELINE THẤT BẠI")
        print("=" * 80)
        print(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()

