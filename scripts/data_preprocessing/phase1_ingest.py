"""
Phase 1: Data Ingestion
========================
Mục tiêu: Load dữ liệu thô từ JSON Lines files vào Polars DataFrame
- Không xử lý hay clean dữ liệu ở bước này
- Chỉ kiểm tra ingest và schema

Chạy độc lập: python app/data_preprocessing/phase1_ingest.py
"""

import polars as pl
from pathlib import Path
import sys
import io

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


def ingest_reviews_data(file_path: Path) -> pl.DataFrame:
    """
    Đọc file reviews (All_Beauty.jsonl) vào Polars DataFrame.
    
    Args:
        file_path: Đường dẫn đến file JSON Lines
        
    Returns:
        Polars DataFrame chứa reviews data
    """
    print(f"Đang đọc file reviews: {file_path}")
    try:
        # Thử đọc trực tiếp với Polars
        df = pl.read_ndjson(str(file_path))
        return df
    except Exception as e:
        print(f"⚠️  Lỗi khi đọc với Polars: {e}")
        print("   Đang thử đọc với pandas rồi convert sang Polars...")
        # Fallback: đọc với pandas rồi convert
        import pandas as pd
        import json
        
        data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data.append(json.loads(line.strip()))
                except json.JSONDecodeError as je:
                    print(f"⚠️  Lỗi JSON ở dòng {line_num}: {je}")
                    continue
                except Exception as e:
                    print(f"⚠️  Lỗi khác ở dòng {line_num}: {e}")
                    continue
        
        if not data:
            raise ValueError("Không đọc được dữ liệu nào từ file")
        
        # Convert sang pandas DataFrame
        pdf = pd.DataFrame(data)
        # Convert sang Polars
        df = pl.from_pandas(pdf)
        print(f"✅ Đã đọc {len(df):,} dòng bằng pandas fallback")
        return df


def ingest_metadata(file_path: Path) -> pl.DataFrame:
    """
    Đọc file metadata (meta_All_Beauty.jsonl) vào Polars DataFrame.
    
    Args:
        file_path: Đường dẫn đến file JSON Lines
        
    Returns:
        Polars DataFrame chứa metadata
    """
    print(f"Đang đọc file metadata: {file_path}")
    try:
        # Thử đọc trực tiếp với Polars
        df = pl.read_ndjson(str(file_path))
        return df
    except Exception as e:
        print(f"⚠️  Lỗi khi đọc với Polars: {e}")
        print("   Đang thử đọc với pandas rồi convert sang Polars...")
        # Fallback: đọc với pandas rồi convert
        import pandas as pd
        import json
        
        data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data.append(json.loads(line.strip()))
                except json.JSONDecodeError as je:
                    print(f"⚠️  Lỗi JSON ở dòng {line_num}: {je}")
                    continue
                except Exception as e:
                    print(f"⚠️  Lỗi khác ở dòng {line_num}: {e}")
                    continue
        
        if not data:
            raise ValueError("Không đọc được dữ liệu nào từ file")
        
        # Convert sang pandas DataFrame
        pdf = pd.DataFrame(data)
        # Convert sang Polars
        df = pl.from_pandas(pdf)
        print(f"✅ Đã đọc {len(df):,} dòng bằng pandas fallback")
        return df


def main():
    """Hàm chính để chạy phase 1 ingestion."""
    # Xác định đường dẫn đến file dữ liệu
    # Hỗ trợ chạy từ root hoặc từ trong thư mục app/data_preprocessing
    script_path = Path(__file__).resolve()
    # Tìm project root (thư mục chứa data/)
    current = script_path.parent
    while current != current.parent:
        data_dir = current / "data"
        if data_dir.exists() and (data_dir / "raw").exists():
            project_root = current
            break
        current = current.parent
    else:
        # Fallback: giả định chạy từ root
        project_root = script_path.parent.parent.parent
    
    data_raw_dir = project_root / "data" / "raw"
    
    # Thử tìm file với extension .jsonl hoặc .json
    reviews_file = None
    metadata_file = None
    
    # Tìm file reviews
    for ext in [".jsonl", ".json"]:
        candidate = data_raw_dir / f"All_Beauty{ext}"
        if candidate.exists():
            reviews_file = candidate
            break
    
    # Tìm file metadata
    for ext in [".jsonl", ".json"]:
        candidate = data_raw_dir / f"meta_All_Beauty{ext}"
        if candidate.exists():
            metadata_file = candidate
            break
    
    # Kiểm tra file tồn tại
    if not reviews_file or not reviews_file.exists():
        print(f"[ERROR] Không tìm thấy file reviews: All_Beauty.jsonl hoặc All_Beauty.json")
        print(f"   Vui lòng đảm bảo file tồn tại tại: {data_raw_dir}")
        return
    
    if not metadata_file or not metadata_file.exists():
        print(f"[ERROR] Không tìm thấy file metadata: meta_All_Beauty.jsonl hoặc meta_All_Beauty.json")
        print(f"   Vui lòng đảm bảo file tồn tại tại: {data_raw_dir}")
        return
    
    print("=" * 80)
    print("PHASE 1: DATA INGESTION")
    print("=" * 80)
    print()
    
    # Ingest reviews data
    print("[REVIEWS DATA]")
    print("-" * 80)
    reviews_df = ingest_reviews_data(reviews_file)
    print(f"[OK] Số dòng: {len(reviews_df):,}")
    print(f"[OK] Số cột: {len(reviews_df.columns)}")
    print("\nSchema:")
    print(reviews_df.schema)
    print()
    
    # Ingest metadata
    print("[METADATA]")
    print("-" * 80)
    metadata_df = ingest_metadata(metadata_file)
    print(f"[OK] Số dòng: {len(metadata_df):,}")
    print(f"[OK] Số cột: {len(metadata_df.columns)}")
    print("\nSchema:")
    print(metadata_df.schema)
    print()
    
    print("=" * 80)
    print("[OK] PHASE 1 HOÀN TẤT: Dữ liệu đã được ingest thành công!")
    print("=" * 80)
    
    return reviews_df, metadata_df


if __name__ == "__main__":
    main()

