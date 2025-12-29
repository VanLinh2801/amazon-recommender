"""
Script để chuẩn bị và load dữ liệu vào database.
Tự động chạy các phase preprocessing nếu cần và load vào database.

Usage:
    python backend/scripts/prepare_and_load_data.py
"""
import sys
import asyncio
import os
import subprocess
from pathlib import Path

# Fix encoding cho Windows console
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm backend vào path
BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BASE_DIR.parent
sys.path.insert(0, str(BASE_DIR))

# Load .env file if exists
try:
    from dotenv import load_dotenv
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        print(f"✅ Loaded .env file from {env_path}")
except ImportError:
    pass

from app.web.utils.database import normalize_database_url


def check_data_files():
    """Kiểm tra xem các file parquet đã có chưa."""
    data_processed = PROJECT_ROOT / "data" / "processed"
    
    items_file = data_processed / "items_for_rs.parquet"
    reviews_file = data_processed / "reviews_clean.parquet"
    
    return {
        "items_exists": items_file.exists(),
        "reviews_exists": reviews_file.exists(),
        "items_file": items_file,
        "reviews_file": reviews_file
    }


def run_preprocessing_phases():
    """Chạy các phase preprocessing để tạo dữ liệu."""
    print("\n" + "=" * 80)
    print("CHẠY CÁC PHASE PREPROCESSING")
    print("=" * 80)
    
    phases = [
        ("Phase 1: Ingest", "scripts/data_preprocessing/phase1_ingest.py"),
        ("Phase 2: Normalize", "scripts/data_preprocessing/phase2_normalize.py"),
        ("Phase 3: Cleaning", "scripts/data_preprocessing/phase3_cleaning.py"),
        ("Phase 4: Build Interactions", "scripts/data_preprocessing/phase4_build_interactions.py"),
    ]
    
    for phase_name, script_path in phases:
        print(f"\n[{phase_name}] Đang chạy {script_path}...")
        script_full_path = PROJECT_ROOT / script_path
        
        if not script_full_path.exists():
            print(f"⚠️  Không tìm thấy script: {script_full_path}")
            continue
        
        try:
            result = subprocess.run(
                [sys.executable, str(script_full_path)],
                cwd=PROJECT_ROOT,
                check=True,
                capture_output=True,
                text=True
            )
            print(f"✅ {phase_name} hoàn thành!")
            if result.stdout:
                print(result.stdout[-500:])  # In 500 ký tự cuối
        except subprocess.CalledProcessError as e:
            print(f"❌ Lỗi khi chạy {phase_name}: {e}")
            if e.stdout:
                print(e.stdout[-500:])
            if e.stderr:
                print(e.stderr[-500:])
            return False
    
    return True


async def main():
    """Hàm chính."""
    print("=" * 80)
    print("CHUẨN BỊ VÀ LOAD DỮ LIỆU VÀO DATABASE")
    print("=" * 80)
    
    # Kiểm tra file data
    print("\n[1] Kiểm tra file parquet...")
    data_status = check_data_files()
    
    if not data_status["items_exists"] or not data_status["reviews_exists"]:
        print("⚠️  Các file parquet chưa có!")
        print(f"   - items_for_rs.parquet: {'✅' if data_status['items_exists'] else '❌'}")
        print(f"   - reviews_clean.parquet: {'✅' if data_status['reviews_exists'] else '❌'}")
        
        print("\n⚠️  Tự động chạy preprocessing để tạo dữ liệu...")
        if not run_preprocessing_phases():
            print("\n❌ Lỗi khi chạy preprocessing. Vui lòng kiểm tra lại.")
            print("\nHoặc chạy thủ công:")
            print("   1. python scripts/data_preprocessing/phase1_ingest.py")
            print("   2. python scripts/data_preprocessing/phase2_normalize.py")
            print("   3. python scripts/data_preprocessing/phase3_cleaning.py")
            print("   4. python scripts/data_preprocessing/phase4_build_interactions.py")
            return False
    
    # Kiểm tra lại sau khi chạy preprocessing
    data_status = check_data_files()
    if not data_status["items_exists"] or not data_status["reviews_exists"]:
        print("\n❌ Vẫn chưa có đủ file parquet sau khi chạy preprocessing!")
        return False
    
    # Load vào database
    print("\n[2] Đang load dữ liệu vào database...")
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        print("❌ ERROR: Chưa có DATABASE_URL")
        return False
    
    # Import và chạy load script
    from scripts.load_data_to_db import load_data_to_database
    success = await load_data_to_database(database_url)
    
    if success:
        print("\n" + "=" * 80)
        print("✅ HOÀN TẤT: Dữ liệu đã được load vào database!")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("❌ CÓ LỖI XẢY RA!")
        print("=" * 80)
    
    return success


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)

