"""
Script để load dữ liệu vào database từ các file parquet có sẵn.
Có thể load từ nhiều nguồn khác nhau tùy theo file có sẵn.

Usage:
    python backend/scripts/load_data_to_db.py
"""
import sys
import asyncio
import os
from pathlib import Path

# Fix encoding cho Windows console
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm backend vào path
BASE_DIR = Path(__file__).resolve().parent.parent
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

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from app.db.init_postgres import (
    get_project_root,
    load_products,
    load_items_from_reviews,
    load_reviews
)
from app.web.utils.database import normalize_database_url


def find_data_files(project_root: Path):
    """
    Tìm các file parquet có sẵn để load vào database.
    
    Returns:
        dict với keys: items_file, reviews_file
    """
    # Nếu project_root là backend/, tìm từ parent
    if project_root.name == "backend":
        project_root = project_root.parent
    
    data_processed = project_root / "data" / "processed"
    
    print(f"  Đang tìm trong: {data_processed}")
    
    # Nếu không tìm thấy, thử từ backend/data
    if not data_processed.exists():
        alt_path = BASE_DIR.parent / "data" / "processed"
        print(f"  Thử đường dẫn khác: {alt_path}")
        if alt_path.exists():
            data_processed = alt_path
    
    # Tìm items file
    items_candidates = [
        data_processed / "items_for_rs.parquet",
        data_processed / "metadata_clean.parquet",
        data_processed / "metadata_normalized.parquet",
    ]
    
    items_file = None
    for candidate in items_candidates:
        if candidate.exists():
            items_file = candidate
            print(f"✅ Tìm thấy items file: {items_file}")
            break
    
    # Tìm reviews file
    reviews_candidates = [
        data_processed / "reviews_clean.parquet",
        data_processed / "reviews_normalized.parquet",
    ]
    
    reviews_file = None
    for candidate in reviews_candidates:
        if candidate.exists():
            reviews_file = candidate
            print(f"✅ Tìm thấy reviews file: {reviews_file}")
            break
    
    return {
        "items_file": items_file,
        "reviews_file": reviews_file
    }


async def load_data_to_database(database_url: str):
    """
    Load dữ liệu vào database từ các file parquet có sẵn.
    """
    print("=" * 80)
    print("LOAD DỮ LIỆU VÀO DATABASE")
    print("=" * 80)
    
    # Normalize URL
    async_url = normalize_database_url(database_url)
    
    # Ẩn password trong log
    safe_url = database_url.split("@")[1] if "@" in database_url else database_url
    print(f"\n📍 Database: {safe_url}")
    
    # Tìm project root
    project_root = get_project_root()
    print(f"📍 Project root: {project_root}")
    
    # Tìm các file data
    print("\n🔍 Đang tìm các file parquet...")
    data_files = find_data_files(project_root)
    
    if not data_files["items_file"] and not data_files["reviews_file"]:
        print("\n❌ ERROR: Không tìm thấy file parquet nào để load!")
        print("\nCác file cần có:")
        print("  - data/processed/items_for_rs.parquet (hoặc metadata_clean.parquet)")
        print("  - data/processed/reviews_clean.parquet (hoặc reviews_normalized.parquet)")
        print("\nNếu chưa có, hãy chạy các phase preprocessing:")
        print("  1. python scripts/data_preprocessing/phase1_ingest.py")
        print("  2. python scripts/data_preprocessing/phase2_normalize.py")
        print("  3. python scripts/data_preprocessing/phase3_cleaning.py")
        print("  4. python scripts/data_preprocessing/phase4_build_interactions.py")
        return False
    
    # Tạo engine
    try:
        # Kiểm tra xem có phải Render database không (cần SSL)
        connect_args = {}
        if "render.com" in async_url.lower():
            connect_args = {"ssl": "require"}
            print("\n📍 Phát hiện Render database - sử dụng SSL connection")
        
        engine = create_async_engine(async_url, echo=False, connect_args=connect_args)
        print("\n[0] Đang kết nối database...")
        
        # Test connection
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        print("✅ Kết nối thành công!")
        
        # Load products/items
        if data_files["items_file"]:
            print(f"\n[1] Đang load products từ {data_files['items_file'].name}...")
            try:
                await load_products(engine, data_files["items_file"])
            except Exception as e:
                print(f"⚠️  Lỗi khi load products: {e}")
                import traceback
                traceback.print_exc()
        
        # Load reviews và items từ reviews
        if data_files["reviews_file"]:
            print(f"\n[2] Đang load items từ reviews data...")
            try:
                await load_items_from_reviews(engine, data_files["reviews_file"])
            except Exception as e:
                print(f"⚠️  Lỗi khi load items từ reviews: {e}")
                import traceback
                traceback.print_exc()
            
            print(f"\n[3] Đang load reviews...")
            try:
                await load_reviews(engine, data_files["reviews_file"])
            except Exception as e:
                print(f"⚠️  Lỗi khi load reviews: {e}")
                import traceback
                traceback.print_exc()
        
        await engine.dispose()
        
        print("\n" + "=" * 80)
        print("✅ LOAD DỮ LIỆU THÀNH CÔNG!")
        print("=" * 80)
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Hàm chính."""
    # Lấy database URL từ environment hoặc argument
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        print("❌ ERROR: Chưa có DATABASE_URL")
        print("\nUsage:")
        print("  Set environment variable:")
        print("    $env:DATABASE_URL='postgresql://user:pass@host/db'")
        print("  Then run:")
        print("    python backend/scripts/load_data_to_db.py")
        sys.exit(1)
    
    success = await load_data_to_database(database_url)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())

