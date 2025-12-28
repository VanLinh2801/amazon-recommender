"""
Script setup database: chạy schema và có thể load dữ liệu.

Usage:
    # Chỉ chạy schema
    python backend/scripts/setup_database.py
    
    # Chạy schema với database URL cụ thể
    python backend/scripts/setup_database.py "postgresql://user:pass@host/db"
    
    # Chạy schema + load dữ liệu
    python backend/scripts/setup_database.py --load-data
"""
import sys
import asyncio
import os
import io
import argparse
from pathlib import Path

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm backend vào path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

# Import từ init_postgres.py
from app.db.init_postgres import (
    execute_schema,
    get_project_root
)


def convert_to_asyncpg_url(url: str) -> str:
    """
    Convert PostgreSQL URL sang asyncpg format nếu cần.
    
    Args:
        url: Connection string (có thể là postgresql:// hoặc postgresql+asyncpg://)
        
    Returns:
        Connection string với asyncpg driver
    """
    if url.startswith("postgresql://"):
        # Convert sang postgresql+asyncpg://
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql+asyncpg://"):
        # Đã đúng format
        return url
    else:
        raise ValueError(f"Invalid database URL format: {url}")


async def setup_database(database_url: str, load_data: bool = False):
    """
    Setup database: chạy schema và có thể load dữ liệu.
    
    Args:
        database_url: Connection string
        load_data: Có load dữ liệu không (cần parquet files)
    """
    print("=" * 80)
    print("SETUP DATABASE")
    print("=" * 80)
    
    # Convert URL nếu cần
    try:
        async_url = convert_to_asyncpg_url(database_url)
    except ValueError as e:
        print(f"❌ ERROR: {e}")
        return False
    
    # Ẩn password trong log
    safe_url = database_url.split("@")[1] if "@" in database_url else database_url
    print(f"\n📍 Database: {safe_url}")
    
    # Tìm project root
    project_root = get_project_root()
    schema_file = project_root / "database.sql"
    
    if not schema_file.exists():
        print(f"❌ ERROR: Không tìm thấy schema file: {schema_file}")
        return False
    
    # Tạo engine
    try:
        engine = create_async_engine(async_url, echo=False)
        print("\n[0] Đang kết nối database...")
        
        # Test connection
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        print("✅ Kết nối thành công!")
        
        # Chạy schema
        print("\n[1] Đang chạy schema...")
        await execute_schema(engine, schema_file)
        
        # Load dữ liệu nếu được yêu cầu
        if load_data:
            print("\n[2] Đang load dữ liệu...")
            try:
                from app.db.init_postgres import (
                    load_products,
                    load_items_from_reviews,
                    load_reviews
                )
                
                items_file = project_root / "data" / "processed" / "items_for_rs.parquet"
                reviews_file = project_root / "data" / "processed" / "reviews_clean.parquet"
                
                if items_file.exists():
                    await load_products(engine, items_file)
                else:
                    print(f"⚠️  Không tìm thấy: {items_file}")
                
                if reviews_file.exists():
                    await load_items_from_reviews(engine, reviews_file)
                    await load_reviews(engine, reviews_file)
                else:
                    print(f"⚠️  Không tìm thấy: {reviews_file}")
                    
            except ImportError as e:
                print(f"⚠️  Không thể import load functions: {e}")
                print("   Bỏ qua load dữ liệu")
        
        await engine.dispose()
        
        print("\n" + "=" * 80)
        print("✅ SETUP DATABASE THÀNH CÔNG!")
        print("=" * 80)
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Hàm chính."""
    parser = argparse.ArgumentParser(description="Setup database schema")
    parser.add_argument(
        "database_url",
        nargs="?",
        help="Database connection string (hoặc dùng DATABASE_URL env var)"
    )
    parser.add_argument(
        "--load-data",
        action="store_true",
        help="Load dữ liệu từ parquet files (nếu có)"
    )
    
    args = parser.parse_args()
    
    # Lấy database URL
    database_url = args.database_url or os.getenv("DATABASE_URL")
    
    if not database_url:
        print("❌ ERROR: Chưa có DATABASE_URL")
        print("\nUsage:")
        print("  python backend/scripts/setup_database.py <DATABASE_URL>")
        print("  python backend/scripts/setup_database.py <DATABASE_URL> --load-data")
        print("\nHoặc set environment variable:")
        print("  export DATABASE_URL='postgresql://user:pass@host/db'")
        print("  python backend/scripts/setup_database.py")
        sys.exit(1)
    
    success = await setup_database(database_url, load_data=args.load_data)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())

