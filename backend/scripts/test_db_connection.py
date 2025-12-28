"""
Script test kết nối database.

Usage:
    # Từ environment variable
    python backend/scripts/test_db_connection.py
    
    # Hoặc truyền connection string trực tiếp
    python backend/scripts/test_db_connection.py "postgresql+asyncpg://user:pass@host/db"
"""
import sys
import asyncio
import os
import io
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


async def test_connection(database_url: str):
    """Test kết nối database."""
    print("=" * 60)
    print("TEST KẾT NỐI DATABASE")
    print("=" * 60)
    
    # Convert URL nếu cần
    try:
        async_url = convert_to_asyncpg_url(database_url)
    except ValueError as e:
        print(f"❌ ERROR: {e}")
        return False
    
    # Ẩn password trong log
    safe_url = database_url.split("@")[1] if "@" in database_url else database_url
    print(f"\n📍 Database: {safe_url}")
    print(f"🔗 URL format: {'✅ postgresql+asyncpg://' if 'asyncpg' in async_url else '⚠️  postgresql:// (sẽ convert)'}")
    
    # Tạo engine
    try:
        engine = create_async_engine(async_url, echo=False)
        print("\n[1] Đang kết nối...")
        
        # Test connection
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT version()"))
            version = result.scalar()
            print(f"✅ Kết nối thành công!")
            print(f"\n📊 PostgreSQL Version:")
            print(f"   {version}")
            
            # Kiểm tra database name
            result = await conn.execute(text("SELECT current_database()"))
            db_name = result.scalar()
            print(f"\n📁 Database name: {db_name}")
            
            # Kiểm tra tables
            result = await conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))
            tables = [row[0] for row in result.fetchall()]
            
            if tables:
                print(f"\n📋 Tables trong database ({len(tables)}):")
                for table in tables:
                    print(f"   - {table}")
            else:
                print(f"\n⚠️  Chưa có tables nào trong database")
                print(f"   → Cần chạy schema từ database.sql")
        
        await engine.dispose()
        print("\n" + "=" * 60)
        print("✅ TEST THÀNH CÔNG!")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: Không thể kết nối database")
        print(f"   Chi tiết: {str(e)}")
        print("\n💡 Kiểm tra:")
        print("   1. Database URL đúng chưa?")
        print("   2. Database đã được tạo chưa?")
        print("   3. Firewall/network có cho phép connection không?")
        print("   4. Username/password đúng chưa?")
        return False


async def main():
    """Hàm chính."""
    # Lấy database URL từ command line hoặc environment
    if len(sys.argv) > 1:
        database_url = sys.argv[1]
    else:
        # Thử từ environment variable
        database_url = os.getenv("DATABASE_URL")
        
        if not database_url:
            print("❌ ERROR: Chưa có DATABASE_URL")
            print("\nUsage:")
            print("  python backend/scripts/test_db_connection.py <DATABASE_URL>")
            print("\nHoặc set environment variable:")
            print("  export DATABASE_URL='postgresql://user:pass@host/db'")
            print("  python backend/scripts/test_db_connection.py")
            sys.exit(1)
    
    success = await test_connection(database_url)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())

