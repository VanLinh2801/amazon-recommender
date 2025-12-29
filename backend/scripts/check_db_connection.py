"""
Script kiểm tra kết nối database
=================================

Kiểm tra xem có thể kết nối đến PostgreSQL không.

Usage:
    python backend/scripts/check_db_connection.py
"""

import sys
import os
import asyncio
from pathlib import Path

# Thêm backend vào path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from app.config import settings
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text


def normalize_database_url(url: str) -> str:
    """
    Chuẩn hóa database URL:
    - Convert postgresql:// -> postgresql+asyncpg://
    - Thêm port 5432 nếu thiếu cho Render database
    """
    # Convert postgresql:// -> postgresql+asyncpg://
    if url.startswith("postgresql://") and "+asyncpg" not in url:
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    
    # Nếu là Render database và thiếu port, thêm port 5432
    if "render.com" in url.lower() and ":5432" not in url:
        # Tìm vị trí sau @ và trước /
        if "@" in url and "/" in url:
            at_pos = url.rfind("@")
            slash_pos = url.find("/", at_pos)
            if slash_pos > at_pos:
                # Chèn :5432 trước dấu /
                url = url[:slash_pos] + ":5432" + url[slash_pos:]
    
    return url


async def check_connection():
    """Kiểm tra kết nối database."""
    print("=" * 60)
    print("KIỂM TRA KẾT NỐI DATABASE")
    print("=" * 60)
    
    # Lấy database URL và normalize
    db_url = settings.database_url
    original_url = db_url
    
    # Normalize URL
    db_url = normalize_database_url(db_url)
    
    # Mask password trong URL để hiển thị
    if '@' in db_url:
        parts = db_url.split('@')
        user_pass = parts[0].split('//')[1] if '//' in parts[0] else parts[0]
        if ':' in user_pass:
            user = user_pass.split(':')[0]
            masked_url = db_url.replace(user_pass, f"{user}:***")
        else:
            masked_url = db_url.replace(user_pass, "***")
    else:
        masked_url = db_url
    
    print(f"\nOriginal URL: {original_url.replace(original_url.split('@')[0].split('//')[1] if '@' in original_url else '', '***') if '@' in original_url else original_url}")
    print(f"Normalized URL: {masked_url}")
    
    # Kiểm tra xem có phải Render database không
    is_render_db = 'render.com' in db_url.lower()
    if is_render_db:
        print("📍 Phát hiện Render database")
        print("   - Đã thêm port 5432 (nếu thiếu)")
        print("   - Sẽ thử kết nối với SSL")
    
    try:
        print("\nĐang thử kết nối...")
        
        # Render database thường cần SSL
        connect_args = {}
        if is_render_db:
            # Thử với SSL mode require
            connect_args = {
                "ssl": "require"
            }
            print("   Thử kết nối với SSL mode 'require'...")
        
        engine = create_async_engine(
            db_url,
            echo=False,
            pool_pre_ping=True,
            connect_args=connect_args if connect_args else {}
        )
        
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT version();"))
            version = result.scalar()
            print(f"✅ Kết nối thành công!")
            print(f"\nPostgreSQL version: {version}")
            
            # Kiểm tra database có tồn tại không
            result = await conn.execute(text("SELECT current_database();"))
            db_name = result.scalar()
            print(f"Database name: {db_name}")
            
        await engine.dispose()
        return True
        
    except ConnectionRefusedError as e:
        print(f"\n❌ Lỗi: Không thể kết nối đến PostgreSQL server")
        print(f"   Chi tiết: {e}")
        
        if is_render_db:
            print("\n🔧 CÁCH KHẮC PHỤC (Render Database):")
            print("   1. Kiểm tra DATABASE_URL có đúng không:")
            print("      - Format: postgresql+asyncpg://user:pass@host.region-postgres.render.com:5432/dbname")
            print("      - Đảm bảo có port :5432")
            print("      - Đảm bảo có region trong hostname (ví dụ: oregon-postgres.render.com)")
            print("   2. Kiểm tra database có đang active trên Render không:")
            print("      - Vào Render Dashboard > PostgreSQL service")
            print("      - Kiểm tra status phải là 'Active'")
            print("   3. Kiểm tra firewall/network:")
            print("      - Render database chỉ accept connections từ whitelisted IPs")
            print("      - Vào Render Dashboard > PostgreSQL > Settings > Network Access")
            print("      - Thêm IP của bạn vào whitelist (hoặc enable 'Allow connections from anywhere')")
            print("   4. Kiểm tra SSL connection:")
            print("      - Render database yêu cầu SSL")
            print("      - Script đã tự động thử với SSL mode 'require'")
        else:
            print("\n🔧 CÁCH KHẮC PHỤC (Local Database):")
            print("   1. Kiểm tra PostgreSQL đã được cài đặt chưa")
            print("   2. Kiểm tra PostgreSQL service đã chạy chưa:")
            print("      - Windows: Services > PostgreSQL")
            print("      - Linux: sudo systemctl status postgresql")
            print("      - Mac: brew services list")
            print("   3. Khởi động PostgreSQL nếu chưa chạy:")
            print("      - Windows: Services > PostgreSQL > Start")
            print("      - Linux: sudo systemctl start postgresql")
            print("      - Mac: brew services start postgresql")
            print("   4. Kiểm tra port 5432 có đang được sử dụng không")
        return False
        
    except Exception as e:
        print(f"\n❌ Lỗi kết nối: {type(e).__name__}")
        print(f"   Chi tiết: {e}")
        
        if is_render_db:
            print("\n🔧 CÁCH KHẮC PHỤC (Render Database):")
            print("   1. Kiểm tra DATABASE_URL:")
            print("      - Lấy từ Render Dashboard > PostgreSQL > Connection String")
            print("      - Đảm bảo format: postgresql+asyncpg://user:pass@host:5432/dbname")
            print("      - Set environment variable: $env:DATABASE_URL='...' (PowerShell)")
            print("   2. Kiểm tra Network Access trên Render:")
            print("      - Vào PostgreSQL service > Settings > Network Access")
            print("      - Enable 'Allow connections from anywhere' hoặc thêm IP của bạn")
            print("   3. Kiểm tra database status:")
            print("      - Database phải ở trạng thái 'Active'")
            print("      - Nếu 'Paused', click 'Resume' để khởi động lại")
            print("   4. Thử kết nối với psql để test:")
            print("      psql 'postgresql://user:pass@host:5432/dbname'")
        else:
            print("\n🔧 CÁCH KHẮC PHỤC (Local Database):")
            print("   1. Kiểm tra DATABASE_URL trong config.py hoặc environment variable")
            print("   2. Kiểm tra username, password, host, port có đúng không")
            print("   3. Kiểm tra database 'recommender' đã được tạo chưa")
            print("   4. Kiểm tra user có quyền truy cập database không")
        return False


async def main():
    """Hàm chính."""
    # Kiểm tra xem có DATABASE_URL trong environment không
    import os
    env_db_url = os.getenv("DATABASE_URL")
    if env_db_url:
        print(f"📌 Phát hiện DATABASE_URL trong environment variable")
        print(f"   Sẽ sử dụng URL từ environment thay vì config.py")
    
    success = await check_connection()
    
    if not success:
        print("\n" + "=" * 60)
        print("HƯỚNG DẪN THIẾT LẬP DATABASE")
        print("=" * 60)
        
        is_render = 'render.com' in (env_db_url or settings.database_url).lower()
        
        if is_render:
            print("\n🔧 CHO RENDER DATABASE:")
            print("\n1. Set DATABASE_URL trong PowerShell:")
            print('   $env:DATABASE_URL="postgresql+asyncpg://user:pass@host:5432/dbname"')
            print("\n2. Kiểm tra Network Access trên Render:")
            print("   - Vào Render Dashboard > PostgreSQL service")
            print("   - Settings > Network Access")
            print("   - Enable 'Allow connections from anywhere'")
            print("\n3. Kiểm tra database status:")
            print("   - Database phải ở trạng thái 'Active'")
            print("   - Nếu 'Paused', click 'Resume'")
            print("\n4. Test lại:")
            print("   python backend/scripts/check_db_connection.py")
        else:
            print("\n1. Tạo database:")
            print("   psql -U postgres")
            print("   CREATE DATABASE recommender;")
            print("   \\q")
            print("\n2. Hoặc chạy script setup:")
            print("   python backend/scripts/setup_database.py")
            print("\n3. Kiểm tra lại kết nối:")
            print("   python backend/scripts/check_db_connection.py")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())

