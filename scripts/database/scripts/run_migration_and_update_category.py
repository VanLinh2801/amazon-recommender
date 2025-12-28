"""
Run Migration and Update Category
==================================
Script để chạy migration thêm cột category và cập nhật dữ liệu.

Chạy: python app/database/scripts/run_migration_and_update_category.py
"""

import sys
import asyncio
from pathlib import Path
import os
import io

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm project root vào path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Thay đổi working directory về project root
os.chdir(project_root)

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

# Import settings - đọc trực tiếp từ config file
try:
    from app.config import settings
    DATABASE_URL = settings.database_url
except ImportError:
    # Fallback: đọc trực tiếp từ file config
    import importlib.util
    config_path = project_root / "app" / "config.py"
    if config_path.exists():
        spec = importlib.util.spec_from_file_location("config", config_path)
        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)
        DATABASE_URL = config_module.settings.database_url
    else:
        # Fallback cuối cùng: đọc từ environment
        DATABASE_URL = os.getenv(
            "DATABASE_URL",
            "postgresql+asyncpg://postgres:VanLinh04@localhost:5432/recommender"
        )
        print(f"[WARNING] Could not find config file, using DATABASE_URL from environment")


async def run_migration():
    """Chạy migration để thêm cột category."""
    print("=" * 80)
    print("RUNNING MIGRATION: Add category column to items table")
    print("=" * 80)
    
    engine = create_async_engine(
        DATABASE_URL,
        echo=False
    )
    
    migration_file = project_root / 'app' / 'database' / 'migrations' / 'add_category_to_items.sql'
    # Nếu không tìm thấy, thử đường dẫn khác
    if not migration_file.exists():
        migration_file = Path(__file__).resolve().parent.parent / 'migrations' / 'add_category_to_items.sql'
    
    if not migration_file.exists():
        print(f"[ERROR] Không tìm thấy file migration: {migration_file}")
        await engine.dispose()
        return False
    
    print(f"\n[1] Đang đọc migration file: {migration_file}")
    with open(migration_file, 'r', encoding='utf-8') as f:
        migration_sql = f.read()
    
    print("[2] Đang chạy migration...")
    try:
        async with engine.begin() as conn:
            await conn.execute(text(migration_sql))
        print("[OK] Migration đã chạy thành công!")
        await engine.dispose()
        return True
    except Exception as e:
        print(f"[ERROR] Lỗi khi chạy migration: {e}")
        import traceback
        traceback.print_exc()
        await engine.dispose()
        return False


async def main():
    """Hàm chính."""
    # Chạy migration
    migration_success = await run_migration()
    
    if not migration_success:
        print("\n[ERROR] Migration thất bại, không thể tiếp tục!")
        return False
    
    # Import và chạy update category
    print("\n" + "=" * 80)
    print("UPDATING CATEGORY FROM SEMANTIC ATTRIBUTES")
    print("=" * 80)
    
    # Import trực tiếp từ file (dùng đường dẫn tương đối từ script hiện tại)
    import importlib.util
    script_dir = Path(__file__).resolve().parent
    update_script_path = script_dir / 'update_items_category.py'
    
    if not update_script_path.exists():
        print(f"[ERROR] Không tìm thấy file: {update_script_path}")
        return False
    
    spec = importlib.util.spec_from_file_location("update_items_category", update_script_path)
    update_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(update_module)
    
    update_success = await update_module.update_items_category()
    
    if update_success:
        print("\n" + "=" * 80)
        print("[OK] HOÀN TẤT: Migration và cập nhật category đã thành công!")
        print("=" * 80)
        return True
    else:
        print("\n[ERROR] Cập nhật category thất bại!")
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n[INFO] Đã dừng bởi người dùng")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Lỗi không mong đợi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

