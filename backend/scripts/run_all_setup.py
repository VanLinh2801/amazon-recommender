"""
Script tổng hợp để chạy tất cả các bước setup:
1. Preprocessing (Phase 1-5)
2. Setup database schema
3. Load dữ liệu vào database
4. Chạy migration
5. Chạy các scripts cập nhật category

Usage:
    python backend/scripts/run_all_setup.py
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


def run_script(script_path: Path, description: str) -> bool:
    """
    Chạy một Python script và trả về True nếu thành công.
    
    Args:
        script_path: Đường dẫn đến script
        description: Mô tả script
        
    Returns:
        True nếu thành công, False nếu có lỗi
    """
    print(f"\n{'=' * 80}")
    print(f"[{description}]")
    print(f"{'=' * 80}")
    print(f"Đang chạy: {script_path}")
    
    if not script_path.exists():
        print(f"❌ Không tìm thấy file: {script_path}")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=False,  # Hiển thị output trực tiếp
            text=True
        )
        print(f"✅ {description} hoàn thành!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi khi chạy {description}: {e}")
        return False
    except Exception as e:
        print(f"❌ Lỗi không mong đợi: {e}")
        import traceback
        traceback.print_exc()
        return False


async def setup_database_schema(database_url: str) -> bool:
    """Setup database schema."""
    print(f"\n{'=' * 80}")
    print("[SETUP DATABASE SCHEMA]")
    print(f"{'=' * 80}")
    
    # Import setup_database function
    sys.path.insert(0, str(BASE_DIR / "scripts"))
    from setup_database import setup_database
    
    success = await setup_database(database_url, load_data=False)
    return success


async def load_data_to_database(database_url: str) -> bool:
    """Load dữ liệu vào database."""
    print(f"\n{'=' * 80}")
    print("[LOAD DATA TO DATABASE]")
    print(f"{'=' * 80}")
    
    # Import load_data_to_database function
    sys.path.insert(0, str(BASE_DIR / "scripts"))
    from load_data_to_db import load_data_to_database as load_func
    success = await load_func(database_url)
    return success


async def run_migration_and_update_category(database_url: str) -> bool:
    """Chạy migration và update category."""
    print(f"\n{'=' * 80}")
    print("[MIGRATION & UPDATE CATEGORY]")
    print(f"{'=' * 80}")
    
    # Import và chạy migration script
    migration_script = PROJECT_ROOT / "scripts" / "database" / "scripts" / "run_migration_and_update_category.py"
    
    if not migration_script.exists():
        print(f"❌ Không tìm thấy migration script: {migration_script}")
        return False
    
    # Chạy script migration (nó sẽ tự xử lý async)
    try:
        result = subprocess.run(
            [sys.executable, str(migration_script)],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=False,
            text=True
        )
        print("✅ Migration và update category hoàn thành!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi khi chạy migration: {e}")
        return False


async def main():
    """Hàm chính để chạy tất cả các bước setup."""
    print("=" * 80)
    print("SETUP HOÀN CHỈNH - CHẠY TẤT CẢ CÁC BƯỚC")
    print("=" * 80)
    
    # Lấy database URL
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("❌ ERROR: Chưa có DATABASE_URL")
        print("\nVui lòng set environment variable:")
        print("  $env:DATABASE_URL='postgresql://user:pass@host/db'")
        return False
    
    print(f"\n📍 Database: {database_url.split('@')[1] if '@' in database_url else database_url}")
    print(f"📍 Project root: {PROJECT_ROOT}")
    
    # Danh sách các bước cần chạy
    steps = []
    
    # Step 1-5: Preprocessing
    preprocessing_scripts = [
        ("Phase 1: Ingest", PROJECT_ROOT / "scripts" / "data_preprocessing" / "phase1_ingest.py"),
        ("Phase 2: Normalize", PROJECT_ROOT / "scripts" / "data_preprocessing" / "phase2_normalize.py"),
        ("Phase 3: Cleaning", PROJECT_ROOT / "scripts" / "data_preprocessing" / "phase3_cleaning.py"),
        ("Phase 4: Build Interactions", PROJECT_ROOT / "scripts" / "data_preprocessing" / "phase4_build_interactions.py"),
        ("Phase 5: Build 5-Core", PROJECT_ROOT / "scripts" / "data_preprocessing" / "phase5_build_5core.py"),
    ]
    
    print(f"\n{'=' * 80}")
    print("BƯỚC 1-5: PREPROCESSING")
    print(f"{'=' * 80}")
    
    for desc, script_path in preprocessing_scripts:
        if not run_script(script_path, desc):
            print(f"\n❌ Dừng lại do lỗi ở {desc}")
            return False
    
    # Step 6: Setup database schema
    print(f"\n{'=' * 80}")
    print("BƯỚC 6: SETUP DATABASE SCHEMA")
    print(f"{'=' * 80}")
    
    if not await setup_database_schema(database_url):
        print("\n❌ Dừng lại do lỗi khi setup database schema")
        return False
    
    # Step 7: Load data vào database
    print(f"\n{'=' * 80}")
    print("BƯỚC 7: LOAD DATA VÀO DATABASE")
    print(f"{'=' * 80}")
    
    if not await load_data_to_database(database_url):
        print("\n❌ Dừng lại do lỗi khi load dữ liệu")
        return False
    
    # Step 8: Chạy migration và update category
    print(f"\n{'=' * 80}")
    print("BƯỚC 8: MIGRATION & UPDATE CATEGORY")
    print(f"{'=' * 80}")
    
    if not await run_migration_and_update_category(database_url):
        print("\n⚠️  Có lỗi khi chạy migration, nhưng tiếp tục...")
        # Không dừng lại vì có thể migration đã chạy rồi
    
    # Step 9: Update products category (optional)
    print(f"\n{'=' * 80}")
    print("BƯỚC 9: UPDATE PRODUCTS CATEGORY (Optional)")
    print(f"{'=' * 80}")
    
    update_products_script = PROJECT_ROOT / "scripts" / "database" / "scripts" / "update_products_category.py"
    if update_products_script.exists():
        print("⚠️  Script update_products_category.py cần chạy với --execute flag")
        print("   Bạn có thể chạy thủ công sau:")
        print(f"   python {update_products_script} --execute")
    else:
        print("⚠️  Không tìm thấy script update_products_category.py")
    
    # Hoàn thành
    print("\n" + "=" * 80)
    print("✅ HOÀN TẤT TẤT CẢ CÁC BƯỚC SETUP!")
    print("=" * 80)
    print("\nCác bước đã hoàn thành:")
    print("  ✅ Phase 1-5: Preprocessing")
    print("  ✅ Setup database schema")
    print("  ✅ Load dữ liệu vào database")
    print("  ✅ Migration và update category")
    print("\nCác bước tùy chọn:")
    print("  ⚠️  Update products category (chạy thủ công nếu cần)")
    
    return True


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Đã dừng bởi người dùng (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Lỗi không mong đợi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

