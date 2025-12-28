"""
Filter Embedding Text by Products in Database
=============================================
Script để filter embedding_text.parquet, chỉ giữ lại các products còn lại trong database.

Logic:
1. Lấy danh sách parent_asin còn lại trong database (từ bảng products)
2. Filter embedding_text.parquet để chỉ giữ lại các products có parent_asin trong danh sách đó
3. Backup file cũ và lưu file mới

Chạy: python app/database/scripts/filter_embedding_text_by_products.py
"""

import sys
import io
from pathlib import Path
import polars as pl
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add project root to path
script_path = Path(__file__).resolve()
project_root = script_path.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Try to import settings
try:
    from app.config import settings
    DATABASE_URL = settings.database_url
except ImportError:
    # Fallback: đọc trực tiếp từ file config
    import importlib.util
    import os
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
except Exception as e:
    # Fallback cuối cùng: đọc từ environment
    import os
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:VanLinh04@localhost:5432/recommender"
    )
    print(f"[WARNING] Error importing settings: {e}, using DATABASE_URL from environment")


async def get_products_in_database(db: AsyncSession) -> set[str]:
    """
    Lấy danh sách parent_asin còn lại trong database.
    
    Args:
        db: Database session
        
    Returns:
        Set of parent_asin strings
    """
    result = await db.execute(
        text("""
            SELECT DISTINCT parent_asin
            FROM products
        """)
    )
    
    rows = result.fetchall()
    parent_asins = set(str(row.parent_asin) for row in rows)
    
    return parent_asins


def filter_embedding_text(
    project_root: Path,
    products_in_db: set[str],
    dry_run: bool = True
) -> dict:
    """
    Filter embedding_text.parquet để chỉ giữ lại products còn lại trong database.
    
    Args:
        project_root: Project root path
        products_in_db: Set of parent_asin strings còn lại trong database
        dry_run: Nếu True, chỉ log không thực sự filter
        
    Returns:
        Dict với thống kê
    """
    embedding_text_file = project_root / "data" / "embedding" / "embedding_text.parquet"
    
    if not embedding_text_file.exists():
        print("⚠️  Không tìm thấy embedding_text.parquet, skip filter")
        return {"filtered": 0, "kept": 0, "removed": 0}
    
    print("\n" + "=" * 80)
    print("FILTER EMBEDDING_TEXT.PARQUET BY PRODUCTS IN DATABASE")
    print("=" * 80)
    
    # Load embedding_text.parquet
    print(f"Đang load embedding_text từ {embedding_text_file}...")
    df = pl.read_parquet(str(embedding_text_file))
    print(f"  Số items ban đầu: {len(df):,}")
    print(f"  Columns: {df.columns}")
    
    # Kiểm tra cột item_id hoặc parent_asin
    item_id_col = None
    if "parent_asin" in df.columns:
        item_id_col = "parent_asin"
    elif "item_id" in df.columns:
        item_id_col = "item_id"
    else:
        print("⚠️  Không tìm thấy cột parent_asin hoặc item_id, skip filter")
        return {"filtered": 0, "kept": 0, "removed": 0}
    
    print(f"  Sử dụng cột: {item_id_col}")
    print(f"  Products trong database: {len(products_in_db):,}")
    
    # Filter: chỉ giữ lại products có trong database
    print(f"\nĐang filter embedding_text...")
    
    # Convert item_id_col sang string để so sánh
    df_filtered = df.filter(
        pl.col(item_id_col).cast(pl.Utf8).is_in(list(products_in_db))
    )
    
    removed_count = len(df) - len(df_filtered)
    
    print(f"  Items sẽ được GIỮ LẠI: {len(df_filtered):,}")
    print(f"  Items sẽ bị XÓA: {removed_count:,}")
    
    if removed_count > 0:
        # Lấy sample items sẽ bị xóa
        df_removed = df.filter(
            ~pl.col(item_id_col).cast(pl.Utf8).is_in(list(products_in_db))
        )
        removed_items = df_removed[item_id_col].head(10).to_list()
        print(f"\nSample items sẽ bị xóa (10 đầu):")
        for item in removed_items:
            print(f"    - {item}")
    
    if dry_run:
        print("\n[DRY RUN] Không thực sự filter embedding_text. Để filter thật, chạy với --execute")
        return {
            "filtered": 0,
            "kept": len(df_filtered),
            "removed": removed_count,
            "dry_run": True
        }
    
    # Backup file cũ
    import shutil
    from datetime import datetime
    backup_dir = embedding_text_file.parent / "backup"
    backup_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    backup_file = backup_dir / f"embedding_text_backup_{timestamp}.parquet"
    
    print(f"\nĐang backup file cũ...")
    shutil.copy2(embedding_text_file, backup_file)
    print(f"  Backup: {backup_file}")
    
    # Lưu filtered embedding_text
    print(f"\nĐang lưu filtered embedding_text...")
    df_filtered.write_parquet(str(embedding_text_file))
    print(f"  [OK] Đã lưu {len(df_filtered):,} items vào {embedding_text_file}")
    
    return {
        "filtered": len(df_filtered),
        "kept": len(df_filtered),
        "removed": removed_count,
        "dry_run": False
    }


async def main():
    """Hàm chính."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Filter embedding_text.parquet theo products còn lại trong database")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Thực sự filter (mặc định là dry-run)"
    )
    args = parser.parse_args()
    
    print("=" * 80)
    print("FILTER EMBEDDING_TEXT BY PRODUCTS IN DATABASE")
    print("=" * 80)
    
    # Kết nối database
    engine = create_async_engine(DATABASE_URL, echo=False)
    AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with AsyncSessionLocal() as db:
        try:
            # Lấy danh sách products còn lại trong database
            print("\nĐang lấy danh sách products từ database...")
            products_in_db = await get_products_in_database(db)
            print(f"  Tìm thấy {len(products_in_db):,} products trong database")
            
            # Filter embedding_text
            stats = filter_embedding_text(
                project_root=project_root,
                products_in_db=products_in_db,
                dry_run=not args.execute
            )
            
            print("\n" + "=" * 80)
            print("KẾT QUẢ")
            print("=" * 80)
            print(f"\n[Filter Embedding Text]")
            print(f"  Items được giữ lại: {stats['kept']:,}")
            print(f"  Items bị xóa: {stats['removed']:,}")
            
            if stats.get("dry_run"):
                print("\n⚠️  Đây là DRY RUN. Để thực sự filter, chạy với --execute")
            
        except Exception as e:
            print(f"❌ Lỗi: {e}")
            import traceback
            traceback.print_exc()
            await db.rollback()
            sys.exit(1)
        finally:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

