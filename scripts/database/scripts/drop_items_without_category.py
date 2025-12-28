"""
Script để drop các items không có category, nhưng giữ lại items có trong interaction5core.

Logic:
1. Load danh sách item_id từ interactions_5core.parquet
2. Kiểm tra database để tìm items không có category
3. Drop items không có category, NHƯNG giữ lại items có trong interaction5core
4. Xóa items không có category khỏi embedding data (item_embeddings.npy và item_ids.json)
"""

import sys
import io
import json
from pathlib import Path
import polars as pl
import numpy as np
import asyncio
import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

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
except Exception:
    import os
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/recommender")


def load_5core_items(project_root: Path) -> tuple[set[str], set[str]]:
    """
    Load danh sách item_id từ interactions_5core.parquet.
    
    Args:
        project_root: Project root path
        
    Returns:
        Tuple of (set of item_id (parent_asin) strings, set of asin strings nếu có)
    """
    interactions_path = project_root / "data" / "processed" / "interactions_5core.parquet"
    
    if not interactions_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {interactions_path}")
    
    print(f"Đang load items từ {interactions_path}...")
    df = pl.read_parquet(str(interactions_path))
    
    # Lấy unique item_id (thường là parent_asin)
    item_ids = df["item_id"].unique().to_list()
    item_set = set(str(item_id) for item_id in item_ids)
    
    # Nếu có cột asin, cũng lấy luôn
    asin_set = set()
    if "asin" in df.columns:
        asins = df["asin"].unique().to_list()
        asin_set = set(str(asin) for asin in asins if asin is not None)
    
    print(f"  Tìm thấy {len(item_set):,} unique item_id (parent_asin) trong interaction5core")
    if asin_set:
        print(f"  Tìm thấy {len(asin_set):,} unique asin trong interaction5core")
    
    return item_set, asin_set


async def get_items_with_category(db: AsyncSession) -> set[str]:
    """
    Lấy danh sách items CÓ category từ database (parent_asin hoặc asin).
    
    Args:
        db: Database session
        
    Returns:
        Set of parent_asin và asin strings có category
    """
    result = await db.execute(
        text("""
            SELECT DISTINCT
                i.asin,
                i.parent_asin,
                i.category,
                p.main_category
            FROM items i
            JOIN products p ON i.parent_asin = p.parent_asin
            WHERE (i.category IS NOT NULL AND i.category != '')
               OR (p.main_category IS NOT NULL AND p.main_category != '')
        """)
    )
    
    rows = result.fetchall()
    items_with_category = set()
    
    for row in rows:
        if row.asin:
            items_with_category.add(row.asin)
        if row.parent_asin:
            items_with_category.add(row.parent_asin)
    
    return items_with_category


async def get_items_without_category(db: AsyncSession) -> list[dict]:
    """
    Lấy danh sách items không có category từ database.
    
    Args:
        db: Database session
        
    Returns:
        List of dict với asin và parent_asin
    """
    result = await db.execute(
        text("""
            SELECT 
                i.asin,
                i.parent_asin,
                i.category,
                p.main_category
            FROM items i
            JOIN products p ON i.parent_asin = p.parent_asin
            WHERE (i.category IS NULL OR i.category = '')
              AND (p.main_category IS NULL OR p.main_category = '')
        """)
    )
    
    rows = result.fetchall()
    items = [
        {
            "asin": row.asin,
            "parent_asin": row.parent_asin,
            "category": row.category,
            "main_category": row.main_category
        }
        for row in rows
    ]
    
    return items


async def drop_items_without_category(
    db: AsyncSession,
    items_to_drop: list[dict],
    dry_run: bool = True
) -> dict:
    """
    Drop items không có category.
    
    Args:
        db: Database session
        items_to_drop: List of items cần drop
        dry_run: Nếu True, chỉ log không thực sự drop
        
    Returns:
        Dict với thống kê
    """
    if not items_to_drop:
        print("Không có items nào cần drop.")
        return {"dropped": 0, "skipped": 0}
    
    asins_to_drop = [item["asin"] for item in items_to_drop]
    
    # Kiểm tra xem có items nào đang được reference không
    result = await db.execute(
        text("""
            SELECT COUNT(*) as count
            FROM interaction_logs
            WHERE asin = ANY(:asins)
        """),
        {"asins": asins_to_drop}
    )
    interaction_count = result.fetchone().count
    
    result = await db.execute(
        text("""
            SELECT COUNT(*) as count
            FROM cart_items
            WHERE asin = ANY(:asins)
        """),
        {"asins": asins_to_drop}
    )
    cart_count = result.fetchone().count
    
    print(f"\nThống kê items cần drop:")
    print(f"  Tổng số items: {len(items_to_drop):,}")
    print(f"  Items có trong interaction_logs: {interaction_count:,}")
    print(f"  Items có trong cart_items: {cart_count:,}")
    
    if dry_run:
        print("\n[DRY RUN] Không thực sự drop items. Để drop thật, chạy với --execute")
        return {"dropped": 0, "skipped": len(items_to_drop), "dry_run": True}
    
    # Drop items
    print("\nĐang drop items...")
    
    # 1. Drop từ cart_items (nếu có)
    if cart_count > 0:
        await db.execute(
            text("""
                DELETE FROM cart_items
                WHERE asin = ANY(:asins)
            """),
            {"asins": asins_to_drop}
        )
        print(f"  Đã xóa {cart_count:,} items từ cart_items")
    
    # 2. Drop từ items table
    result = await db.execute(
        text("""
            DELETE FROM items
            WHERE asin = ANY(:asins)
        """),
        {"asins": asins_to_drop}
    )
    dropped_count = result.rowcount
    print(f"  Đã xóa {dropped_count:,} items từ items table")
    
    await db.commit()
    
    return {"dropped": dropped_count, "skipped": 0, "dry_run": False}


def filter_embedding_text(
    project_root: Path,
    items_with_category: set[str],
    dry_run: bool = True
) -> dict:
    """
    Filter embedding_text.parquet để chỉ giữ lại items có category.
    
    Args:
        project_root: Project root path
        items_with_category: Set of item IDs có category
        dry_run: Nếu True, chỉ log không thực sự filter
        
    Returns:
        Dict với thống kê
    """
    embedding_text_file = project_root / "data" / "embedding" / "embedding_text.parquet"
    
    if not embedding_text_file.exists():
        print("⚠️  Không tìm thấy embedding_text.parquet, skip filter")
        return {"filtered": 0, "kept": 0, "removed": 0}
    
    print("\n" + "=" * 80)
    print("FILTER EMBEDDING_TEXT.PARQUET")
    print("=" * 80)
    
    # Load embedding_text.parquet
    print(f"Đang load embedding_text từ {embedding_text_file}...")
    df = pl.read_parquet(str(embedding_text_file))
    print(f"  Số items ban đầu: {len(df):,}")
    print(f"  Columns: {df.columns}")
    
    # Kiểm tra cột item_id hoặc parent_asin
    item_id_col = None
    if "item_id" in df.columns:
        item_id_col = "item_id"
    elif "parent_asin" in df.columns:
        item_id_col = "parent_asin"
    else:
        print("⚠️  Không tìm thấy cột item_id hoặc parent_asin, skip filter")
        return {"filtered": 0, "kept": 0, "removed": 0}
    
    print(f"  Sử dụng cột: {item_id_col}")
    
    # Filter: chỉ giữ lại items có category
    print(f"\nĐang filter embedding_text...")
    print(f"  Items có category: {len(items_with_category):,}")
    
    # Convert item_id_col sang string để so sánh
    df_filtered = df.filter(
        pl.col(item_id_col).cast(pl.Utf8).is_in(list(items_with_category))
    )
    
    removed_count = len(df) - len(df_filtered)
    
    print(f"  Items sẽ được GIỮ LẠI: {len(df_filtered):,}")
    print(f"  Items sẽ bị XÓA: {removed_count:,}")
    
    if removed_count > 0:
        # Lấy sample items sẽ bị xóa
        df_removed = df.filter(
            ~pl.col(item_id_col).cast(pl.Utf8).is_in(list(items_with_category))
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


def filter_embeddings(
    project_root: Path,
    items_with_category: set[str],
    dry_run: bool = True
) -> dict:
    """
    Filter embeddings để chỉ giữ lại items có category.
    
    Args:
        project_root: Project root path
        items_with_category: Set of item IDs có category
        dry_run: Nếu True, chỉ log không thực sự filter
        
    Returns:
        Dict với thống kê
    """
    embeddings_dir = project_root / "artifacts" / "embeddings"
    embeddings_file = embeddings_dir / "item_embeddings.npy"
    item_ids_file = embeddings_dir / "item_ids.json"
    
    if not embeddings_file.exists() or not item_ids_file.exists():
        print("⚠️  Không tìm thấy embedding files, skip filter embeddings")
        return {"filtered": 0, "kept": 0, "removed": 0}
    
    print("\n" + "=" * 80)
    print("FILTER EMBEDDINGS")
    print("=" * 80)
    
    # Load embeddings và item_ids
    print(f"Đang load embeddings từ {embeddings_file}...")
    embeddings = np.load(str(embeddings_file))
    print(f"  Shape: {embeddings.shape}")
    
    print(f"Đang load item_ids từ {item_ids_file}...")
    with open(item_ids_file, 'r', encoding='utf-8') as f:
        item_ids = json.load(f)
    print(f"  Số item_ids: {len(item_ids):,}")
    
    # Kiểm tra số lượng khớp nhau
    if len(embeddings) != len(item_ids):
        print(f"⚠️  Cảnh báo: Số embeddings ({len(embeddings):,}) != số item_ids ({len(item_ids):,})")
        min_len = min(len(embeddings), len(item_ids))
        embeddings = embeddings[:min_len]
        item_ids = item_ids[:min_len]
        print(f"  Đã cắt về {min_len:,} items")
    
    # Filter: chỉ giữ lại items có category
    print(f"\nĐang filter embeddings...")
    print(f"  Items có category: {len(items_with_category):,}")
    
    keep_indices = []
    removed_items = []
    
    for idx, item_id in enumerate(item_ids):
        item_id_str = str(item_id)
        if item_id_str in items_with_category:
            keep_indices.append(idx)
        else:
            removed_items.append(item_id_str)
    
    print(f"  Items sẽ được GIỮ LẠI: {len(keep_indices):,}")
    print(f"  Items sẽ bị XÓA: {len(removed_items):,}")
    
    if removed_items:
        print(f"\nSample items sẽ bị xóa (10 đầu):")
        for item in removed_items[:10]:
            print(f"    - {item}")
    
    if dry_run:
        print("\n[DRY RUN] Không thực sự filter embeddings. Để filter thật, chạy với --execute")
        return {
            "filtered": 0,
            "kept": len(keep_indices),
            "removed": len(removed_items),
            "dry_run": True
        }
    
    # Filter embeddings và item_ids
    print("\nĐang filter embeddings và item_ids...")
    filtered_embeddings = embeddings[keep_indices]
    filtered_item_ids = [item_ids[i] for i in keep_indices]
    
    print(f"  Embeddings sau filter: {filtered_embeddings.shape}")
    print(f"  Item_ids sau filter: {len(filtered_item_ids):,}")
    
    # Backup files cũ
    import shutil
    from datetime import datetime
    backup_dir = embeddings_dir / "backup"
    backup_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    backup_embeddings = backup_dir / f"item_embeddings_backup_{timestamp}.npy"
    backup_item_ids = backup_dir / f"item_ids_backup_{timestamp}.json"
    
    print(f"\nĐang backup files cũ...")
    shutil.copy2(embeddings_file, backup_embeddings)
    shutil.copy2(item_ids_file, backup_item_ids)
    print(f"  Backup embeddings: {backup_embeddings}")
    print(f"  Backup item_ids: {backup_item_ids}")
    
    # Lưu filtered embeddings và item_ids
    print(f"\nĐang lưu filtered embeddings...")
    np.save(str(embeddings_file), filtered_embeddings)
    print(f"  [OK] Đã lưu embeddings: {filtered_embeddings.shape}")
    
    print(f"Đang lưu filtered item_ids...")
    with open(item_ids_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_item_ids, f, ensure_ascii=False, indent=2)
    print(f"  [OK] Đã lưu {len(filtered_item_ids):,} item_ids")
    
    return {
        "filtered": len(keep_indices),
        "kept": len(keep_indices),
        "removed": len(removed_items),
        "dry_run": False
    }


async def main():
    """Hàm chính."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Drop items không có category, giữ lại items trong interaction5core")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Thực sự drop items (mặc định là dry-run)"
    )
    args = parser.parse_args()
    
    print("=" * 80)
    print("DROP ITEMS KHÔNG CÓ CATEGORY (GIỮ LẠI ITEMS TRONG INTERACTION5CORE)")
    print("=" * 80)
    
    # Load 5core items
    try:
        items_5core_parent_asins, items_5core_asins = load_5core_items(project_root)
    except Exception as e:
        print(f"❌ Lỗi khi load interaction5core: {e}")
        sys.exit(1)
    
    # Kết nối database
    engine = create_async_engine(DATABASE_URL, echo=False)
    AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with AsyncSessionLocal() as db:
        try:
            # Lấy items không có category
            print("\nĐang kiểm tra items không có category trong database...")
            items_without_category = await get_items_without_category(db)
            print(f"  Tìm thấy {len(items_without_category):,} items không có category")
            
            # Filter: chỉ drop items KHÔNG có trong interaction5core
            # Kiểm tra cả parent_asin và asin
            items_to_drop = []
            items_to_keep = []
            
            for item in items_without_category:
                parent_asin = item["parent_asin"] or item["asin"]
                asin = item["asin"]
                
                # Giữ lại nếu parent_asin hoặc asin có trong interaction5core
                if (parent_asin in items_5core_parent_asins or 
                    asin in items_5core_asins or
                    parent_asin in items_5core_asins or
                    asin in items_5core_parent_asins):
                    items_to_keep.append(item)
                else:
                    items_to_drop.append(item)
            
            print(f"\nKết quả filter:")
            print(f"  Items sẽ được DROP: {len(items_to_drop):,}")
            print(f"  Items sẽ được GIỮ LẠI (có trong interaction5core): {len(items_to_keep):,}")
            
            if items_to_keep:
                print(f"\nItems được giữ lại (sample 10 đầu):")
                for item in items_to_keep[:10]:
                    print(f"  - ASIN: {item['asin']}, Parent: {item['parent_asin']}")
            
            # Drop items
            stats = await drop_items_without_category(
                db=db,
                items_to_drop=items_to_drop,
                dry_run=not args.execute
            )
            
            # Filter embedding_text.parquet
            items_with_category = await get_items_with_category(db)
            embedding_text_stats = filter_embedding_text(
                project_root=project_root,
                items_with_category=items_with_category,
                dry_run=not args.execute
            )
            
            # Filter embeddings (item_embeddings.npy và item_ids.json)
            embedding_stats = filter_embeddings(
                project_root=project_root,
                items_with_category=items_with_category,
                dry_run=not args.execute
            )
            
            print("\n" + "=" * 80)
            print("KẾT QUẢ")
            print("=" * 80)
            print(f"\n[Database]")
            print(f"  Items đã drop: {stats['dropped']:,}")
            print(f"  Items đã skip: {stats['skipped']:,}")
            
            print(f"\n[Embedding Text]")
            print(f"  Items được giữ lại: {embedding_text_stats['kept']:,}")
            print(f"  Items bị xóa: {embedding_text_stats['removed']:,}")
            
            print(f"\n[Embeddings (npy/json)]")
            print(f"  Items được giữ lại: {embedding_stats['kept']:,}")
            print(f"  Items bị xóa: {embedding_stats['removed']:,}")
            
            if stats.get("dry_run") or embedding_stats.get("dry_run"):
                print("\n⚠️  Đây là DRY RUN. Để thực sự drop items và filter embeddings, chạy với --execute")
            
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

