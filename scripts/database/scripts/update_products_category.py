"""
Update Products Category from Items Category
===========================================
Script để cập nhật main_category của products từ category của items.

Logic:
1. Lấy category từ items.category (category chi tiết từ semantic_attributes)
2. Cập nhật products.main_category với category chi tiết nhất từ các items của product đó
3. Drop products không có category (sau khi update)

Chạy: python app/database/scripts/update_products_category.py
"""

import sys
import asyncio
from pathlib import Path
import os
import io
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
except Exception:
    import os
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/recommender")


async def update_products_category_from_items(db: AsyncSession, dry_run: bool = True) -> dict:
    """
    Cập nhật products.main_category từ items.category.
    
    Logic:
    - Lấy category chi tiết nhất từ các items của product
    - Nếu product có nhiều items với categories khác nhau, lấy category phổ biến nhất
    - Nếu không có category nào, giữ nguyên hoặc set NULL
    
    Args:
        db: Database session
        dry_run: Nếu True, chỉ log không thực sự update
        
    Returns:
        Dict với thống kê
    """
    print("\n" + "=" * 80)
    print("UPDATE PRODUCTS.MAIN_CATEGORY FROM ITEMS.CATEGORY")
    print("=" * 80)
    
    # Lấy category từ items cho mỗi product
    # Lấy category phổ biến nhất (mode) từ các items của product
    result = await db.execute(
        text("""
            SELECT 
                p.parent_asin,
                p.main_category as old_category,
                i.category as item_category,
                COUNT(*) as item_count
            FROM products p
            LEFT JOIN items i ON p.parent_asin = i.parent_asin
            WHERE i.category IS NOT NULL AND i.category != ''
            GROUP BY p.parent_asin, p.main_category, i.category
            ORDER BY p.parent_asin, COUNT(*) DESC
        """)
    )
    
    rows = result.fetchall()
    
    # Tạo mapping: parent_asin -> category (lấy category phổ biến nhất)
    product_categories = {}
    for row in rows:
        parent_asin = row.parent_asin
        item_category = row.item_category
        
        if parent_asin not in product_categories:
            product_categories[parent_asin] = {
                "category": item_category,
                "count": row.item_count,
                "old_category": row.old_category
            }
        else:
            # Nếu category này phổ biến hơn, cập nhật
            if row.item_count > product_categories[parent_asin]["count"]:
                product_categories[parent_asin] = {
                    "category": item_category,
                    "count": row.item_count,
                    "old_category": row.old_category
                }
    
    print(f"\nTìm thấy {len(product_categories):,} products có category từ items")
    
    # Thống kê
    updated_count = 0
    unchanged_count = 0
    
    for parent_asin, data in product_categories.items():
        old_cat = data["old_category"]
        new_cat = data["category"]
        
        # Chỉ update nếu category khác nhau và không phải "All Beauty"
        if old_cat != new_cat and old_cat != "All Beauty":
            unchanged_count += 1
        elif old_cat != new_cat or old_cat == "All Beauty" or old_cat is None:
            updated_count += 1
    
    print(f"  Products sẽ được UPDATE: {updated_count:,}")
    print(f"  Products không thay đổi: {unchanged_count:,}")
    
    if updated_count > 0:
        print(f"\nSample products sẽ được update (10 đầu):")
        count = 0
        for parent_asin, data in list(product_categories.items())[:10]:
            if data["old_category"] != data["category"] or data["old_category"] == "All Beauty" or data["old_category"] is None:
                print(f"  - {parent_asin}: '{data['old_category']}' -> '{data['category']}'")
                count += 1
                if count >= 10:
                    break
    
    if dry_run:
        print("\n[DRY RUN] Không thực sự update products. Để update thật, chạy với --execute")
        return {
            "updated": 0,
            "unchanged": unchanged_count,
            "dry_run": True
        }
    
    # Update products
    print("\nĐang update products.main_category...")
    
    update_count = 0
    for parent_asin, data in product_categories.items():
        old_cat = data["old_category"]
        new_cat = data["category"]
        
        # Chỉ update nếu cần
        if old_cat != new_cat or old_cat == "All Beauty" or old_cat is None:
            await db.execute(
                text("""
                    UPDATE products
                    SET main_category = :category
                    WHERE parent_asin = :parent_asin
                """),
                {
                    "category": new_cat,
                    "parent_asin": parent_asin
                }
            )
            update_count += 1
    
    await db.commit()
    print(f"  [OK] Đã update {update_count:,} products")
    
    return {
        "updated": update_count,
        "unchanged": unchanged_count,
        "dry_run": False
    }


async def drop_products_without_category(
    db: AsyncSession,
    items_5core: set[str],
    dry_run: bool = True
) -> dict:
    """
    Drop products không có category, nhưng giữ lại products có trong interaction5core.
    
    Args:
        db: Database session
        items_5core: Set of parent_asins có trong interaction5core
        dry_run: Nếu True, chỉ log không thực sự drop
        
    Returns:
        Dict với thống kê
    """
    print("\n" + "=" * 80)
    print("DROP PRODUCTS WITHOUT CATEGORY")
    print("=" * 80)
    
    # Lấy products không có category
    result = await db.execute(
        text("""
            SELECT 
                p.parent_asin,
                p.main_category,
                COUNT(i.asin) as item_count
            FROM products p
            LEFT JOIN items i ON p.parent_asin = i.parent_asin
            WHERE (p.main_category IS NULL OR p.main_category = '' OR p.main_category = 'All Beauty')
            GROUP BY p.parent_asin, p.main_category
        """)
    )
    
    rows = result.fetchall()
    products_without_category = [
        {
            "parent_asin": row.parent_asin,
            "main_category": row.main_category,
            "item_count": row.item_count
        }
        for row in rows
    ]
    
    print(f"Tìm thấy {len(products_without_category):,} products không có category")
    
    # Filter: chỉ drop products KHÔNG có trong interaction5core
    products_to_drop = []
    products_to_keep = []
    
    for product in products_without_category:
        parent_asin = product["parent_asin"]
        if parent_asin not in items_5core:
            products_to_drop.append(product)
        else:
            products_to_keep.append(product)
    
    print(f"\nKết quả filter:")
    print(f"  Products sẽ được DROP: {len(products_to_drop):,}")
    print(f"  Products sẽ được GIỮ LẠI (có trong interaction5core): {len(products_to_keep):,}")
    
    if products_to_keep:
        print(f"\nProducts được giữ lại (sample 10 đầu):")
        for product in products_to_keep[:10]:
            print(f"  - {product['parent_asin']}: category='{product['main_category']}', items={product['item_count']}")
    
    if not products_to_drop:
        print("\nKhông có products nào cần drop.")
        return {"dropped": 0, "skipped": 0}
    
    # Kiểm tra xem có items nào đang được reference không
    parent_asins_to_drop = [p["parent_asin"] for p in products_to_drop]
    
    result = await db.execute(
        text("""
            SELECT COUNT(*) as count
            FROM items
            WHERE parent_asin = ANY(:parent_asins)
        """),
        {"parent_asins": parent_asins_to_drop}
    )
    item_count = result.fetchone().count
    
    result = await db.execute(
        text("""
            SELECT COUNT(*) as count
            FROM cart_items ci
            JOIN items i ON ci.asin = i.asin
            WHERE i.parent_asin = ANY(:parent_asins)
        """),
        {"parent_asins": parent_asins_to_drop}
    )
    cart_count = result.fetchone().count
    
    print(f"\nThống kê products cần drop:")
    print(f"  Tổng số products: {len(products_to_drop):,}")
    print(f"  Items liên quan: {item_count:,}")
    print(f"  Items trong cart: {cart_count:,}")
    
    if dry_run:
        print("\n[DRY RUN] Không thực sự drop products. Để drop thật, chạy với --execute")
        return {"dropped": 0, "skipped": len(products_to_drop), "dry_run": True}
    
    # Drop products (phải xóa theo thứ tự để tránh foreign key violation)
    print("\nĐang drop products...")
    
    # 1. Drop từ cart_items trước (nếu có)
    if cart_count > 0:
        await db.execute(
            text("""
                DELETE FROM cart_items
                WHERE asin IN (
                    SELECT i.asin
                    FROM items i
                    WHERE i.parent_asin = ANY(:parent_asins)
                )
            """),
            {"parent_asins": parent_asins_to_drop}
        )
        print(f"  Đã xóa {cart_count:,} items từ cart_items")
    
    # 2. Drop từ interaction_logs (nếu có)
    result = await db.execute(
        text("""
            SELECT COUNT(*) as count
            FROM interaction_logs
            WHERE asin IN (
                SELECT i.asin
                FROM items i
                WHERE i.parent_asin = ANY(:parent_asins)
            )
        """),
        {"parent_asins": parent_asins_to_drop}
    )
    interaction_count = result.fetchone().count
    
    if interaction_count > 0:
        await db.execute(
            text("""
                DELETE FROM interaction_logs
                WHERE asin IN (
                    SELECT i.asin
                    FROM items i
                    WHERE i.parent_asin = ANY(:parent_asins)
                )
            """),
            {"parent_asins": parent_asins_to_drop}
        )
        print(f"  Đã xóa {interaction_count:,} records từ interaction_logs")
    
    # 3. Drop từ reviews (phải xóa trước vì foreign key constraint)
    result = await db.execute(
        text("""
            SELECT COUNT(*) as count
            FROM reviews
            WHERE asin IN (
                SELECT i.asin
                FROM items i
                WHERE i.parent_asin = ANY(:parent_asins)
            )
        """),
        {"parent_asins": parent_asins_to_drop}
    )
    reviews_count = result.fetchone().count
    
    if reviews_count > 0:
        await db.execute(
            text("""
                DELETE FROM reviews
                WHERE asin IN (
                    SELECT i.asin
                    FROM items i
                    WHERE i.parent_asin = ANY(:parent_asins)
                )
            """),
            {"parent_asins": parent_asins_to_drop}
        )
        print(f"  Đã xóa {reviews_count:,} reviews")
    
    # 4. Drop items (sau khi đã xóa reviews)
    result = await db.execute(
        text("""
            DELETE FROM items
            WHERE parent_asin = ANY(:parent_asins)
        """),
        {"parent_asins": parent_asins_to_drop}
    )
    items_dropped = result.rowcount
    print(f"  Đã xóa {items_dropped:,} items")
    
    # 5. Drop products (cuối cùng)
    result = await db.execute(
        text("""
            DELETE FROM products
            WHERE parent_asin = ANY(:parent_asins)
        """),
        {"parent_asins": parent_asins_to_drop}
    )
    products_dropped = result.rowcount
    print(f"  Đã xóa {products_dropped:,} products")
    
    await db.commit()
    
    return {"dropped": products_dropped, "skipped": 0, "dry_run": False}


def load_5core_items(project_root: Path) -> set[str]:
    """
    Load danh sách parent_asin từ interactions_5core.parquet.
    
    Args:
        project_root: Project root path
        
    Returns:
        Set of parent_asin strings
    """
    interactions_path = project_root / "data" / "processed" / "interactions_5core.parquet"
    
    if not interactions_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {interactions_path}")
    
    print(f"Đang load items từ {interactions_path}...")
    import polars as pl
    df = pl.read_parquet(str(interactions_path))
    
    # Lấy unique item_id (thường là parent_asin)
    item_ids = df["item_id"].unique().to_list()
    item_set = set(str(item_id) for item_id in item_ids)
    
    print(f"  Tìm thấy {len(item_set):,} unique item_id (parent_asin) trong interaction5core")
    
    return item_set


async def main():
    """Hàm chính."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Update products.main_category từ items.category và drop products không có category")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Thực sự update và drop (mặc định là dry-run)"
    )
    args = parser.parse_args()
    
    print("=" * 80)
    print("UPDATE PRODUCTS CATEGORY AND DROP PRODUCTS WITHOUT CATEGORY")
    print("=" * 80)
    
    # Load 5core items
    try:
        items_5core = load_5core_items(project_root)
    except Exception as e:
        print(f"❌ Lỗi khi load interaction5core: {e}")
        sys.exit(1)
    
    # Kết nối database
    engine = create_async_engine(DATABASE_URL, echo=False)
    AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with AsyncSessionLocal() as db:
        try:
            # Step 1: Update products.main_category từ items.category
            update_stats = await update_products_category_from_items(
                db=db,
                dry_run=not args.execute
            )
            
            # Step 2: Drop products không có category
            drop_stats = await drop_products_without_category(
                db=db,
                items_5core=items_5core,
                dry_run=not args.execute
            )
            
            print("\n" + "=" * 80)
            print("KẾT QUẢ")
            print("=" * 80)
            print(f"\n[Update Products Category]")
            print(f"  Products đã update: {update_stats['updated']:,}")
            print(f"  Products không thay đổi: {update_stats['unchanged']:,}")
            
            print(f"\n[Drop Products Without Category]")
            print(f"  Products đã drop: {drop_stats['dropped']:,}")
            print(f"  Products đã skip: {drop_stats['skipped']:,}")
            
            if update_stats.get("dry_run") or drop_stats.get("dry_run"):
                print("\n⚠️  Đây là DRY RUN. Để thực sự update và drop, chạy với --execute")
            
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

