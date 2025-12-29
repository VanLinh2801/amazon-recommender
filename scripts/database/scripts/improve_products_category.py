"""
Script để cải thiện category cho products trong database.
Extract và normalize category từ raw_metadata cho các products có category là "All Beauty".
"""
import asyncio
import json
import sys
import io
from pathlib import Path
from typing import Optional, Dict, Any

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm backend vào path
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(BASE_DIR / "backend"))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text, select
from app.config import settings
from app.web.utils.database import normalize_database_url

DATABASE_URL = settings.database_url


def extract_and_normalize_category(raw_metadata: Optional[Dict[str, Any]], current_category: str) -> str:
    """
    Extract và normalize category từ raw_metadata.
    
    Logic:
    1. Nếu main_category không phải "All Beauty", giữ nguyên
    2. Extract từ categories list nếu có
    3. Extract từ details nếu có
    4. Extract từ title (keyword-based)
    5. Default: "Beauty"
    """
    if current_category and current_category != "All Beauty":
        return current_category
    
    if not raw_metadata:
        return "Beauty"
    
    try:
        # Thử extract từ categories list
        categories = raw_metadata.get("categories", [])
        if categories and len(categories) > 0:
            # categories thường là nested list: [["Beauty", "Personal Care", "Hair Care"]]
            if isinstance(categories[0], list) and len(categories[0]) > 0:
                cat = categories[0][0]
                if cat and cat != "All Beauty":
                    return cat
            elif isinstance(categories[0], str):
                cat = categories[0]
                if cat and cat != "All Beauty":
                    return cat
        
        # Thử extract từ details
        details = raw_metadata.get("details", {})
        if isinstance(details, dict):
            # Tìm các keys liên quan đến category
            for key in ["category", "main_category", "product_category", "category_name"]:
                if key in details:
                    cat = details[key]
                    if cat and cat != "All Beauty":
                        return str(cat)
        
        # Extract từ title (keyword-based)
        title = raw_metadata.get("title", "").lower()
        category_keywords = {
            "hair": "Hair Care",
            "skin": "Skin Care",
            "makeup": "Makeup",
            "fragrance": "Fragrance",
            "nail": "Nail Care",
            "bath": "Bath & Body",
            "oral": "Oral Care",
            "shave": "Shaving",
            "wig": "Hair Care",
            "cleansing": "Skin Care",
            "moisturizer": "Skin Care",
            "serum": "Skin Care",
            "lip": "Makeup",
            "eye": "Makeup",
            "foundation": "Makeup",
            "perfume": "Fragrance",
            "cologne": "Fragrance",
            "shampoo": "Hair Care",
            "conditioner": "Hair Care",
            "lotion": "Skin Care",
            "cream": "Skin Care",
            "toner": "Skin Care",
            "mask": "Skin Care",
            "brush": "Makeup",
            "mascara": "Makeup",
            "eyeliner": "Makeup",
            "blush": "Makeup",
            "concealer": "Makeup",
        }
        
        for keyword, category in category_keywords.items():
            if keyword in title:
                return category
        
        # Default
        return "Beauty"
        
    except (KeyError, IndexError, TypeError, AttributeError) as e:
        return "Beauty"


async def update_products_category(dry_run: bool = True):
    """
    Cập nhật category cho products có category là "All Beauty".
    """
    print("=" * 80)
    print("IMPROVE PRODUCTS CATEGORY")
    print("=" * 80)
    
    if dry_run:
        print("\n[DRY RUN MODE] - Khong cap nhat database")
    else:
        print("\n[EXECUTE MODE] - Se cap nhat database")
    
    # Kết nối database
    async_url = normalize_database_url(DATABASE_URL)
    engine = create_async_engine(async_url, echo=False)
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        async with async_session() as session:
            # Lấy tất cả products có category là "All Beauty"
            query = text("""
                SELECT parent_asin, main_category, raw_metadata
                FROM products
                WHERE main_category = 'All Beauty' OR main_category IS NULL
            """)
            
            result = await session.execute(query)
            products = result.fetchall()
            
            print(f"\n[1] Tìm thấy {len(products):,} products cần cập nhật category")
            
            if len(products) == 0:
                print("\n✅ Không có products nào cần cập nhật!")
                return
            
            # Thống kê category mới
            category_counts = {}
            updates = []
            
            print("\n[2] Đang extract và normalize category...")
            for i, (parent_asin, main_category, raw_metadata_json) in enumerate(products, 1):
                if i % 1000 == 0:
                    print(f"  Đã xử lý {i:,}/{len(products):,} products...")
                
                # Parse raw_metadata
                raw_metadata = None
                if raw_metadata_json:
                    try:
                        if isinstance(raw_metadata_json, str):
                            raw_metadata = json.loads(raw_metadata_json)
                        else:
                            raw_metadata = raw_metadata_json
                    except (json.JSONDecodeError, TypeError):
                        raw_metadata = None
                
                # Extract category mới
                new_category = extract_and_normalize_category(raw_metadata, main_category or "All Beauty")
                
                # Đếm
                category_counts[new_category] = category_counts.get(new_category, 0) + 1
                
                # Lưu update
                if new_category != (main_category or "All Beauty"):
                    updates.append({
                        "parent_asin": parent_asin,
                        "new_category": new_category
                    })
            
            # In thống kê
            print("\n[3] Thống kê category mới:")
            sorted_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
            for category, count in sorted_categories[:20]:
                print(f"  {category}: {count:,} products")
            
            print(f"\n[4] Sẽ cập nhật {len(updates):,} products")
            
            # Cập nhật database
            if not dry_run and len(updates) > 0:
                print("\n[5] Đang cập nhật database...")
                
                # Batch update
                batch_size = 1000
                for i in range(0, len(updates), batch_size):
                    batch = updates[i:i + batch_size]
                    
                    # Tạo VALUES clause
                    values_clauses = []
                    params = {}
                    for j, update in enumerate(batch):
                        asin_param = f"asin_{j}"
                        cat_param = f"cat_{j}"
                        values_clauses.append(f"(:{asin_param}, :{cat_param})")
                        params[asin_param] = update["parent_asin"]
                        params[cat_param] = update["new_category"]
                    
                    # Update query
                    update_query = text(f"""
                        UPDATE products
                        SET main_category = tmp.new_category
                        FROM (VALUES {', '.join(values_clauses)}) AS tmp(parent_asin, new_category)
                        WHERE products.parent_asin = tmp.parent_asin
                    """)
                    
                    await session.execute(update_query, params)
                    
                    if (i + batch_size) % 5000 == 0:
                        print(f"  Đã cập nhật {min(i + batch_size, len(updates)):,}/{len(updates):,} products...")
                
                await session.commit()
                print(f"\n[OK] Da cap nhat {len(updates):,} products thanh cong!")
            else:
                print("\n[DRY RUN] - Khong cap nhat database")
                print(f"   Se cap nhat {len(updates):,} products neu chay voi --execute")
            
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await engine.dispose()


async def main():
    """Hàm chính."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Cải thiện category cho products trong database")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Thực sự cập nhật database (mặc định là dry-run)"
    )
    
    args = parser.parse_args()
    
    await update_products_category(dry_run=not args.execute)


if __name__ == "__main__":
    asyncio.run(main())

