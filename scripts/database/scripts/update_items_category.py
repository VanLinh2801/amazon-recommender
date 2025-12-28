"""
Update Items Category from Semantic Attributes
===============================================
Script để cập nhật category của items từ file semantic_attributes.parquet.

Logic:
1. Load semantic_attributes.parquet
2. Map product_type -> category dựa trên logic đã có
3. Cập nhật category vào bảng items trong database

Chạy: python app/database/scripts/update_items_category.py
"""

import sys
import asyncio
from pathlib import Path
import os
import io
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm project root vào path
# Từ app/database/scripts/update_items_category.py -> D:\Recommender
project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Thay đổi working directory về project root
os.chdir(project_root)

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


# Mapping từ product_type sang category (dựa trên build_items_embedding_en.py)
PRODUCT_TYPE_TO_CATEGORY = {
    'eyeliner': 'Beauty and Eye Makeup',
    'mascara': 'Beauty and Eye Makeup',
    'eyeshadow': 'Beauty and Eye Makeup',
    'eye makeup': 'Beauty and Eye Makeup',
    'lipstick': 'Beauty and Lip Makeup',
    'lip makeup': 'Beauty and Lip Makeup',
    'foundation': 'Beauty and Face Makeup',
    'concealer': 'Beauty and Face Makeup',
    'blush': 'Beauty and Face Makeup',
    'face makeup': 'Beauty and Face Makeup',
    'wig': 'Beauty and Hair Extensions',
    'lace front wig': 'Beauty and Hair Extensions',
    'hair extension': 'Beauty and Hair Extensions',
    'hair clip': 'Beauty and Hair Accessories',
    'hair accessory': 'Beauty and Hair Accessories',
    'headband': 'Beauty and Hair Accessories',
    'scrunchie': 'Beauty and Hair Accessories',
    'hair spray': 'Beauty and Hair Styling',
    'hairspray': 'Beauty and Hair Styling',
    'hair gel': 'Beauty and Hair Styling',
    'gel': 'Beauty and Hair Styling',
    'hair styling': 'Beauty and Hair Styling',
    'makeup kit': 'Beauty and Makeup Kits',
    'beauty kit': 'Beauty and Makeup Kits',
    'cosmetic kit': 'Beauty and Makeup Kits',
    'makeup brush': 'Beauty and Makeup Tools',
    'makeup sponge': 'Beauty and Makeup Tools',
    'beauty accessories': 'Beauty and Makeup Tools',
    'shampoo': 'Beauty and Hair Care',
    'conditioner': 'Beauty and Hair Care',
    'hair care': 'Beauty and Hair Care',
    'skincare': 'Beauty and Skincare',
    'cream': 'Beauty and Skincare',
    'lotion': 'Beauty and Skincare',
    'serum': 'Beauty and Skincare',
    'fragrance': 'Beauty and Fragrance',
    'perfume': 'Beauty and Fragrance',
    'cologne': 'Beauty and Fragrance',
    'nail polish': 'Beauty and Nail Care',
    'gel nail': 'Beauty and Nail Care',
    'nail care': 'Beauty and Nail Care',
    'deodorant': 'Beauty and Personal Care',
    'antiperspirant': 'Beauty and Personal Care',
    'personal care': 'Beauty and Personal Care',
    'default': 'Beauty Products'  # Fallback
}


def map_product_type_to_category(product_type: str) -> str:
    """
    Map product_type sang category.
    
    Args:
        product_type: Product type từ semantic_attributes
        
    Returns:
        Category string
    """
    if not product_type:
        return 'Beauty Products'
    
    product_type_lower = product_type.lower().strip()
    
    # Tìm exact match trước
    if product_type_lower in PRODUCT_TYPE_TO_CATEGORY:
        return PRODUCT_TYPE_TO_CATEGORY[product_type_lower]
    
    # Tìm partial match
    for key, category in PRODUCT_TYPE_TO_CATEGORY.items():
        if key in product_type_lower or product_type_lower in key:
            return category
    
    # Fallback
    return 'Beauty Products'


async def update_items_category():
    """
    Cập nhật category cho items từ semantic_attributes.parquet.
    """
    print("=" * 80)
    print("UPDATE ITEMS CATEGORY FROM SEMANTIC ATTRIBUTES")
    print("=" * 80)
    
    # Đường dẫn file - project_root đã được tính đúng (D:\Recommender)
    semantic_file = project_root / 'data' / 'embedding' / 'semantic_attributes.parquet'
    
    # Debug: in ra đường dẫn để kiểm tra
    print(f"[DEBUG] project_root: {project_root}")
    print(f"[DEBUG] semantic_file path: {semantic_file}")
    print(f"[DEBUG] File exists: {semantic_file.exists()}")
    
    if not semantic_file.exists():
        print(f"[ERROR] Không tìm thấy file: {semantic_file}")
        return False
    
    print(f"\n[1] Đang load file: {semantic_file}")
    df = pd.read_parquet(semantic_file)
    print(f"[OK] Đã load {len(df):,} items từ semantic_attributes.parquet")
    
    # Map product_type -> category
    print("\n[2] Đang map product_type -> category...")
    df['category'] = df['product_type'].apply(map_product_type_to_category)
    
    # Thống kê categories
    print("\n[3] Thống kê categories:")
    category_counts = df['category'].value_counts()
    for category, count in category_counts.head(20).items():
        print(f"  {category}: {count:,} items")
    
    # Kết nối database
    print("\n[4] Đang kết nối database...")
    engine = create_async_engine(
        DATABASE_URL,
        echo=False
    )
    
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    
    # Cập nhật category
    print("\n[5] Đang cập nhật category vào database...")
    updated_count = 0
    skipped_count = 0
    
    async with async_session() as session:
        for idx, row in df.iterrows():
            parent_asin = row['parent_asin']
            category = row['category']
            
            if pd.isna(parent_asin) or not parent_asin:
                skipped_count += 1
                continue
            
            try:
                # Cập nhật tất cả items có parent_asin này
                result = await session.execute(
                    text("""
                        UPDATE items 
                        SET category = :category 
                        WHERE parent_asin = :parent_asin
                    """),
                    {"category": category, "parent_asin": str(parent_asin)}
                )
                
                if result.rowcount > 0:
                    updated_count += result.rowcount
                
                if (idx + 1) % 1000 == 0:
                    await session.commit()
                    print(f"  Đã xử lý: {idx + 1:,}/{len(df):,} items, updated: {updated_count:,}")
            
            except Exception as e:
                print(f"[ERROR] Lỗi khi cập nhật parent_asin {parent_asin}: {e}")
                await session.rollback()
                continue
        
        # Commit lần cuối
        await session.commit()
    
    print(f"\n[OK] Hoàn tất!")
    print(f"  - Tổng items trong file: {len(df):,}")
    print(f"  - Đã cập nhật: {updated_count:,} items")
    print(f"  - Bỏ qua: {skipped_count:,} items")
    
    await engine.dispose()
    return True


if __name__ == "__main__":
    try:
        success = asyncio.run(update_items_category())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n[INFO] Đã dừng bởi người dùng")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Lỗi không mong đợi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

