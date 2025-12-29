"""Script để kiểm tra category distribution trong database"""
import asyncio
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(BASE_DIR / "backend"))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from app.config import settings
from app.web.utils.database import normalize_database_url


async def check_category_distribution():
    """Kiểm tra category distribution trong database."""
    engine = create_async_engine(normalize_database_url(settings.database_url), echo=False)
    async_session = async_sessionmaker(engine, class_=AsyncSession)
    
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT main_category, COUNT(*) as count 
            FROM products 
            GROUP BY main_category 
            ORDER BY count DESC
        """))
        rows = result.fetchall()
        
        print("Category distribution in database:")
        print("=" * 60)
        total = 0
        for row in rows:
            category = row.main_category or "NULL"
            count = row.count
            total += count
            print(f"{category}: {count:,} products")
        
        print("=" * 60)
        print(f"Total: {total:,} products")
    
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(check_category_distribution())

