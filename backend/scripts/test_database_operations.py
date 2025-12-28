"""
Script test các operations trên database để đảm bảo hoạt động đúng.

Test:
- Connection
- CRUD operations
- Foreign keys
- Indexes
- Transactions

Usage:
    python backend/scripts/test_database_operations.py "postgresql://..."
"""
import sys
import asyncio
import os
import io
from pathlib import Path
from datetime import datetime

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm backend vào path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text, select
import bcrypt


def convert_to_asyncpg_url(url: str) -> str:
    """Convert PostgreSQL URL sang asyncpg format nếu cần."""
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql+asyncpg://"):
        return url
    else:
        raise ValueError(f"Invalid database URL format: {url}")


async def test_connection(engine):
    """Test 1: Connection."""
    print("\n" + "=" * 60)
    print("TEST 1: CONNECTION")
    print("=" * 60)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT version(), current_database(), current_user"))
            row = result.fetchone()
            print(f"✅ PostgreSQL Version: {row[0]}")
            print(f"✅ Database: {row[1]}")
            print(f"✅ User: {row[2]}")
        return True
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False


async def test_tables_exist(engine):
    """Test 2: Kiểm tra tables tồn tại."""
    print("\n" + "=" * 60)
    print("TEST 2: TABLES EXISTENCE")
    print("=" * 60)
    
    required_tables = [
        'users', 'products', 'items', 'reviews', 
        'interaction_logs', 'shopping_carts', 'cart_items',
        'invoices', 'invoice_items', 'item_images', 'model_registry'
    ]
    
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))
            existing_tables = {row[0] for row in result.fetchall()}
        
        print(f"📋 Tables trong database: {len(existing_tables)}")
        all_ok = True
        
        for table in required_tables:
            if table in existing_tables:
                print(f"   ✅ {table}")
            else:
                print(f"   ❌ {table} - KHÔNG TỒN TẠI")
                all_ok = False
        
        return all_ok
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False


async def test_user_crud(engine):
    """Test 3: User CRUD operations."""
    print("\n" + "=" * 60)
    print("TEST 3: USER CRUD OPERATIONS")
    print("=" * 60)
    
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        # Test INSERT
        print("\n[3.1] Test INSERT user...")
        test_username = f"test_user_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        password_hash = bcrypt.hashpw("test123".encode(), bcrypt.gensalt()).decode()
        
        async with async_session() as session:
            result = await session.execute(
                text("""
                    INSERT INTO users (username, password_hash, phone_number)
                    VALUES (:username, :password_hash, :phone_number)
                    RETURNING id, username, created_at
                """),
                {
                    "username": test_username,
                    "password_hash": password_hash,
                    "phone_number": "0123456789"
                }
            )
            user = result.fetchone()
            user_id = user[0]
            await session.commit()
        
        print(f"   ✅ Created user: id={user_id}, username={test_username}")
        
        # Test SELECT
        print("\n[3.2] Test SELECT user...")
        async with async_session() as session:
            result = await session.execute(
                text("SELECT id, username, phone_number, created_at FROM users WHERE id = :id"),
                {"id": user_id}
            )
            user = result.fetchone()
            if user:
                print(f"   ✅ Found user: {user[1]}")
            else:
                print(f"   ❌ User not found")
                return False
        
        # Test UPDATE
        print("\n[3.3] Test UPDATE user...")
        async with async_session() as session:
            await session.execute(
                text("UPDATE users SET phone_number = :phone WHERE id = :id"),
                {"phone": "0987654321", "id": user_id}
            )
            await session.commit()
        
        async with async_session() as session:
            result = await session.execute(
                text("SELECT phone_number FROM users WHERE id = :id"),
                {"id": user_id}
            )
            phone = result.scalar()
            if phone == "0987654321":
                print(f"   ✅ Updated phone: {phone}")
            else:
                print(f"   ❌ Update failed")
                return False
        
        # Test DELETE
        print("\n[3.4] Test DELETE user...")
        async with async_session() as session:
            await session.execute(
                text("DELETE FROM users WHERE id = :id"),
                {"id": user_id}
            )
            await session.commit()
        
        async with async_session() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM users WHERE id = :id"),
                {"id": user_id}
            )
            count = result.scalar()
            if count == 0:
                print(f"   ✅ Deleted user")
            else:
                print(f"   ❌ Delete failed")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_product_crud(engine):
    """Test 4: Product CRUD operations."""
    print("\n" + "=" * 60)
    print("TEST 4: PRODUCT CRUD OPERATIONS")
    print("=" * 60)
    
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        # Test INSERT
        print("\n[4.1] Test INSERT product...")
        test_asin = f"TEST{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        async with async_session() as session:
            result = await session.execute(
                text("""
                    INSERT INTO products (parent_asin, title, store, main_category, avg_rating, rating_number)
                    VALUES (:parent_asin, :title, :store, :category, :rating, :rating_num)
                    RETURNING parent_asin, title
                """),
                {
                    "parent_asin": test_asin,
                    "title": "Test Product",
                    "store": "Test Store",
                    "category": "Electronics",
                    "rating": 4.5,
                    "rating_num": 100
                }
            )
            product = result.fetchone()
            await session.commit()
        
        print(f"   ✅ Created product: {product[0]} - {product[1]}")
        
        # Test SELECT
        print("\n[4.2] Test SELECT product...")
        async with async_session() as session:
            result = await session.execute(
                text("SELECT parent_asin, title, avg_rating FROM products WHERE parent_asin = :asin"),
                {"asin": test_asin}
            )
            product = result.fetchone()
            if product:
                print(f"   ✅ Found product: {product[1]} (rating: {product[2]})")
            else:
                print(f"   ❌ Product not found")
                return False
        
        # Test INSERT item (foreign key)
        print("\n[4.3] Test INSERT item (foreign key)...")
        item_asin = f"{test_asin}-ITEM1"
        
        async with async_session() as session:
            await session.execute(
                text("""
                    INSERT INTO items (asin, parent_asin, variant)
                    VALUES (:asin, :parent_asin, :variant)
                """),
                {
                    "asin": item_asin,
                    "parent_asin": test_asin,
                    "variant": "Color: Red"
                }
            )
            await session.commit()
        
        print(f"   ✅ Created item: {item_asin}")
        
        # Test SELECT with JOIN
        print("\n[4.4] Test SELECT with JOIN...")
        async with async_session() as session:
            result = await session.execute(
                text("""
                    SELECT p.parent_asin, p.title, i.asin, i.variant
                    FROM products p
                    JOIN items i ON p.parent_asin = i.parent_asin
                    WHERE p.parent_asin = :asin
                """),
                {"asin": test_asin}
            )
            rows = result.fetchall()
            if rows:
                print(f"   ✅ Found {len(rows)} items for product")
                for row in rows:
                    print(f"      - {row[2]}: {row[3]}")
            else:
                print(f"   ❌ No items found")
                return False
        
        # Cleanup
        print("\n[4.5] Cleanup test data...")
        async with async_session() as session:
            await session.execute(text("DELETE FROM items WHERE asin = :asin"), {"asin": item_asin})
            await session.execute(text("DELETE FROM products WHERE parent_asin = :asin"), {"asin": test_asin})
            await session.commit()
        print(f"   ✅ Cleaned up")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_foreign_keys(engine):
    """Test 5: Foreign key constraints."""
    print("\n" + "=" * 60)
    print("TEST 5: FOREIGN KEY CONSTRAINTS")
    print("=" * 60)
    
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        # Test foreign key violation
        print("\n[5.1] Test foreign key constraint (should fail)...")
        try:
            async with async_session() as session:
                await session.execute(
                    text("INSERT INTO items (asin, parent_asin) VALUES (:asin, :parent_asin)"),
                    {"asin": "INVALID-ITEM", "parent_asin": "NONEXISTENT-PRODUCT"}
                )
                await session.commit()
            print(f"   ❌ Should have failed (foreign key violation)")
            return False
        except Exception as e:
            if "foreign key" in str(e).lower() or "violates foreign key" in str(e).lower():
                print(f"   ✅ Foreign key constraint works: {str(e)[:100]}")
            else:
                print(f"   ⚠️  Unexpected error: {e}")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False


async def test_indexes(engine):
    """Test 6: Indexes."""
    print("\n" + "=" * 60)
    print("TEST 6: INDEXES")
    print("=" * 60)
    
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text("""
                SELECT 
                    indexname, 
                    tablename,
                    indexdef
                FROM pg_indexes
                WHERE schemaname = 'public'
                AND indexname LIKE 'idx_%'
                ORDER BY tablename, indexname
            """))
            indexes = result.fetchall()
        
        if indexes:
            print(f"📋 Found {len(indexes)} indexes:")
            for idx in indexes:
                print(f"   ✅ {idx[0]} on {idx[1]}")
        else:
            print(f"⚠️  No indexes found")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False


async def test_transactions(engine):
    """Test 7: Transactions."""
    print("\n" + "=" * 60)
    print("TEST 7: TRANSACTIONS")
    print("=" * 60)
    
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        test_username = f"tx_test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        password_hash = bcrypt.hashpw("test123".encode(), bcrypt.gensalt()).decode()
        
        # Test commit
        print("\n[7.1] Test transaction COMMIT...")
        async with async_session() as session:
            await session.execute(
                text("INSERT INTO users (username, password_hash) VALUES (:username, :password_hash)"),
                {"username": test_username, "password_hash": password_hash}
            )
            await session.commit()
        
        async with async_session() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM users WHERE username = :username"),
                {"username": test_username}
            )
            count = result.scalar()
            if count == 1:
                print(f"   ✅ Transaction committed successfully")
            else:
                print(f"   ❌ Transaction commit failed")
                return False
        
        # Test rollback
        print("\n[7.2] Test transaction ROLLBACK...")
        test_username2 = f"tx_test2_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        async with async_session() as session:
            await session.execute(
                text("INSERT INTO users (username, password_hash) VALUES (:username, :password_hash)"),
                {"username": test_username2, "password_hash": password_hash}
            )
            await session.rollback()
        
        async with async_session() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM users WHERE username = :username"),
                {"username": test_username2}
            )
            count = result.scalar()
            if count == 0:
                print(f"   ✅ Transaction rolled back successfully")
            else:
                print(f"   ❌ Transaction rollback failed")
                return False
        
        # Cleanup
        async with async_session() as session:
            await session.execute(text("DELETE FROM users WHERE username = :username"), {"username": test_username})
            await session.commit()
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_statistics(engine):
    """Test 8: Database statistics."""
    print("\n" + "=" * 60)
    print("TEST 8: DATABASE STATISTICS")
    print("=" * 60)
    
    try:
        async with engine.begin() as conn:
            # Table counts
            tables = ['users', 'products', 'items', 'reviews', 'interaction_logs', 
                     'shopping_carts', 'cart_items', 'invoices', 'invoice_items']
            
            print("\n📊 Table row counts:")
            for table in tables:
                try:
                    result = await conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    print(f"   {table:20s}: {count:>10,} rows")
                except Exception as e:
                    print(f"   {table:20s}: ERROR - {str(e)[:50]}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False


async def main():
    """Hàm chính."""
    # Lấy database URL
    if len(sys.argv) > 1:
        database_url = sys.argv[1]
    else:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            print("❌ ERROR: Chưa có DATABASE_URL")
            print("\nUsage:")
            print("  python backend/scripts/test_database_operations.py <DATABASE_URL>")
            sys.exit(1)
    
    # Convert URL
    try:
        async_url = convert_to_asyncpg_url(database_url)
    except ValueError as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
    
    print("=" * 60)
    print("DATABASE OPERATIONS TEST SUITE")
    print("=" * 60)
    safe_url = database_url.split("@")[1] if "@" in database_url else database_url
    print(f"📍 Database: {safe_url}")
    
    # Tạo engine
    engine = create_async_engine(async_url, echo=False, pool_pre_ping=True)
    
    try:
        results = []
        
        # Chạy các tests
        results.append(("Connection", await test_connection(engine)))
        results.append(("Tables Existence", await test_tables_exist(engine)))
        results.append(("User CRUD", await test_user_crud(engine)))
        results.append(("Product CRUD", await test_product_crud(engine)))
        results.append(("Foreign Keys", await test_foreign_keys(engine)))
        results.append(("Indexes", await test_indexes(engine)))
        results.append(("Transactions", await test_transactions(engine)))
        results.append(("Statistics", await test_statistics(engine)))
        
        # Tổng kết
        print("\n" + "=" * 60)
        print("TEST RESULTS SUMMARY")
        print("=" * 60)
        
        passed = sum(1 for _, result in results if result)
        total = len(results)
        
        for test_name, result in results:
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{status} - {test_name}")
        
        print("\n" + "=" * 60)
        print(f"TOTAL: {passed}/{total} tests passed")
        print("=" * 60)
        
        if passed == total:
            print("\n🎉 TẤT CẢ TESTS ĐỀU PASS!")
            return True
        else:
            print(f"\n⚠️  {total - passed} test(s) failed")
            return False
        
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        await engine.dispose()


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)

