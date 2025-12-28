"""
Module khởi tạo và load dữ liệu vào PostgreSQL cho web demo.

Chức năng:
- Kết nối PostgreSQL bằng SQLAlchemy async
- Thực thi schema từ database.sql để tạo tables
- Load dữ liệu từ parquet files vào database
- Insert dữ liệu theo batch với ON CONFLICT DO NOTHING

Chạy script:
    python app/db/init_postgres.py
"""

import asyncio
import json
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd
from sqlalchemy import text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

# Connection string
DATABASE_URL = "postgresql+asyncpg://postgres:VanLinh04@localhost:5432/recommender"

# Batch size cho insert
BATCH_SIZE = 1000


def get_project_root() -> Path:
    """Tìm project root directory."""
    script_path = Path(__file__).resolve()
    current = script_path.parent
    while current != current.parent:
        if (current / "database.sql").exists():
            return current
        current = current.parent
    return script_path.parent.parent


def strip_sql_comments(sql: str) -> str:
    """
    Loại bỏ comment -- và /* */ khỏi SQL string.
    
    Args:
        sql: SQL string có thể chứa comments
        
    Returns:
        SQL string đã được strip comments
    """
    lines = []
    in_block_comment = False
    
    for line in sql.split('\n'):
        # Xử lý block comment /* */
        if '/*' in line:
            if '*/' in line:
                # Block comment trên cùng 1 dòng
                start = line.find('/*')
                end = line.find('*/') + 2
                line = line[:start] + line[end:]
            else:
                # Bắt đầu block comment
                start = line.find('/*')
                line = line[:start]
                in_block_comment = True
        
        if in_block_comment:
            if '*/' in line:
                # Kết thúc block comment
                end = line.find('*/') + 2
                line = line[end:]
                in_block_comment = False
            else:
                # Vẫn trong block comment, bỏ qua dòng này
                continue
        
        # Xử lý line comment --
        if '--' in line:
            comment_pos = line.find('--')
            # Kiểm tra xem -- có trong string literal không
            # Đơn giản: tìm -- đầu tiên và cắt từ đó
            line = line[:comment_pos]
        
        lines.append(line)
    
    return '\n'.join(lines)


def wrap_create_type_if_not_exists(statement: str) -> str:
    """
    Wrap CREATE TYPE statement với DO $$ BEGIN IF NOT EXISTS ... END $$.
    
    Args:
        statement: CREATE TYPE statement
        
    Returns:
        Statement đã được wrap với IF NOT EXISTS logic
    """
    statement = statement.strip()
    if not statement.upper().startswith('CREATE TYPE'):
        return statement
    
    # Parse type name từ CREATE TYPE type_name AS ENUM (...)
    # Pattern: CREATE TYPE type_name AS ENUM (...)
    import re
    match = re.match(r'CREATE\s+TYPE\s+(\w+)\s+AS\s+ENUM\s*\((.*?)\);', statement, re.IGNORECASE | re.DOTALL)
    if not match:
        return statement
    
    type_name = match.group(1)
    enum_values = match.group(2).strip()
    
    # Wrap với DO $$ BEGIN IF NOT EXISTS ... END $$
    wrapped = f"""
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{type_name}') THEN
        CREATE TYPE {type_name} AS ENUM ({enum_values});
    END IF;
END $$;
    """.strip()
    
    return wrapped


def parse_sql_statements(sql: str) -> list[str]:
    """
    Parse SQL string thành danh sách các statements.
    Tách theo dấu ; và loại bỏ comments.
    Xử lý đúng string literals để không tách nhầm ; trong string.
    
    Args:
        sql: SQL string
        
    Returns:
        List các SQL statements đã được clean
    """
    # Strip comments trước
    sql = strip_sql_comments(sql)
    
    statements = []
    current_statement = []
    in_string = False
    string_char = None
    
    i = 0
    while i < len(sql):
        char = sql[i]
        
        # Xử lý string literals (chỉ xử lý single quote cho PostgreSQL)
        if char == "'":
            # Kiểm tra escape ('' trong PostgreSQL là escaped single quote)
            if i + 1 < len(sql) and sql[i+1] == "'":
                # Escaped single quote '', bỏ qua cả 2 ký tự
                current_statement.append(char)
                i += 1
                current_statement.append(sql[i])
            elif not in_string:
                # Bắt đầu string literal
                in_string = True
                string_char = "'"
                current_statement.append(char)
            elif char == string_char:
                # Kết thúc string literal
                in_string = False
                string_char = None
                current_statement.append(char)
            else:
                current_statement.append(char)
        elif char == ';' and not in_string:
            # Tìm thấy dấu ; ngoài string literal - kết thúc statement
            current_statement.append(char)
            statement = ''.join(current_statement).strip()
            if statement:
                statements.append(statement)
            current_statement = []
        else:
            current_statement.append(char)
        
        i += 1
    
    # Thêm statement cuối nếu có
    if current_statement:
        statement = ''.join(current_statement).strip()
        if statement:
            statements.append(statement)
    
    return statements


async def execute_schema(engine, schema_file: Path):
    """
    Đọc và thực thi toàn bộ schema từ database.sql.
    
    Mỗi câu SQL được thực thi trong một transaction riêng biệt.
    Script idempotent - có thể chạy nhiều lần an toàn.
    
    Args:
        engine: SQLAlchemy async engine
        schema_file: Đường dẫn đến file database.sql
    """
    print(f"\n[1] Đang đọc schema từ {schema_file}...")
    
    if not schema_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file schema: {schema_file}")
    
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    
    print(f"[OK] Đã đọc schema ({len(schema_sql)} ký tự)")
    
    # Parse SQL statements
    statements = parse_sql_statements(schema_sql)
    
    # Wrap CREATE TYPE statements với IF NOT EXISTS
    wrapped_statements = []
    for stmt in statements:
        if stmt.upper().startswith('CREATE TYPE'):
            wrapped_stmt = wrap_create_type_if_not_exists(stmt)
            wrapped_statements.append(wrapped_stmt)
        else:
            wrapped_statements.append(stmt)
    
    statements = wrapped_statements
    print(f"[OK] Đã parse thành {len(statements)} câu lệnh SQL")
    
    # Thực thi từng statement trong transaction riêng biệt
    total = len(statements)
    success_count = 0
    skip_count = 0
    error_count = 0
    
    for i, statement in enumerate(statements, 1):
        # Mỗi statement trong transaction riêng
        try:
            async with engine.begin() as conn:
                await conn.execute(text(statement))
            # Nếu đến đây thì thành công
            success_count += 1
            preview = statement.replace('\n', ' ').strip()[:50]
            print(f"  [{i}/{total}] OK: {preview}...")
        except Exception as e:
            error_msg = str(e).lower()
            
            # Kiểm tra các lỗi có thể bỏ qua
            is_skip_error = (
                "already exists" in error_msg or
                "duplicate" in error_msg or
                ("relation" in error_msg and "already exists" in error_msg) or
                ("type" in error_msg and "already exists" in error_msg)
            )
            
            if is_skip_error:
                skip_count += 1
                preview = statement.replace('\n', ' ').strip()[:50]
                print(f"  [{i}/{total}] Bỏ qua (đã tồn tại): {preview}...")
            else:
                error_count += 1
                preview = statement.replace('\n', ' ').strip()[:50]
                print(f"  [{i}/{total}] ERROR: {preview}...")
                print(f"         Chi tiết: {str(e)[:200]}")
                # Tiếp tục với statement tiếp theo (không abort)
    
    # Tổng kết
    print(f"\n[OK] Đã thực thi schema:")
    print(f"  - Thành công: {success_count}/{total}")
    print(f"  - Bỏ qua (đã tồn tại): {skip_count}/{total}")
    print(f"  - Lỗi: {error_count}/{total}")


async def load_products(engine, parquet_file: Path):
    """
    Load dữ liệu từ items_for_rs.parquet vào bảng products.
    
    Args:
        engine: SQLAlchemy async engine
        parquet_file: Đường dẫn đến file parquet
    """
    print(f"\n[2] Đang load products từ {parquet_file}...")
    
    if not parquet_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {parquet_file}")
    
    # Đọc parquet file
    df = pd.read_parquet(parquet_file)
    print(f"[OK] Đã đọc {len(df):,} dòng từ parquet file")
    print(f"  Columns: {list(df.columns)}")
    
    # Map columns: item_id → parent_asin
    if "item_id" in df.columns:
        df = df.rename(columns={"item_id": "parent_asin"})
    
    # Chọn các cột cần thiết cho bảng products
    required_columns = [
        "parent_asin", "title", "store", "main_category",
        "avg_rating", "rating_number", "primary_image", "raw_metadata"
    ]
    
    # Kiểm tra columns có tồn tại không
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"  [WARNING] Thiếu columns: {missing_columns}")
        # Chỉ chọn columns có sẵn
        available_columns = [col for col in required_columns if col in df.columns]
        df = df[available_columns]
    else:
        df = df[required_columns]
    
    # Xử lý raw_metadata: convert dict/list sang JSON string nếu cần
    if "raw_metadata" in df.columns:
        def convert_to_json(val):
            if pd.isna(val):
                return None
            if isinstance(val, (dict, list)):
                return json.dumps(val)
            if isinstance(val, str):
                # Nếu đã là JSON string thì giữ nguyên
                try:
                    json.loads(val)
                    return val
                except:
                    return json.dumps(val)
            return val
        
        df["raw_metadata"] = df["raw_metadata"].apply(convert_to_json)
    
    # Xử lý NULL values - convert NaN thành None
    df = df.replace({pd.NA: None, pd.NaT: None})
    df = df.where(pd.notnull(df), None)
    
    # Convert DataFrame thành list of dicts
    records = df.to_dict('records')
    
    # Insert theo batch
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    total_rows = len(records)
    inserted = 0
    
    # Load table metadata từ database
    metadata = MetaData()
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync_conn: metadata.reflect(bind=sync_conn, only=['products']))
    products_table = metadata.tables['products']
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        
        # Xử lý raw_metadata: convert sang JSON string nếu cần
        for record in batch:
            if 'raw_metadata' in record and record['raw_metadata'] is not None:
                val = record['raw_metadata']
                if isinstance(val, (dict, list)):
                    record['raw_metadata'] = json.dumps(val)
                elif isinstance(val, str):
                    # Kiểm tra xem đã là JSON string chưa
                    try:
                        json.loads(val)
                        # Đã là JSON string hợp lệ, giữ nguyên
                    except:
                        # Không phải JSON string, convert
                        record['raw_metadata'] = json.dumps(val)
        
        async with async_session() as session:
            try:
                # Dùng PostgreSQL-specific insert với ON CONFLICT DO NOTHING
                stmt = pg_insert(products_table).values(batch)
                stmt = stmt.on_conflict_do_nothing(index_elements=['parent_asin'])
                
                result = await session.execute(stmt)
                await session.commit()
                
                # Đếm số rows thực sự được insert (không có trong asyncpg, dùng len batch)
                inserted += len(batch)
                print(f"  Đã insert batch {i//BATCH_SIZE + 1}: {len(batch)} rows (tổng: {inserted:,}/{total_rows:,})")
            except Exception as e:
                print(f"  [ERROR] Lỗi khi insert batch {i//BATCH_SIZE + 1}: {e}")
                print(f"  [DEBUG] Batch sample: {batch[0] if batch else 'empty'}")
                await session.rollback()
                raise
    
    print(f"[OK] Đã load {inserted:,} products vào database!")


async def load_items_from_reviews(engine, parquet_file: Path):
    """
    Tạo items entries từ reviews data (vì reviews.asin references items.asin).
    Chỉ tạo các items chưa tồn tại.
    
    Args:
        engine: SQLAlchemy async engine
        parquet_file: Đường dẫn đến file reviews parquet
    """
    print(f"\n[3a] Đang tạo items từ reviews data...")
    
    if not parquet_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {parquet_file}")
    
    # Đọc parquet file để lấy unique asins
    df = pd.read_parquet(parquet_file)
    
    # Lấy unique (asin, parent_asin) pairs
    if "asin" not in df.columns or "parent_asin" not in df.columns:
        print("  [SKIP] Không có asin hoặc parent_asin trong reviews data")
        return
    
    items_df = df[["asin", "parent_asin"]].drop_duplicates()
    items_df = items_df.dropna(subset=["asin", "parent_asin"])
    
    print(f"[OK] Tìm thấy {len(items_df):,} unique items cần tạo")
    
    if len(items_df) == 0:
        print("  [SKIP] Không có items nào để tạo")
        return
    
    # Convert DataFrame thành list of dicts
    items_df = items_df.replace({pd.NA: None, pd.NaT: None})
    items_df = items_df.where(pd.notnull(items_df), None)
    records = items_df.to_dict('records')
    
    # Lọc bỏ records có None values
    records = [r for r in records if r.get('asin') and r.get('parent_asin')]
    
    if not records:
        print("  [SKIP] Không có items nào để insert")
        return
    
    # Insert theo batch
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    total_rows = len(records)
    inserted = 0
    
    # Load table metadata từ database
    metadata = MetaData()
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync_conn: metadata.reflect(bind=sync_conn, only=['items']))
    items_table = metadata.tables['items']
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        
        async with async_session() as session:
            try:
                stmt = pg_insert(items_table).values(batch)
                stmt = stmt.on_conflict_do_nothing(index_elements=['asin'])
                
                await session.execute(stmt)
                await session.commit()
                inserted += len(batch)
                print(f"  Đã insert batch {i//BATCH_SIZE + 1}: {len(batch)} items (tổng: {inserted:,}/{total_rows:,})")
            except Exception as e:
                print(f"  [WARNING] Lỗi khi insert batch {i//BATCH_SIZE + 1}: {e}")
                await session.rollback()
                # Tiếp tục với batch tiếp theo
    
    print(f"[OK] Đã tạo {inserted:,} items từ reviews data!")


async def load_reviews(engine, parquet_file: Path):
    """
    Load dữ liệu từ reviews_clean.parquet vào bảng reviews.
    
    Args:
        engine: SQLAlchemy async engine
        parquet_file: Đường dẫn đến file parquet
    """
    print(f"\n[3] Đang load reviews từ {parquet_file}...")
    
    if not parquet_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {parquet_file}")
    
    # Đọc parquet file
    df = pd.read_parquet(parquet_file)
    print(f"[OK] Đã đọc {len(df):,} dòng từ parquet file")
    print(f"  Columns: {list(df.columns)}")
    
    # Map columns cho bảng reviews
    # reviews table cần: user_id, asin, rating, review_title, review_text, helpful_vote, verified, review_ts
    # Parquet có: amazon_user_id, asin, rating, review_title, review_text, helpful_vote, verified, ts
    
    # Chọn các cột cần thiết
    column_mapping = {
        "asin": "asin",
        "rating": "rating",
        "review_title": "review_title",
        "review_text": "review_text",
        "helpful_vote": "helpful_vote",
        "verified": "verified",
        "ts": "review_ts"
    }
    
    # Kiểm tra columns có tồn tại không
    available_mapping = {}
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            available_mapping[old_col] = new_col
    
    df_selected = df[list(available_mapping.keys())].copy()
    df_selected = df_selected.rename(columns=available_mapping)
    
    # user_id sẽ set NULL vì chưa có users table được populate
    df_selected["user_id"] = None
    
    # Xử lý NULL values
    df_selected = df_selected.where(pd.notnull(df_selected), None)
    
    # Đảm bảo review_ts là datetime
    if "review_ts" in df_selected.columns:
        df_selected["review_ts"] = pd.to_datetime(df_selected["review_ts"], errors="coerce")
    
    # Xử lý NULL values
    df_selected = df_selected.replace({pd.NA: None, pd.NaT: None})
    df_selected = df_selected.where(pd.notnull(df_selected), None)
    
    # Convert DataFrame thành list of dicts
    records = df_selected.to_dict('records')
    
    # Insert theo batch
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    total_rows = len(records)
    inserted = 0
    
    # Load table metadata từ database
    metadata = MetaData()
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync_conn: metadata.reflect(bind=sync_conn, only=['reviews']))
    reviews_table = metadata.tables['reviews']
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        
        async with async_session() as session:
            try:
                # Reviews không cần ON CONFLICT vì không có unique constraint
                stmt = pg_insert(reviews_table).values(batch)
                await session.execute(stmt)
                await session.commit()
                inserted += len(batch)
                print(f"  Đã insert batch {i//BATCH_SIZE + 1}: {len(batch)} rows (tổng: {inserted:,}/{total_rows:,})")
            except Exception as e:
                print(f"  [ERROR] Lỗi khi insert batch {i//BATCH_SIZE + 1}: {e}")
                print(f"  [DEBUG] Batch sample: {batch[0] if batch else 'empty'}")
                await session.rollback()
                raise
    
    print(f"[OK] Đã load {inserted:,} reviews vào database!")


async def main():
    """Hàm chính để khởi tạo database."""
    print("=" * 80)
    print("KHỞI TẠO POSTGRESQL DATABASE CHO WEB DEMO")
    print("=" * 80)
    
    # Tìm project root
    project_root = get_project_root()
    print(f"\nProject root: {project_root}")
    
    # Đường dẫn các file
    schema_file = project_root / "database.sql"
    items_file = project_root / "data" / "processed" / "items_for_rs.parquet"
    reviews_file = project_root / "data" / "processed" / "reviews_clean.parquet"
    
    # Tạo async engine
    print(f"\n[0] Đang kết nối đến database: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else DATABASE_URL}")
    engine = create_async_engine(DATABASE_URL, echo=False)
    
    try:
        # Test connection
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        print("[OK] Đã kết nối database thành công!")
        
        # 1. Thực thi schema
        await execute_schema(engine, schema_file)
        
        # 2. Load products
        await load_products(engine, items_file)
        
        # 3a. Tạo items từ reviews data (vì reviews.asin references items.asin)
        await load_items_from_reviews(engine, reviews_file)
        
        # 3. Load reviews
        await load_reviews(engine, reviews_file)
        
        print("\n" + "=" * 80)
        print("[OK] HOÀN TẤT: Database đã được khởi tạo và load dữ liệu thành công!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Lỗi: {e}")
        raise
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

