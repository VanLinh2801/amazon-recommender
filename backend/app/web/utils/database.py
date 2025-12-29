"""
Database connection utilities for async SQLAlchemy.
"""
import logging
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from app.config import settings

logger = logging.getLogger(__name__)

def normalize_database_url(url: str) -> str:
    """
    Chuẩn hóa database URL:
    - Convert postgresql:// -> postgresql+asyncpg://
    - Thêm port 5432 nếu thiếu cho Render database
    """
    # Convert postgresql:// -> postgresql+asyncpg://
    if url.startswith("postgresql://") and "+asyncpg" not in url:
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    
    # Nếu là Render database và thiếu port, thêm port 5432
    if "render.com" in url.lower() and ":5432" not in url:
        # Tìm vị trí sau @ và trước /
        if "@" in url and "/" in url:
            at_pos = url.rfind("@")
            slash_pos = url.find("/", at_pos)
            if slash_pos > at_pos:
                # Chèn :5432 trước dấu /
                url = url[:slash_pos] + ":5432" + url[slash_pos:]
    
    return url

# Normalize database URL
normalized_db_url = normalize_database_url(settings.database_url)

# Kiểm tra xem có phải Render database không (cần SSL)
is_render_db = 'render.com' in normalized_db_url.lower()

# Log database URL (mask password)
def mask_url(url: str) -> str:
    """Mask password trong database URL để log."""
    if '@' in url:
        parts = url.split('@')
        user_pass = parts[0].split('//')[1] if '//' in parts[0] else parts[0]
        if ':' in user_pass:
            user = user_pass.split(':')[0]
            return url.replace(user_pass, f"{user}:***")
    return url

# Create async engine
try:
    connect_args = {}
    if is_render_db:
        # Render database yêu cầu SSL
        connect_args = {"ssl": "require"}
        logger.info("📍 Detected Render database - using SSL connection")
        if normalized_db_url != settings.database_url:
            logger.info("   Normalized database URL (added port 5432 and asyncpg driver)")
    else:
        logger.info("📍 Using local database (localhost)")
        logger.info("   Database: local PostgreSQL on localhost:5432")
    
    logger.info(f"🔗 Database URL: {mask_url(normalized_db_url)}")
    
    engine = create_async_engine(
        normalized_db_url,
        echo=False,
        pool_pre_ping=True,  # Kiểm tra connection trước khi dùng
        pool_size=20,  # Tăng từ 10 lên 20
        max_overflow=30,  # Tăng từ 20 lên 30
        pool_recycle=3600,  # Recycle connections sau 1 giờ để tránh stale connections
        pool_timeout=30,  # Timeout khi lấy connection từ pool (30 giây)
        connect_args={
            **connect_args,
            "command_timeout": 60,  # Timeout cho mỗi query (60 giây)
            "server_settings": {
                "application_name": "recommender_api"
            }
        }
    )
    logger.info("✅ Database engine created successfully")
except Exception as e:
    logger.error(f"Failed to create database engine: {e}")
    logger.error("Please check:")
    if is_render_db:
        logger.error("  1. DATABASE_URL is correct (Render database)")
        logger.error("  2. Network Access is enabled on Render (Settings > Network Access)")
        logger.error("  3. Database is Active (not Paused)")
    else:
        logger.error("  1. PostgreSQL server is running")
        logger.error("  2. DATABASE_URL is correct")
        logger.error("  3. Database exists")
    logger.error("Run: python backend/scripts/check_db_connection.py to diagnose")
    raise

# Create session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)


async def get_db() -> AsyncSession:
    """
    Dependency để lấy database session.
    Sử dụng trong FastAPI routes.
    
    Đảm bảo session được đóng đúng cách ngay cả khi có exception.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            # Commit nếu không có exception
            await session.commit()
        except Exception:
            # Rollback nếu có exception
            await session.rollback()
            raise
        finally:
            # Đảm bảo session được đóng
            await session.close()

