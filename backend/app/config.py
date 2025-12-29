import os
from pathlib import Path
from typing import Optional, List

BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env file if exists
try:
    from dotenv import load_dotenv
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        print(f"[OK] Loaded .env file from {env_path}")
except ImportError:
    # python-dotenv not installed, skip
    pass


class Settings:
    # Data directory
    data_dir: Path = BASE_DIR
    
    # Database - Force sử dụng local database
    # Bỏ qua DATABASE_URL environment variable để luôn dùng local database
    # Local database: postgresql+asyncpg://postgres:VanLinh04@localhost:5432/recommender
    database_url: str = "postgresql+asyncpg://postgres:VanLinh04@localhost:5432/recommender"
    
    # JWT Settings
    jwt_secret_key: str = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 30 * 24 * 60  # 30 days
    
    # CORS Settings
    cors_origins: List[str] = os.getenv(
        "CORS_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000,http://localhost:3001"
    ).split(",")
    
    # Redis (optional)
    redis_url: Optional[str] = os.getenv("REDIS_URL", None)
    
    # Qdrant (optional)
    qdrant_url: Optional[str] = os.getenv("QDRANT_URL", "http://localhost:6333")


settings = Settings()



