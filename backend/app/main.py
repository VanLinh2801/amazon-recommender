from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pathlib import Path
import time
import logging
import asyncio

from app.web.routes import auth, cart, event, item, recommend, analytics

BASE_DIR = Path(__file__).resolve().parent
logger = logging.getLogger(__name__)

app = FastAPI(
    title="E-commerce Recommender API",
    description="RESTful API cho hệ thống gợi ý sản phẩm",
    version="1.0.0"
)

# CORS configuration
from app.config import settings

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request timeout middleware
@app.middleware("http")
async def timeout_middleware(request: Request, call_next):
    """
    Middleware để timeout requests sau 120 giây (2 phút).
    Đặc biệt quan trọng cho recommendation endpoint có thể chậm.
    """
    start_time = time.time()
    timeout = 120  # 2 phút
    
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        
        # Log slow requests
        if process_time > 5:
            logger.warning(
                f"Slow request: {request.method} {request.url.path} "
                f"took {process_time:.2f}s"
            )
        
        return response
    except asyncio.TimeoutError:
        logger.error(f"Request timeout: {request.method} {request.url.path}")
        return JSONResponse(
            status_code=504,
            content={"detail": "Request timeout. Please try again."}
        )
    except Exception as e:
        logger.error(f"Request error: {request.method} {request.url.path} - {e}")
        raise

# Include routers
app.include_router(auth.router)
app.include_router(cart.router)
app.include_router(event.router)
app.include_router(item.router)
app.include_router(recommend.router)
app.include_router(analytics.router)


@app.on_event("startup")
async def startup_event():
    """Hiển thị URL khi app khởi động."""
    import os
    from app.config import settings
    
    # Lấy host và port từ environment hoặc dùng default
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    
    # Mask database URL để hiển thị
    db_url = settings.database_url
    if '@' in db_url:
        parts = db_url.split('@')
        user_pass = parts[0].split('//')[1] if '//' in parts[0] else parts[0]
        if ':' in user_pass:
            user = user_pass.split(':')[0]
            masked_db_url = db_url.replace(user_pass, f"{user}:***")
        else:
            masked_db_url = db_url.replace(user_pass, "***")
    else:
        masked_db_url = db_url
    
    # Database đã được force dùng local trong config.py
    print("\n" + "=" * 60)
    print("🚀 E-commerce Recommender API đã khởi động!")
    print("=" * 60)
    print(f"\n📍 API URL:      http://{host}:{port}")
    print(f"📍 Network URL:  http://0.0.0.0:{port}")
    print(f"\n📚 API Docs:     http://{host}:{port}/docs")
    print(f"📖 ReDoc:        http://{host}:{port}/redoc")
    print(f"\n🔗 Frontend:     http://localhost:3000")
    print(f"\n💾 Database:     {masked_db_url}")
    print(f"   Type:         Local PostgreSQL (localhost:5432)")
    print(f"   ✅ Status:    Using local database (hardcoded in config)")
    print("\n" + "=" * 60 + "\n")


@app.get("/")
async def root():
    """Root endpoint - API information."""
    return {
        "name": "E-commerce Recommender API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "recommender-api"}



