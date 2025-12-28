from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path

from app.web.routes import auth, cart, event, item, recommend, analytics

BASE_DIR = Path(__file__).resolve().parent

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
    
    # Lấy host và port từ environment hoặc dùng default
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    
    print("\n" + "=" * 60)
    print("🚀 E-commerce Recommender API đã khởi động!")
    print("=" * 60)
    print(f"\n📍 API URL:      http://{host}:{port}")
    print(f"📍 Network URL:  http://0.0.0.0:{port}")
    print(f"\n📚 API Docs:     http://{host}:{port}/docs")
    print(f"📖 ReDoc:        http://{host}:{port}/redoc")
    print(f"\n🔗 Frontend:     http://localhost:3000")
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



