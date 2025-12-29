"""
Script để chạy FastAPI application với hiển thị URL rõ ràng.
"""
import uvicorn
import os

if __name__ == "__main__":
    # Lấy host và port từ environment hoặc dùng default
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    
    # Chỉ dùng reload trong development
    # Mặc định là development để có auto-reload
    is_development = os.getenv("ENVIRONMENT", "development").lower() == "development"
    reload = is_development
    
    print("\n" + "=" * 60)
    if is_development:
        print("🚀 Đang khởi động E-commerce Recommender Demo (Development)...")
    else:
        print("🚀 Đang khởi động E-commerce Recommender API (Production)...")
    print("=" * 60 + "\n")
    
    # Chạy uvicorn
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=reload,  # Chỉ reload trong development
        log_level="info"
    )

