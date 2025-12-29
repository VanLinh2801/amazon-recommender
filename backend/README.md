# Backend - E-commerce Recommender API

FastAPI backend cho hệ thống recommendation.

## Cấu trúc

```
backend/
├── app/                    # Application code
│   ├── config.py          # Configuration
│   ├── main.py            # FastAPI app
│   ├── db/                # Database utilities
│   ├── recommender/       # Recommendation services
│   ├── web/               # API routes, services, schemas
│   └── ...
├── artifacts/             # Model artifacts
│   ├── embeddings/        # Item embeddings
│   ├── mf/               # Matrix factorization models
│   ├── popularity/       # Popularity data
│   └── ranking/         # Ranking models
├── database.sql          # Database schema
├── requirements.txt      # Python dependencies
├── run.py               # Entry point
├── Procfile             # Render process file
└── render.yaml          # Render configuration
```

## Local Development

### 1. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### 2. Cấu hình environment variables

Tạo file `.env` hoặc set environment variables:

```env
DATABASE_URL=postgresql+asyncpg://postgres:password@localhost:5432/recommender
REDIS_URL=redis://localhost:6379
QDRANT_URL=http://localhost:6333
JWT_SECRET_KEY=your-secret-key
CORS_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
```

### 3. Chạy server

```bash
python run.py
```

Hoặc:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Deployment trên Render

1. Connect GitHub repository
2. Chọn root directory: `backend`
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
5. Set environment variables:
   - `DATABASE_URL`: PostgreSQL connection string
   - `REDIS_URL`: Redis connection string (optional)
   - `QDRANT_URL`: Qdrant connection string (optional)
   - `JWT_SECRET_KEY`: Secret key cho JWT
   - `CORS_ORIGINS`: Vercel frontend URL (comma-separated)

## API Documentation

Sau khi server chạy, truy cập:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Health Check

```bash
curl http://localhost:8000/health
```




