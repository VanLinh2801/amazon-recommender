# Cấu trúc Deployment

## Tổng quan

Dự án được tổ chức lại để deploy:
- **Frontend (Next.js)** → Vercel
- **Backend (FastAPI)** → Render

## Cấu trúc thư mục mới

```
Recommender/
├── backend/                    # FastAPI backend (deploy Render)
│   ├── app/                    # Application code
│   ├── artifacts/             # Model artifacts (MF, embeddings, ranking)
│   ├── database.sql           # Database schema
│   ├── requirements.txt       # Python dependencies
│   ├── run.py                 # Entry point
│   ├── Procfile               # Render process file
│   └── render.yaml            # Render configuration
│
├── frontend/                  # Next.js frontend (deploy Vercel)
│   ├── app/                   # Next.js App Router
│   ├── components/            # React components
│   ├── lib/                   # Utilities
│   ├── package.json           # Node dependencies
│   └── vercel.json            # Vercel configuration
│
├── scripts/                   # Training/preprocessing scripts (local)
│   ├── data_preprocessing/    # Data preprocessing scripts
│   ├── models/                # Model training scripts
│   ├── embedding/             # Embedding training scripts
│   └── database/              # Database migration scripts
│
└── README.md                  # Main documentation
```

## Deployment Steps

### 1. Backend trên Render

1. Tạo Web Service trên Render
2. Connect GitHub repository
3. Cấu hình:
   - **Root Directory**: `backend`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python run.py` hoặc `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
   - **Environment Variables**:
     - `DATABASE_URL`: PostgreSQL connection string
     - `REDIS_URL`: Redis connection string (nếu dùng)
     - `QDRANT_URL`: Qdrant connection string (nếu dùng)
     - `JWT_SECRET_KEY`: Secret key cho JWT
     - `CORS_ORIGINS`: Vercel frontend URL

### 2. Frontend trên Vercel

1. Import project vào Vercel
2. Cấu hình:
   - **Root Directory**: `frontend`
   - **Framework Preset**: Next.js
   - **Build Command**: `pnpm build` hoặc `npm run build`
   - **Output Directory**: `.next`
   - **Environment Variables**:
     - `NEXT_PUBLIC_API_URL`: Render backend URL

## Environment Variables

### Backend (Render)
```env
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/dbname
REDIS_URL=redis://host:6379
QDRANT_URL=http://host:6333
JWT_SECRET_KEY=your-secret-key
CORS_ORIGINS=https://your-app.vercel.app
PORT=10000
```

### Frontend (Vercel)
```env
NEXT_PUBLIC_API_URL=https://your-backend.onrender.com
```

