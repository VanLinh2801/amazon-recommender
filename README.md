# E-commerce Recommender System

Hệ thống recommendation cho e-commerce sử dụng:
- **Collaborative Filtering** (Matrix Factorization)
- **Content-based Filtering** (Item Embeddings)
- **Ranking Model** (Learning-to-Rank)
- **Re-ranking** (Rule-based với Redis)

## Cấu trúc dự án

```
Recommender/
├── backend/              # FastAPI backend (deploy Render)
│   ├── app/              # Application code
│   ├── artifacts/       # Model artifacts
│   ├── database.sql     # Database schema
│   └── requirements.txt # Python dependencies
│
├── frontend/            # Next.js frontend (deploy Vercel)
│   ├── app/             # Next.js App Router
│   ├── components/      # React components
│   └── package.json      # Node dependencies
│
└── scripts/             # Training/preprocessing scripts (local)
    ├── data_preprocessing/
    ├── models/
    ├── embedding/
    └── database/
```

## Quick Start

### Backend (Local)

```bash
cd backend
pip install -r requirements.txt
python run.py
```

### Frontend (Local)

```bash
cd frontend
pnpm install
pnpm dev
```

## Deployment

Xem [DEPLOYMENT_STRUCTURE.md](./DEPLOYMENT_STRUCTURE.md) để biết chi tiết về deployment.

### Backend → Render
- Root directory: `backend`
- Build: `pip install -r requirements.txt`
- Start: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

### Frontend → Vercel
- Root directory: `frontend`
- Framework: Next.js
- Build: `pnpm build`

## Environment Variables

### Backend
- `DATABASE_URL`: PostgreSQL connection string
- `REDIS_URL`: Redis connection string (optional)
- `QDRANT_URL`: Qdrant connection string (optional)
- `JWT_SECRET_KEY`: Secret key cho JWT
- `CORS_ORIGINS`: Frontend URLs (comma-separated)

### Frontend
- `NEXT_PUBLIC_API_URL`: Backend API URL

## Documentation

- [Backend README](./backend/README.md)
- [Frontend README](./frontend/README.md)
- [Scripts README](./scripts/README.md)
- [Deployment Guide](./DEPLOYMENT_STRUCTURE.md)
