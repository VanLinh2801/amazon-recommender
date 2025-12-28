# Deployment Quick Start

Hướng dẫn nhanh để deploy hệ thống Recommender.

## 🚀 Deploy trong 5 bước

### Bước 1: Kiểm tra Artifacts
```bash
python backend/scripts/check_artifacts.py
```

### Bước 2: Setup Database
1. Tạo PostgreSQL trên Render hoặc external
2. Chạy schema:
   ```bash
   psql $DATABASE_URL -f backend/database.sql
   ```

### Bước 3: Deploy Backend (Render)
1. Render Dashboard → New + → Web Service
2. Connect GitHub repo
3. Cấu hình:
   - **Root Directory**: `backend`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
4. Environment Variables:
   ```env
   DATABASE_URL=postgresql+asyncpg://...
   JWT_SECRET_KEY=<generate với: openssl rand -hex 32>
   CORS_ORIGINS=https://your-app.vercel.app
   PORT=10000
   ```
5. Deploy và lưu URL backend

### Bước 4: Deploy Frontend (Vercel)
1. Vercel Dashboard → Add New → Project
2. Import GitHub repo
3. Cấu hình:
   - **Root Directory**: `frontend`
   - **Framework**: Next.js
4. Environment Variable:
   ```env
   NEXT_PUBLIC_API_URL=https://your-backend.onrender.com
   ```
5. Deploy

### Bước 5: Cập nhật CORS
Quay lại Render, cập nhật `CORS_ORIGINS` với URL Vercel và redeploy.

## ✅ Kiểm tra

```bash
# Health check
curl https://your-backend.onrender.com/health

# API docs
open https://your-backend.onrender.com/docs
```

## 📚 Chi tiết

Xem [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md) để biết thêm chi tiết và troubleshooting.

