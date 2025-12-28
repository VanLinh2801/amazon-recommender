# Hướng dẫn Deployment

## Tổng quan

Hệ thống được deploy trên:
- **Frontend (Next.js)**: Vercel
- **Backend (FastAPI)**: Render
- **Database**: PostgreSQL (Render hoặc external)
- **Cache**: Redis (optional, Render hoặc external)
- **Vector DB**: Qdrant (optional, external)

## Checklist trước khi Deploy

Trước khi bắt đầu deploy, hãy đảm bảo:

- [ ] **Artifacts đã sẵn sàng**: Chạy `python backend/scripts/check_artifacts.py` để kiểm tra
- [ ] **Database schema đã được tạo**: File `backend/database.sql` đã sẵn sàng
- [ ] **Git repository đã được push**: Code đã được commit và push lên GitHub
- [ ] **Environment variables đã được chuẩn bị**: Có sẵn các credentials cần thiết
- [ ] **Artifacts đã được commit hoặc upload**: Các file model đã có trong repo hoặc storage

### Kiểm tra Artifacts

```bash
# Từ thư mục root của project
python backend/scripts/check_artifacts.py
```

Script này sẽ kiểm tra:
- ✅ Matrix Factorization artifacts (`mf/`)
- ✅ Popularity data (`popularity/`)
- ✅ Ranking model (`ranking/`)
- ✅ Embeddings (`embeddings/`)

**Lưu ý quan trọng về Artifacts:**
- Artifacts có thể rất lớn (hàng trăm MB), nên cân nhắc:
  - **Option 1**: Commit vào Git (nếu repo cho phép file lớn)
  - **Option 2**: Upload lên cloud storage (S3, Google Cloud Storage) và download khi deploy
  - **Option 3**: Sử dụng Git LFS (Large File Storage)

## Bước 0: Chuẩn bị Database

### 0.1. Tạo PostgreSQL Database

**Trên Render:**
1. Vào Render Dashboard → "New +" → "PostgreSQL"
2. Chọn plan (Free tier có giới hạn)
3. Lưu connection string (sẽ dùng cho `DATABASE_URL`)

**Hoặc dùng External Database:**
- AWS RDS, Google Cloud SQL, hoặc database service khác
- Đảm bảo cho phép connections từ Render IPs

### 0.2. Chạy Database Schema

Sau khi có database, chạy schema:

```bash
# Cách 1: Dùng psql
psql $DATABASE_URL -f backend/database.sql

# Cách 2: Dùng Python script (nếu có)
python backend/app/db/init_postgres.py
```

**Lưu ý:** Đảm bảo schema đã được chạy trước khi deploy backend.

## Bước 1: Chuẩn bị Backend trên Render

### 1.1. Tạo Web Service trên Render

1. Đăng nhập vào [Render Dashboard](https://dashboard.render.com)
2. Click "New +" → "Web Service"
3. Connect GitHub repository
4. Chọn repository và branch

### 1.2. Cấu hình Service

**Basic Settings:**
- **Name**: `recommender-api` (hoặc tên bạn muốn)
- **Environment**: `Python 3`
- **Region**: Chọn region gần bạn nhất
- **Branch**: `main` (hoặc branch bạn muốn deploy)
- **Root Directory**: `backend`

**Build & Deploy:**
- **Build Command**: `pip install -r requirements.txt`
- **Start Command**: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

### 1.3. Environment Variables

Thêm các biến môi trường sau trong Render Dashboard:

#### Bắt buộc:
```env
DATABASE_URL=postgresql+asyncpg://user:password@host:5432/dbname
JWT_SECRET_KEY=your-very-secure-secret-key-here
CORS_ORIGINS=https://your-app.vercel.app
PORT=10000
```

#### Tùy chọn (nếu sử dụng):
```env
REDIS_URL=redis://host:6379
QDRANT_URL=http://host:6333
ENVIRONMENT=production
```

**Chi tiết từng biến:**

1. **DATABASE_URL** (Bắt buộc)
   - Format: `postgresql+asyncpg://user:password@host:port/dbname`
   - Lấy từ PostgreSQL service trên Render hoặc external database
   - **Render Database:**
     - Internal (từ Render services): `postgresql+asyncpg://user:pass@host/dbname`
     - External (từ local/other services): `postgresql+asyncpg://user:pass@host.region-postgres.render.com:5432/dbname`
   - Ví dụ: `postgresql+asyncpg://user:pass@dpg-xxx.oregon-postgres.render.com:5432/dbname`
   - **Lưu ý:** Scripts tự động convert `postgresql://` → `postgresql+asyncpg://` nếu cần

2. **JWT_SECRET_KEY** (Bắt buộc)
   - Generate secret key mạnh:
     ```bash
     openssl rand -hex 32
     ```
   - Hoặc dùng Python:
     ```python
     import secrets
     print(secrets.token_urlsafe(32))
     ```

3. **CORS_ORIGINS** (Bắt buộc)
   - URL của frontend trên Vercel
   - Có thể có nhiều origins, phân cách bằng dấu phẩy
   - Ví dụ: `https://your-app.vercel.app,https://www.your-app.vercel.app`
   - **Lưu ý**: Cập nhật sau khi có URL frontend

4. **PORT** (Bắt buộc)
   - Render tự động set `$PORT`, nhưng có thể set cụ thể: `10000`

5. **REDIS_URL** (Tùy chọn)
   - Chỉ cần nếu sử dụng Redis cho caching
   - Format: `redis://host:port` hoặc `rediss://host:port` (SSL)
   - Tạo Redis instance trên Render hoặc dùng external

6. **QDRANT_URL** (Tùy chọn)
   - Chỉ cần nếu sử dụng Qdrant cho vector search
   - Format: `http://host:port` hoặc `https://host:port`
   - Deploy Qdrant riêng hoặc dùng cloud service

7. **ENVIRONMENT** (Tùy chọn)
   - Set `production` để tắt auto-reload
   - Mặc định: `production`

### 1.4. Deploy

Click "Create Web Service" và đợi build/deploy hoàn tất.

**Lưu URL backend**: Copy URL của service (ví dụ: `https://recommender-api.onrender.com`)

## Bước 2: Deploy Frontend trên Vercel

### 2.1. Import Project

1. Đăng nhập vào [Vercel Dashboard](https://vercel.com/dashboard)
2. Click "Add New..." → "Project"
3. Import GitHub repository

### 2.2. Cấu hình Project

**Project Settings:**
- **Framework Preset**: Next.js
- **Root Directory**: `frontend`
- **Build Command**: `pnpm build` (hoặc `npm run build`)
- **Output Directory**: `.next`
- **Install Command**: `pnpm install` (hoặc `npm install`)

### 2.3. Environment Variables

Thêm biến môi trường:

```env
NEXT_PUBLIC_API_URL=https://recommender-api.onrender.com
```

**Lưu ý:** Thay `https://recommender-api.onrender.com` bằng URL backend thực tế của bạn.

### 2.4. Deploy

Click "Deploy" và đợi build/deploy hoàn tất.

**Lưu URL frontend**: Copy URL của deployment (ví dụ: `https://your-app.vercel.app`)

## Bước 3: Cập nhật CORS

Sau khi có URL frontend, quay lại Render và cập nhật `CORS_ORIGINS`:

```env
CORS_ORIGINS=https://your-app.vercel.app
```

Sau đó redeploy backend.

## Bước 4: Kiểm tra

### 4.1. Kiểm tra Backend

```bash
curl https://your-backend.onrender.com/health
```

Kết quả mong đợi:
```json
{"status":"ok","service":"recommender-api"}
```

### 4.2. Kiểm tra Frontend

Truy cập URL Vercel và kiểm tra:
- Trang chủ load được
- API calls hoạt động
- Không có CORS errors trong console

## Troubleshooting

### Backend không start

1. Kiểm tra logs trên Render Dashboard
2. Kiểm tra environment variables
3. Kiểm tra `requirements.txt` có đầy đủ dependencies
4. Kiểm tra `Procfile` hoặc start command

### CORS Errors

1. Đảm bảo `CORS_ORIGINS` có URL frontend chính xác
2. URL phải match chính xác (bao gồm `https://`)
3. Redeploy backend sau khi thay đổi CORS

### Database Connection Errors

1. Kiểm tra `DATABASE_URL` format đúng
2. Đảm bảo database cho phép connections từ Render IPs
3. Kiểm tra database đã được tạo và schema đã được chạy

### Frontend không kết nối được Backend

1. Kiểm tra `NEXT_PUBLIC_API_URL` đúng
2. Kiểm tra backend đang chạy (health check)
3. Kiểm tra CORS settings
4. Kiểm tra browser console để xem lỗi cụ thể

## Lưu ý quan trọng

### Artifacts

**Vấn đề:** Artifacts (model files) có thể rất lớn (hàng trăm MB) và không nên commit vào Git thông thường.

**Giải pháp:**

1. **Option 1: Git LFS (Recommended)**
   ```bash
   # Cài đặt Git LFS
   git lfs install
   
   # Track artifacts
   git lfs track "backend/artifacts/**/*.npy"
   git lfs track "backend/artifacts/**/*.pkl"
   git lfs track "backend/artifacts/**/*.parquet"
   
   # Commit
   git add .gitattributes
   git add backend/artifacts/
   git commit -m "Add artifacts with LFS"
   ```

2. **Option 2: Cloud Storage (S3, GCS)**
   - Upload artifacts lên S3/GCS
   - Download trong build script:
     ```bash
     # Thêm vào build command trên Render
     pip install -r requirements.txt && \
     aws s3 sync s3://your-bucket/artifacts backend/artifacts/
     ```

3. **Option 3: Commit trực tiếp (chỉ nếu nhỏ)**
   - Chỉ nên dùng nếu artifacts < 100MB
   - Render có giới hạn repo size

### Database Schema

- **Bắt buộc**: Chạy `backend/database.sql` trên database trước khi deploy
- Kiểm tra schema đã được tạo bằng cách connect và list tables

### Secrets & Security

- ❌ **KHÔNG** commit secrets vào Git
- ✅ Chỉ dùng environment variables trên Render/Vercel
- ✅ Sử dụng strong JWT secret key
- ✅ Enable HTTPS (Render/Vercel tự động)

### Free Tier Limitations

**Render Free Tier:**
- ⚠️ Service có thể sleep sau 15 phút không có traffic
- ⚠️ Build time giới hạn
- ⚠️ Database có giới hạn connections
- 💡 **Giải pháp**: Upgrade lên paid plan hoặc dùng external services

**Vercel Free Tier:**
- ✅ Không có sleep time
- ✅ Hỗ trợ tốt cho Next.js
- ⚠️ Có giới hạn bandwidth

### Performance Tips

1. **Artifacts Loading**: Artifacts được load lazy, nhưng lần đầu có thể chậm
2. **Database Connections**: Sử dụng connection pooling
3. **Caching**: Sử dụng Redis để cache recommendations
4. **CDN**: Vercel tự động có CDN cho static assets

## Bước 5: Post-Deployment

### 5.1. Kiểm tra Health

```bash
# Backend health check
curl https://your-backend.onrender.com/health

# API docs
open https://your-backend.onrender.com/docs
```

### 5.2. Test Endpoints

```bash
# Test authentication
curl -X POST https://your-backend.onrender.com/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"test","password":"test123"}'

# Test recommendations (cần token)
curl https://your-backend.onrender.com/api/recommend \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### 5.3. Monitor Logs

- **Render**: Dashboard → Service → Logs
- **Vercel**: Dashboard → Project → Deployments → Logs

## Troubleshooting

### Backend không start

**Triệu chứng:** Service không khởi động được

**Giải pháp:**
1. Kiểm tra logs trên Render Dashboard
2. Kiểm tra environment variables đã set đúng chưa
3. Kiểm tra `requirements.txt` có đầy đủ dependencies
4. Kiểm tra `Procfile` hoặc start command
5. Kiểm tra artifacts có tồn tại không:
   ```bash
   # Trong build logs, kiểm tra
   ls -la backend/artifacts/
   ```

### Artifacts không tìm thấy

**Triệu chứng:** `FileNotFoundError: Không tìm thấy: artifacts/...`

**Giải pháp:**
1. Đảm bảo artifacts đã được commit hoặc download
2. Kiểm tra path: `backend/artifacts/` (relative từ root directory)
3. Nếu dùng Git LFS, đảm bảo LFS đã được cài trên Render
4. Nếu dùng cloud storage, kiểm tra download script trong build command

### CORS Errors

**Triệu chứng:** Browser console hiển thị CORS errors

**Giải pháp:**
1. Đảm bảo `CORS_ORIGINS` có URL frontend chính xác
2. URL phải match chính xác (bao gồm `https://`, không có trailing slash)
3. Redeploy backend sau khi thay đổi CORS
4. Kiểm tra trong browser DevTools → Network → Headers

### Database Connection Errors

**Triệu chứng:** `asyncpg.exceptions.InvalidPasswordError` hoặc connection timeout

**Giải pháp:**
1. Kiểm tra `DATABASE_URL` format đúng:
   - Phải có `+asyncpg` trong scheme: `postgresql+asyncpg://...`
   - Kiểm tra username, password, host, port, database name
2. Đảm bảo database cho phép connections từ Render IPs
3. Kiểm tra database đã được tạo và schema đã được chạy
4. Test connection:
   ```bash
   psql $DATABASE_URL -c "SELECT 1;"
   ```

### Frontend không kết nối được Backend

**Triệu chứng:** API calls fail, network errors

**Giải pháp:**
1. Kiểm tra `NEXT_PUBLIC_API_URL` đúng trong Vercel environment variables
2. Kiểm tra backend đang chạy (health check)
3. Kiểm tra CORS settings
4. Kiểm tra browser console để xem lỗi cụ thể
5. Kiểm tra Network tab trong DevTools

### Render Service Sleep

**Triệu chứng:** Service không response sau một thời gian không dùng

**Giải pháp:**
1. Free tier tự động sleep sau 15 phút không có traffic
2. Request đầu tiên sau khi sleep sẽ mất ~30 giây để wake up
3. **Giải pháp**: Upgrade lên paid plan hoặc setup health check cron job

## Next Steps

Sau khi deploy thành công:

1. **Setup Monitoring**
   - Render có built-in metrics
   - Có thể tích hợp với Datadog, New Relic, etc.

2. **Setup Error Tracking**
   - Sentry: https://sentry.io
   - LogRocket: https://logrocket.com
   - Hoặc dùng Render logs

3. **Setup CI/CD**
   - Auto-deploy khi push lên main branch
   - Render và Vercel đều hỗ trợ auto-deploy

4. **Optimize Performance**
   - Enable caching (Redis)
   - Optimize database queries
   - CDN cho static assets (Vercel tự động)

5. **Backup & Recovery**
   - Setup database backups
   - Backup artifacts
   - Document recovery procedures

6. **Security Hardening**
   - Enable rate limiting
   - Setup API keys nếu cần
   - Regular security updates

