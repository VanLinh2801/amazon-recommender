# Hướng dẫn Deploy Frontend lên Vercel

## Tổng quan

Frontend được deploy trên **Vercel** - platform tốt nhất cho Next.js.

## Bước 1: Chuẩn bị

### 1.1. Kiểm tra code

```bash
# Từ thư mục root
cd frontend

# Kiểm tra dependencies
pnpm install

# Test build local
pnpm build

# Test chạy local
pnpm dev
```

### 1.2. Đảm bảo code đã được push lên GitHub

```bash
git add .
git commit -m "Prepare for frontend deployment"
git push origin main
```

## Bước 2: Deploy trên Vercel

### 2.1. Đăng nhập Vercel

1. Truy cập [Vercel Dashboard](https://vercel.com/dashboard)
2. Đăng nhập bằng GitHub account
3. Click "Add New..." → "Project"

### 2.2. Import Repository

1. Chọn GitHub repository của bạn
2. Click "Import"

### 2.3. Cấu hình Project

**Project Settings:**

- **Project Name**: `recommender-frontend` (hoặc tên bạn muốn)
- **Framework Preset**: `Next.js` (tự động detect)
- **Root Directory**: `frontend` ⚠️ **QUAN TRỌNG**
- **Build Command**: `pnpm build` (hoặc `npm run build`)
- **Output Directory**: `.next` (mặc định)
- **Install Command**: `pnpm install` (hoặc `npm install`)

**Lưu ý:** 
- Phải set **Root Directory** = `frontend` vì project có cấu trúc monorepo
- Vercel sẽ tự động detect Next.js nếu có `next.config.mjs`

### 2.4. Environment Variables

Thêm biến môi trường:

```env
NEXT_PUBLIC_API_URL=http://localhost:8000
```

**Giải thích:**
- `NEXT_PUBLIC_API_URL`: URL của backend API
- Vì chưa có backend, có thể để `http://localhost:8000` hoặc URL placeholder
- Khi có backend, sẽ cập nhật sau

**Cách thêm:**
1. Trong Vercel project settings
2. Vào tab "Environment Variables"
3. Add variable:
   - **Key**: `NEXT_PUBLIC_API_URL`
   - **Value**: `http://localhost:8000` (hoặc backend URL nếu có)
   - **Environment**: Production, Preview, Development (chọn tất cả)

### 2.5. Deploy

1. Click "Deploy"
2. Đợi build hoàn tất (thường 2-5 phút)
3. Vercel sẽ tự động tạo URL: `https://your-project.vercel.app`

## Bước 3: Kiểm tra Deployment

### 3.1. Truy cập URL

Mở URL Vercel đã cung cấp, kiểm tra:
- ✅ Trang chủ load được
- ✅ Không có lỗi trong console (F12)
- ✅ UI hiển thị đúng

### 3.2. Kiểm tra Console

Mở Browser DevTools (F12) → Console:
- ⚠️ Có thể có lỗi API calls (vì chưa có backend) - **ĐÂY LÀ BÌNH THƯỜNG**
- ✅ Không có lỗi build/compile
- ✅ Không có lỗi CORS (vì chưa có backend)

### 3.3. Test các trang

- `/` - Trang chủ
- `/login` - Đăng nhập
- `/register` - Đăng ký
- `/dashboard` - Dashboard (cần login)

## Bước 4: Cập nhật khi có Backend

Khi đã deploy backend:

1. Vào Vercel Dashboard → Project → Settings → Environment Variables
2. Cập nhật `NEXT_PUBLIC_API_URL`:
   ```env
   NEXT_PUBLIC_API_URL=https://your-backend.onrender.com
   ```
3. Redeploy (hoặc đợi auto-deploy nếu có push code mới)

## Troubleshooting

### Build Failed

**Lỗi:** Build fails với lỗi TypeScript hoặc dependencies

**Giải pháp:**
1. Kiểm tra `next.config.mjs` có `ignoreBuildErrors: true` không
2. Kiểm tra `package.json` có đầy đủ dependencies
3. Xem build logs trên Vercel để biết lỗi cụ thể

### Root Directory không đúng

**Lỗi:** Vercel không tìm thấy Next.js app

**Giải pháp:**
1. Vào Project Settings → General
2. Set **Root Directory** = `frontend`
3. Redeploy

### Environment Variables không hoạt động

**Lỗi:** `NEXT_PUBLIC_API_URL` không được nhận

**Giải pháp:**
1. Đảm bảo variable name bắt đầu với `NEXT_PUBLIC_`
2. Redeploy sau khi thêm/sửa environment variables
3. Variables chỉ có hiệu lực sau khi rebuild

### API Calls Fail

**Lỗi:** Network errors khi gọi API

**Giải pháp:**
- Nếu chưa có backend: **ĐÂY LÀ BÌNH THƯỜNG**
- Frontend vẫn hiển thị được, chỉ API calls sẽ fail
- Khi có backend, cập nhật `NEXT_PUBLIC_API_URL` và redeploy

### CORS Errors

**Lỗi:** CORS errors trong console

**Giải pháp:**
- Chỉ xảy ra khi đã có backend
- Cần cấu hình CORS trên backend
- Xem [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md) để setup CORS

## Best Practices

1. **Custom Domain** (Optional):
   - Vercel cho phép add custom domain
   - Vào Settings → Domains → Add domain

2. **Preview Deployments**:
   - Mỗi PR tự động tạo preview deployment
   - Test trước khi merge vào main

3. **Analytics** (Optional):
   - Vercel có built-in analytics
   - Enable trong Settings → Analytics

4. **Environment Variables per Environment**:
   - Có thể set khác nhau cho Production/Preview/Development
   - Ví dụ: Production dùng production API, Preview dùng staging API

## Checklist

Trước khi nộp:

- [ ] Frontend đã deploy thành công trên Vercel
- [ ] URL Vercel hoạt động và load được
- [ ] Không có lỗi build/compile
- [ ] UI hiển thị đúng
- [ ] Các trang chính hoạt động (Home, Login, Register)
- [ ] Environment variables đã được set
- [ ] Code đã được push lên GitHub

## Next Steps

Sau khi deploy frontend:

1. ✅ Frontend đã sẵn sàng để demo/nộp
2. 🔄 Khi có backend, cập nhật `NEXT_PUBLIC_API_URL`
3. 🔄 Deploy backend và test integration

