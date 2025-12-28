# Frontend Deployment - Quick Start

## Deploy Frontend lên Vercel trong 3 bước

### Bước 1: Push code lên GitHub
```bash
git add .
git commit -m "Prepare frontend for deployment"
git push origin main
```

### Bước 2: Deploy trên Vercel

1. Truy cập [vercel.com](https://vercel.com) và đăng nhập
2. Click "Add New..." → "Project"
3. Import GitHub repository
4. **QUAN TRỌNG**: Set **Root Directory** = `frontend`
5. Click "Deploy"

### Bước 3: Set Environment Variable

Sau khi deploy, vào Settings → Environment Variables:

```env
NEXT_PUBLIC_API_URL=http://localhost:8000
```

**Lưu ý:** Vì chưa có backend, có thể để `http://localhost:8000`. Khi có backend, cập nhật lại.

## ✅ Xong!

Frontend sẽ có URL: `https://your-project.vercel.app`

## 📚 Chi tiết

Xem [FRONTEND_DEPLOYMENT.md](./FRONTEND_DEPLOYMENT.md) để biết thêm chi tiết và troubleshooting.

