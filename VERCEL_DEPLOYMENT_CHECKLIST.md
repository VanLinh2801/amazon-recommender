# Vercel Deployment Checklist - Fix Path Alias Issue

## Vấn đề: Module Not Found - Path Alias (@/)

Lỗi `Cannot find module '@/lib/utils'` khi deploy trên Vercel.

## Nguyên nhân có thể

1. **Root Directory chưa đúng** - Vercel không biết đâu là root của Next.js app
2. **jsconfig.json chưa được commit** - Next.js cần file này để resolve path aliases
3. **Build cache cũ** - Cache cũ có thể gây conflict

## Giải pháp từng bước

### ✅ Bước 1: Đảm bảo files đã được commit

```bash
# Kiểm tra
git status

# Commit các files cần thiết
git add frontend/jsconfig.json
git add frontend/tsconfig.json
git add frontend/next.config.mjs
git commit -m "Fix path alias configuration"
git push origin main
```

### ✅ Bước 2: Kiểm tra Root Directory trên Vercel

**QUAN TRỌNG NHẤT:**

1. Vào Vercel Dashboard → Project → **Settings** → **General**
2. Tìm section **"Root Directory"**
3. **PHẢI** set = `frontend` (không phải `.` hoặc để trống)
4. Click **"Save"**

**Lưu ý:** Nếu Root Directory không đúng, Vercel sẽ build từ root của repo thay vì từ `frontend/`, dẫn đến path aliases không hoạt động.

### ✅ Bước 3: Clear Build Cache

1. Vào Vercel Dashboard → Project → **Settings** → **General**
2. Scroll xuống section **"Build & Development Settings"**
3. Click **"Clear Build Cache"**
4. Confirm

### ✅ Bước 4: Kiểm tra Build Settings

Vào **Settings** → **General**, đảm bảo:

- **Framework Preset**: `Next.js` (hoặc `Other`)
- **Build Command**: `pnpm build` (hoặc `npm run build`)
- **Output Directory**: `.next`
- **Install Command**: `pnpm install` (hoặc `npm install`)
- **Node.js Version**: `18.x` hoặc `20.x` (recommended)

### ✅ Bước 5: Redeploy

**Option 1: Auto-deploy từ Git**
- Sau khi push code, Vercel sẽ tự động deploy
- Đợi và kiểm tra build logs

**Option 2: Manual Redeploy**
1. Vào **Deployments** tab
2. Click vào deployment mới nhất (hoặc failed deployment)
3. Click **"Redeploy"**
4. Chọn **"Use existing Build Cache"** = **OFF** (để clear cache)
5. Click **"Redeploy"**

## Kiểm tra Build Logs

Sau khi deploy, kiểm tra build logs:

1. Vào **Deployments** → Click vào deployment
2. Xem **"Build Logs"**
3. Tìm các dòng:
   - ✅ `Compiled successfully` = OK
   - ❌ `Cannot find module '@/lib/utils'` = Vẫn còn lỗi

## Nếu vẫn lỗi

### Debug Step 1: Kiểm tra file structure trên Vercel

Trong build logs, tìm dòng:
```
> Building...
```

Sau đó xem có thấy:
- `frontend/package.json` được tìm thấy?
- `frontend/jsconfig.json` được tìm thấy?

Nếu không thấy `frontend/` prefix → Root Directory chưa đúng.

### Debug Step 2: Test với relative imports (temporary)

Nếu vẫn không được, có thể tạm thời thay path aliases bằng relative imports để test:

```typescript
// Thay vì
import { cn } from '@/lib/utils'

// Dùng
import { cn } from '../../lib/utils'
```

**Lưu ý:** Chỉ dùng để test, sau đó fix lại path alias.

### Debug Step 3: Thử với npm thay vì pnpm

1. Vào **Settings** → **General**
2. Đổi **Install Command** = `npm install`
3. Đổi **Build Command** = `npm run build`
4. Commit `package-lock.json` (nếu có)
5. Redeploy

## Checklist cuối cùng

Trước khi deploy:

- [ ] `frontend/jsconfig.json` đã được commit
- [ ] `frontend/tsconfig.json` có `baseUrl: "."` và `paths`
- [ ] `frontend/next.config.mjs` đã được cập nhật
- [ ] Root Directory trên Vercel = `frontend` ⚠️ **QUAN TRỌNG**
- [ ] Build thành công local (`cd frontend && pnpm build`)
- [ ] Đã clear build cache trên Vercel
- [ ] Đã push code lên GitHub

## Expected Result

Sau khi fix, build logs sẽ hiển thị:

```
✓ Compiled successfully
✓ Collecting page data
✓ Generating static pages
✓ Finalizing page optimization
```

Không còn lỗi `Cannot find module '@/lib/utils'`.

