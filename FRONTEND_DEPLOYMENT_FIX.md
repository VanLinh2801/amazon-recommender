# Fix Lỗi Build Frontend trên Vercel

## Lỗi: Module Not Found - Path Alias (@/)

Nếu gặp lỗi `Cannot find module '@/lib/utils'` hoặc path alias không được resolve, làm theo các bước sau:

## Giải pháp

### Bước 0: Đảm bảo có jsconfig.json

**QUAN TRỌNG:** Next.js cần cả `tsconfig.json` và `jsconfig.json` để resolve path aliases.

File `jsconfig.json` đã được tạo với nội dung:
```json
{
  "compilerOptions": {
    "baseUrl": ".",
    "paths": {
      "@/*": ["./*"]
    }
  }
}
```

Đảm bảo file này đã được commit:
```bash
git add frontend/jsconfig.json
git commit -m "Add jsconfig.json for path alias resolution"
git push origin main
```

### Bước 1: Kiểm tra Root Directory

**QUAN TRỌNG:** Trên Vercel, phải set **Root Directory** = `frontend`

1. Vào Vercel Dashboard → Project → Settings → General
2. Tìm "Root Directory"
3. Set = `frontend`
4. Click "Save"

### Bước 2: Kiểm tra Build Settings

1. Vào Settings → General
2. Kiểm tra:
   - **Framework Preset**: Next.js
   - **Build Command**: `pnpm build` (hoặc `npm run build`)
   - **Output Directory**: `.next`
   - **Install Command**: `pnpm install` (hoặc `npm install`)

### Bước 3: Đảm bảo Lockfile được commit

```bash
# Kiểm tra lockfile có trong repo không
git status

# Nếu thiếu, commit lại
git add frontend/pnpm-lock.yaml  # hoặc package-lock.json
git commit -m "Add lockfile"
git push origin main
```

### Bước 4: Clear Build Cache (nếu cần)

1. Vào Vercel Dashboard → Project → Settings → General
2. Scroll xuống "Clear Build Cache"
3. Click "Clear"
4. Redeploy

### Bước 5: Kiểm tra Environment Variables

1. Vào Settings → Environment Variables
2. Đảm bảo có:
   ```env
   NEXT_PUBLIC_API_URL=http://localhost:8000
   ```
3. Apply cho tất cả environments (Production, Preview, Development)

### Bước 6: Redeploy

1. Vào Deployments tab
2. Click "Redeploy" trên deployment mới nhất
3. Hoặc push code mới để trigger auto-deploy

## Kiểm tra Build Logs

Nếu vẫn lỗi:

1. Vào Deployments → Click vào deployment failed
2. Xem "Build Logs"
3. Tìm dòng lỗi cụ thể
4. Common issues:
   - **"Cannot find module"**: Thiếu dependency → Thêm vào `package.json`
   - **"Path alias not found"**: Vấn đề với `@/` imports → 
     - Kiểm tra `tsconfig.json` có `baseUrl: "."` và `paths`
     - Đảm bảo có `jsconfig.json` (Next.js cần cả 2 files)
     - Kiểm tra Root Directory = `frontend` trên Vercel
   - **"Turbopack error"**: Next.js 16 issue → Đã fix trong `next.config.mjs`

## Checklist

Trước khi deploy lại:

- [ ] `jsconfig.json` đã được tạo và commit
- [ ] `tsconfig.json` có `baseUrl: "."` và `paths` config
- [ ] Root Directory = `frontend` trên Vercel
- [ ] `pnpm-lock.yaml` đã được commit
- [ ] `next.config.mjs` đã được cập nhật (không có webpack config phức tạp)
- [ ] Environment variables đã được set
- [ ] Build thành công local (`pnpm build`)

## Test Build Local

Trước khi deploy, test build local:

```bash
cd frontend
pnpm install
pnpm build
```

Nếu build thành công local nhưng fail trên Vercel:
- Kiểm tra Node.js version (Vercel dùng Node 18+)
- Kiểm tra pnpm version
- Clear cache và redeploy

## Nếu vẫn không được

1. Tạo issue mới trên Vercel với build logs
2. Hoặc thử deploy với npm thay vì pnpm:
   - Đổi Install Command = `npm install`
   - Đổi Build Command = `npm run build`
   - Commit `package-lock.json` thay vì `pnpm-lock.yaml`

