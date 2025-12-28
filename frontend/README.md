# Frontend - E-commerce Recommender

Frontend application được xây dựng với Next.js 16, TypeScript, và Tailwind CSS.

## Yêu cầu

- **Node.js**: >= 18.x
- **pnpm**: >= 8.x (hoặc npm/yarn)

## Cài đặt

### 1. Cài đặt pnpm (nếu chưa có)

```bash
# Windows (PowerShell)
npm install -g pnpm

# Hoặc dùng npm/yarn thay thế
```

### 2. Cài đặt dependencies

```bash
cd frontend
pnpm install
```

Hoặc nếu dùng npm:
```bash
cd frontend
npm install
```

## Cấu hình

### 1. Tạo file `.env.local`

Copy file `.env.example` thành `.env.local`:

```bash
cp .env.example .env.local
```

Hoặc tạo file `.env.local` với nội dung:

```env
NEXT_PUBLIC_API_URL=http://localhost:8000
```

**Lưu ý**: Đảm bảo backend API đang chạy ở port 8000 (hoặc cập nhật URL cho đúng).

## Chạy ứng dụng

### Development mode

```bash
pnpm dev
```

Hoặc:
```bash
npm run dev
```

Ứng dụng sẽ chạy tại: **http://localhost:3000**

### Build production

```bash
pnpm build
pnpm start
```

## Cấu trúc thư mục

```
frontend/
├── app/                    # Next.js App Router
│   ├── page.tsx           # Home page
│   ├── login/             # Login page
│   ├── register/          # Register page
│   ├── dashboard/         # Dashboard
│   ├── cart/              # Shopping cart
│   └── product/[id]/      # Product detail
├── components/            # React components
│   ├── ui/                # shadcn/ui components
│   ├── navbar.tsx
│   ├── footer.tsx
│   └── ...
├── lib/                   # Utilities
└── public/                # Static files
```

## Scripts

- `pnpm dev` - Chạy development server
- `pnpm build` - Build production
- `pnpm start` - Chạy production server
- `pnpm lint` - Chạy ESLint

## Tech Stack

- **Next.js 16** - React framework
- **TypeScript** - Type safety
- **Tailwind CSS** - Styling
- **shadcn/ui** - UI components
- **React Hook Form** - Form handling
- **Zod** - Schema validation

## Kết nối với Backend

Frontend sẽ tự động kết nối với backend API thông qua biến môi trường `NEXT_PUBLIC_API_URL`.

Đảm bảo backend đang chạy trước khi start frontend:

```bash
# Terminal 1: Chạy backend
cd ..
python run.py

# Terminal 2: Chạy frontend
cd frontend
pnpm dev
```

