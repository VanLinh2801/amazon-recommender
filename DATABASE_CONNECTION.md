# Database Connection Guide

## Connection String Format

### Render PostgreSQL Database

Render cung cấp 2 loại connection string:

1. **Internal Connection** (từ services trong cùng Render account):
   ```
   postgresql://user:password@host/dbname
   ```
   - Không cần domain đầy đủ
   - Chỉ hoạt động từ Render services

2. **External Connection** (từ máy local hoặc services khác):
   ```
   postgresql://user:password@host.region-postgres.render.com:5432/dbname
   ```
   - Cần domain đầy đủ với region
   - Cần port 5432
   - Hoạt động từ bất kỳ đâu

### Format cho FastAPI (asyncpg)

Code sử dụng `asyncpg` driver, nên cần format:
```
postgresql+asyncpg://user:password@host:port/dbname
```

**Lưu ý:** Scripts tự động convert `postgresql://` → `postgresql+asyncpg://`

## Database của bạn

**Connection String (External):**
```
postgresql://recommender_qhkw_user:k8cvRIaH5rSns9saC2EEhkBQnAvzGfV6@dpg-d58jdfre5dus73dvifd0-a.oregon-postgres.render.com:5432/recommender_qhkw
```

**Cho Render Environment Variable:**
```
postgresql+asyncpg://recommender_qhkw_user:k8cvRIaH5rSns9saC2EEhkBQnAvzGfV6@dpg-d58jdfre5dus73dvifd0-a.oregon-postgres.render.com:5432/recommender_qhkw
```

## Test Connection

```bash
# Test kết nối
python backend/scripts/test_db_connection.py "postgresql://recommender_qhkw_user:k8cvRIaH5rSns9saC2EEhkBQnAvzGfV6@dpg-d58jdfre5dus73dvifd0-a.oregon-postgres.render.com:5432/recommender_qhkw"
```

## Setup Database Schema

```bash
# Chạy schema
python backend/scripts/setup_database.py "postgresql://recommender_qhkw_user:k8cvRIaH5rSns9saC2EEhkBQnAvzGfV6@dpg-d58jdfre5dus73dvifd0-a.oregon-postgres.render.com:5432/recommender_qhkw"

# Chạy schema + load dữ liệu (nếu có parquet files)
python backend/scripts/setup_database.py "postgresql://..." --load-data
```

## Environment Variable

Trên Render, set:
```env
DATABASE_URL=postgresql+asyncpg://recommender_qhkw_user:k8cvRIaH5rSns9saC2EEhkBQnAvzGfV6@dpg-d58jdfre5dus73dvifd0-a.oregon-postgres.render.com:5432/recommender_qhkw
```

## Security Notes

⚠️ **QUAN TRỌNG:**
- Không commit connection string vào Git
- Chỉ dùng environment variables
- Connection string này đã được expose trong chat - nên đổi password sau khi deploy

