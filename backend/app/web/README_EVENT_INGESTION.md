# Event Ingestion Pipeline

## Tổng quan

Event Ingestion Pipeline xử lý user interaction events với kiến trúc:
- **Redis**: Realtime context (short-term, TTL 15 phút)
- **PostgreSQL**: Long-term event store (interaction_logs table)
- **Background Tasks**: Ghi PostgreSQL không block request

## Kiến trúc

```
Frontend → POST /api/event
    ↓
1. Ghi Redis context (NGAY LẬP TỨC)
    - user:{user_id}:recent_items (List)
    - user:{user_id}:recent_categories (Hash)
    - user:{user_id}:last_active (String)
    ↓
2. Trả HTTP 200 OK (KHÔNG chờ PostgreSQL)
    ↓
3. Background Task → Ghi PostgreSQL (LONG-TERM)
    - INSERT vào interaction_logs
```

## API Endpoint

### POST /api/event

**Request:**
```json
{
  "user_id": 123,
  "asin": "B08XXXX",
  "event_type": "view",
  "metadata": {
    "page": "product_detail",
    "timestamp": "2024-01-01T00:00:00Z"
  }
}
```

**Response:**
```json
{
  "success": true,
  "message": "Event logged: view"
}
```

**Event Types:**
- `view`: User xem item
- `click`: User click vào item
- `add_to_cart`: User thêm vào giỏ hàng
- `purchase`: User mua item
- `rate`: User đánh giá item

## Redis Keys Schema

### user:{user_id}:recent_items
- **Type**: List
- **Value**: Danh sách ASINs (mới nhất ở đầu)
- **Max Length**: 20 items
- **TTL**: 15 phút (900 seconds)

### user:{user_id}:recent_categories
- **Type**: Hash
- **Fields**: `{category: interaction_count}`
- **TTL**: 15 phút

### user:{user_id}:last_active
- **Type**: String (timestamp)
- **TTL**: 15 phút

## Database Schema

### interaction_logs

```sql
CREATE TABLE interaction_logs (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES users(id),
    asin        TEXT NOT NULL REFERENCES items(asin),
    event_type  event_type_enum NOT NULL,
    ts          TIMESTAMP NOT NULL DEFAULT NOW(),
    metadata    JSONB
);
```

**Metadata fields (optional):**
- `category`: Item category (tự động thêm từ products.main_category)
- `brand`: Item brand (tự động thêm từ products.raw_metadata)
- Custom fields từ frontend

## Components

### 1. Event Schemas (`app/web/schemas/event.py`)
- `EventRequest`: Request schema
- `EventResponse`: Response schema
- `InteractionLog`: Internal model
- `EventType`: Enum cho event types

### 2. Redis Context Service (`app/web/services/redis_context_service.py`)
- `RedisContextService`: Quản lý realtime context trong Redis
- `update_realtime_context()`: Update Redis keys
- `get_recent_items()`: Lấy recent items
- `get_recent_categories()`: Lấy recent categories

### 3. Event Logging Service (`app/web/services/event_logging_service.py`)
- `EventLoggingService`: Ghi interaction logs vào PostgreSQL
- `log_interaction()`: INSERT vào interaction_logs (với retry)
- `get_item_category()`: Lấy category từ database
- `get_item_brand()`: Lấy brand từ database

### 4. Event API Route (`app/web/routes/event.py`)
- `POST /api/event`: Nhận events từ frontend
- Background task: `log_interaction_to_postgres()`

## Usage

### 1. Frontend Integration

```typescript
// frontend/lib/api.ts
export async function logEvent(
  userId: number,
  asin: string,
  eventType: 'view' | 'click' | 'add_to_cart' | 'purchase' | 'rate',
  metadata?: Record<string, any>
) {
  const response = await fetch(`${API_BASE_URL}/api/event`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      user_id: userId,
      asin,
      event_type: eventType,
      metadata,
    }),
  });
  
  return response.json();
}

// Usage
await logEvent(1, 'B08XXXX', 'view', { page: 'product_detail' });
```

### 2. Testing

```bash
# Test với Python script
python -m app.web.test_event_api

# Test với curl
curl -X POST http://localhost:8000/api/event \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "asin": "B08XXXX",
    "event_type": "view"
  }'
```

### 3. Verify

**Check Redis:**
```bash
redis-cli
> LRANGE user:1:recent_items 0 -1
> HGETALL user:1:recent_categories
> GET user:1:last_active
```

**Check PostgreSQL:**
```sql
SELECT * FROM interaction_logs 
WHERE user_id = 1 
ORDER BY ts DESC 
LIMIT 10;
```

## Flow Example: User Click Item

1. **Frontend**: User click vào item `B08XXXX`
   ```typescript
   await logEvent(userId, 'B08XXXX', 'click');
   ```

2. **Backend**: Nhận request
   - Lấy category từ database (nhanh)
   - Ghi Redis context (NGAY LẬP TỨC)
     - Push `B08XXXX` vào `user:{user_id}:recent_items`
     - Increment category vào `user:{user_id}:recent_categories`
     - Update `user:{user_id}:last_active`
   - Trả 200 OK

3. **Background Task**: Ghi PostgreSQL (KHÔNG block request)
   - INSERT vào `interaction_logs`
   - Thêm category và brand vào metadata
   - Retry nếu fail

4. **Re-ranking**: Sử dụng Redis context
   - Load `recent_items` và `recent_categories`
   - Áp dụng rules (intent boost, recent penalty, etc.)

## Performance

- **Redis write**: < 5ms (synchronous)
- **PostgreSQL write**: Background (không block request)
- **Request latency**: Chỉ phụ thuộc vào Redis write

## Error Handling

- **Redis fail**: Log warning, vẫn trả 200 OK (không fail request)
- **PostgreSQL fail**: Retry 3 lần, log error nếu vẫn fail
- **Database query fail**: Fallback về None, background task sẽ retry

## Configuration

### Redis TTL
Mặc định: 15 phút (900 seconds)

Có thể thay đổi trong `RedisContextService.__init__()`:
```python
redis_service = RedisContextService(ttl_seconds=1800)  # 30 phút
```

### Retry Policy
Mặc định: 3 retries

Có thể thay đổi trong `EventLoggingService.log_interaction()`:
```python
await EventLoggingService.log_interaction(db, log, max_retries=5)
```

## Integration với Re-ranking

Re-ranking service tự động sử dụng Redis context:

```python
from app.recommender.reranking_service import ReRankingService

reranking_service = ReRankingService()
reranked_items = reranking_service.rerank_items(user_id, ranked_items)

# Re-ranking sẽ tự động load:
# - recent_items từ Redis
# - recent_categories từ Redis
# - Áp dụng rules (intent boost, recent penalty, etc.)
```

## Notes

1. **Redis ≠ Database**: Redis chỉ giữ short-term state, PostgreSQL là source of truth
2. **Non-blocking**: Request không chờ PostgreSQL write
3. **Automatic enrichment**: Category và brand tự động thêm vào metadata
4. **Retry logic**: PostgreSQL write có retry đơn giản
5. **TTL**: Redis keys tự động expire sau 15 phút

## Files

- `app/web/schemas/event.py`: Event schemas
- `app/web/services/redis_context_service.py`: Redis context service
- `app/web/services/event_logging_service.py`: PostgreSQL logging service
- `app/web/routes/event.py`: Event API route
- `app/web/test_event_api.py`: Test script



