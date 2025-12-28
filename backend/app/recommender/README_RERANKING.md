# Re-ranking Service

## Tổng quan

Re-ranking Layer là bước cuối cùng trong pipeline recommendation (Recall -> Ranking -> Re-ranking), sử dụng Redis làm short-term memory để điều chỉnh danh sách recommendation theo hành vi realtime của user.

**Đặc điểm:**
- Rule-based logic (không dùng ML)
- Sử dụng Redis để lưu trữ short-term context
- Điều chỉnh score, không filter items
- Dễ giải thích và debug

## Kiến trúc

```
RankedItems (từ Ranking Layer)
    ↓
Load Redis Context (recent_items, recent_categories)
    ↓
Apply Rules:
  - Intent boost (category)
  - Penalize recent items
  - Diversity penalty
  - Popularity floor
    ↓
Re-rankedItems (sorted by adjusted_score)
```

## Redis Keys

Service đọc các keys sau từ Redis:

1. **`user:{user_id}:recent_items`** (List)
   - Danh sách item_id user vừa xem/click
   - Mới nhất ở đầu list
   - TTL: ~10-30 phút

2. **`user:{user_id}:recent_categories`** (Hash)
   - Mapping `{category: interaction_count}`
   - Dùng để suy ra short-term intent
   - TTL: ~10-30 phút

3. **`user:{user_id}:last_active`** (String, optional)
   - Timestamp của lần active cuối
   - TTL: ~10-30 phút

## Rules

### Rule 1: Short-term Intent Boost

Nếu item.category nằm trong `recent_categories`:
```
adjusted_score *= (1 + min(0.2, 0.05 * interaction_count))
```

**Ví dụ:**
- User vừa xem 5 items "Electronics" → boost +20%
- User vừa xem 2 items "Fashion" → boost +10%

### Rule 2: Penalize Recent Items

Nếu item_id nằm trong `recent_items`:
```
adjusted_score *= 0.7  (penalty -30%)
```

**Mục đích:** Tránh recommend lại items user vừa xem.

### Rule 3: Diversity Penalty

Nếu top-N có quá nhiều items cùng category (>40%):
```
adjusted_score *= 0.85  (penalty -15%)
```

**Mục đích:** Tăng diversity trong top recommendations.

### Rule 4: Popularity Floor (Optional)

Nếu `item.rating_number < threshold` (mặc định: 5):
```
adjusted_score *= 0.9  (penalty -10%)
```

**Mục đích:** Ưu tiên items có đủ reviews.

## Usage

### Basic Usage

```python
from app.recommender.reranking_service import ReRankingService
from app.recommender.ranking_service import RankedItem

# Tạo service
reranking_service = ReRankingService(
    redis_host="localhost",
    redis_port=6379,
    top_n=20
)

# Re-rank items
ranked_items = [
    RankedItem(
        item_id="item_1",
        rank_score=0.95,
        rank_position=1,
        category="Electronics",
        rating_number=1000
    ),
    # ... more items
]

reranked_items = reranking_service.rerank_items(
    user_id="user_123",
    ranked_items=ranked_items
)

# Kết quả
for item in reranked_items:
    print(f"{item.rank_position}. {item.item_id}")
    print(f"   Original: {item.rank_score:.4f}")
    print(f"   Adjusted: {item.adjusted_score:.4f}")
    print(f"   Rules: {item.applied_rules}")
```

### Convenience Function

```python
from app.recommender.reranking_service import rerank_items

reranked_items = rerank_items(
    user_id="user_123",
    ranked_items=ranked_items,
    redis_host="localhost",
    redis_port=6379,
    top_n=20
)
```

### Full Pipeline

Xem `recommendation_pipeline.py` để xem ví dụ full pipeline:
- Recall → Ranking → Re-ranking

## Setup Redis

### Docker

```bash
docker run -d -p 6379:6379 redis
```

### Setup Test Data

```bash
python -m app.recommender.setup_redis_data
```

Script này sẽ tạo mock data cho user `user_test_123`:
- Recent items: `item_2`, `item_4`, `item_6`
- Recent categories: `Electronics: 5`, `Fashion: 2`, `Home: 1`

## Testing

### Test với Mock Data

```bash
python -m app.recommender.test_reranking
```

### Test Full Pipeline

```bash
python -m app.recommender.recommendation_pipeline
```

## Configuration

### ReRankingService Parameters

- `redis_host`: Redis host (default: "localhost")
- `redis_port`: Redis port (default: 6379)
- `redis_db`: Redis database number (default: 0)
- `top_n`: Số lượng items top-N cần trả về (default: 20)
- `min_rating_threshold`: Threshold cho popularity floor rule (default: 5)
- `diversity_threshold`: Threshold cho diversity rule (default: 0.4 = 40%)

### Debug Mode

Bật debug logging:

```python
import app.recommender.reranking_service as reranking_module
reranking_module.DEBUG_RERANKING = True
```

## Output Format

### ReRankedItem

```python
@dataclass
class ReRankedItem:
    item_id: str
    rank_score: float          # Original ranking score
    adjusted_score: float       # Score sau khi áp dụng rules
    rank_position: int          # Vị trí trong ranking (1-based)
    applied_rules: List[str]    # List of rule names đã áp dụng
    category: Optional[str]     # Item category
    rating_number: Optional[int] # Number of ratings
```

### Logging

Service log các thông tin sau:
- Số lượng items được re-rank
- Redis context (recent items, categories)
- Top 5 items với explainability (rules applied)

**Ví dụ log:**
```
INFO - Re-ranking 50 items for user_id: user_123
DEBUG - Redis context: 3 recent items, 3 recent categories
INFO - Top 5 re-ranked items:
  Rank 1: item_1 | rank_score=0.950000 → adjusted_score=0.950000 | rules=['intent_boost(Electronics:+20%)']
  Rank 2: item_3 | rank_score=0.900000 → adjusted_score=0.630000 | rules=['recent_penalty(-30%)']
  ...
```

## Integration với API

Để integrate vào API `/recommend`:

```python
from app.recommender.reranking_service import get_reranking_service

# Trong route handler
reranking_service = get_reranking_service()

# Sau khi có ranked_items từ RankingService
reranked_items = reranking_service.rerank_items(
    user_id=user_id,
    ranked_items=ranked_items
)

# Trả về recommendations
return {
    "user_id": user_id,
    "recommendations": [
        {
            "item_id": item.item_id,
            "score": item.adjusted_score,
            "rank": item.rank_position,
            "rules_applied": item.applied_rules
        }
        for item in reranked_items
    ]
}
```

## Lưu ý

1. **Redis Connection**: Service sẽ fallback về empty list/dict nếu không kết nối được Redis, không raise exception.

2. **Rule Order**: Rules được áp dụng tuần tự:
   - Intent boost
   - Recent penalty
   - Diversity (sau khi có top items)
   - Popularity floor

3. **No Hard Filtering**: Service chỉ điều chỉnh score, không loại bỏ items.

4. **Performance**: Re-ranking rất nhanh (CPU-friendly), phù hợp cho realtime.

5. **TTL**: Redis keys nên có TTL ~10-30 phút để đảm bảo context không quá cũ.

## Dependencies

- `redis`: Redis client library
- `app.recommender.ranking_service`: RankedItem dataclass

## Files

- `reranking_service.py`: Main service implementation
- `test_reranking.py`: Test script
- `setup_redis_data.py`: Helper script để setup Redis data
- `recommendation_pipeline.py`: Full pipeline example



