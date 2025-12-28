"""
Helper script để setup Redis data cho testing re-ranking
========================================================

Usage:
    python -m app.recommender.setup_redis_data
"""

import sys
from pathlib import Path
import redis

# Thêm root directory vào path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))


def setup_redis_data():
    """Setup mock Redis data cho testing."""
    
    print("=" * 80)
    print("SETUP REDIS DATA CHO TESTING")
    print("=" * 80)
    
    # Kết nối Redis
    try:
        client = redis.Redis(
            host="localhost",
            port=6379,
            db=0,
            decode_responses=True
        )
        
        # Test connection
        client.ping()
        print("\n[OK] Đã kết nối Redis thành công!")
        
    except redis.ConnectionError:
        print("\n[ERROR] Không thể kết nối Redis!")
        print("Vui lòng đảm bảo Redis đang chạy:")
        print("  Docker: docker run -d -p 6379:6379 redis")
        return
    
    # Setup data cho test user
    test_user_id = "user_test_123"
    
    print(f"\nSetting up data for user: {test_user_id}")
    
    # 1. Recent items (List)
    key_items = f"user:{test_user_id}:recent_items"
    client.delete(key_items)
    client.lpush(key_items, "item_2", "item_4", "item_6")
    client.expire(key_items, 1800)  # TTL 30 phút
    print(f"  ✅ Recent items: {client.lrange(key_items, 0, -1)}")
    
    # 2. Recent categories (Hash)
    key_categories = f"user:{test_user_id}:recent_categories"
    client.delete(key_categories)
    client.hset(key_categories, mapping={
        "Electronics": "5",
        "Fashion": "2",
        "Home": "1"
    })
    client.expire(key_categories, 1800)  # TTL 30 phút
    print(f"  ✅ Recent categories: {client.hgetall(key_categories)}")
    
    # 3. Last active (String)
    key_last_active = f"user:{test_user_id}:last_active"
    import time
    client.set(key_last_active, str(int(time.time())))
    client.expire(key_last_active, 1800)
    print(f"  ✅ Last active: {client.get(key_last_active)}")
    
    print("\n" + "=" * 80)
    print("[OK] Redis data đã được setup!")
    print("=" * 80)
    print(f"\nKeys created:")
    print(f"  - {key_items}")
    print(f"  - {key_categories}")
    print(f"  - {key_last_active}")
    print(f"\nTTL: 30 phút (1800 seconds)")


if __name__ == "__main__":
    setup_redis_data()



