"""
Redis Context Service
=====================

Service để ghi realtime context vào Redis cho re-ranking.
Redis chỉ giữ short-term state (TTL 10-30 phút).
PostgreSQL là source of truth cho interaction history.
"""

import logging
import time
import redis
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class RedisContextService:
    """
    Service để quản lý realtime context trong Redis.
    
    Redis keys:
    - user:{user_id}:recent_items (List)
    - user:{user_id}:recent_categories (Hash)
    - user:{user_id}:last_active (String)
    """
    
    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        ttl_seconds: int = 900  # 15 phút
    ):
        """
        Khởi tạo RedisContextService.
        
        Args:
            redis_host: Redis host
            redis_port: Redis port
            redis_db: Redis database number
            ttl_seconds: TTL cho các keys (default: 900 = 15 phút)
        """
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True
        )
        self.ttl_seconds = ttl_seconds
        
        logger.info(
            f"RedisContextService initialized: "
            f"redis={redis_host}:{redis_port}, ttl={ttl_seconds}s"
        )
    
    def update_realtime_context(
        self,
        user_id: int,
        asin: str,
        category: Optional[str] = None,
        brand: Optional[str] = None
    ) -> bool:
        """
        Update realtime context trong Redis (NGAY LẬP TỨC).
        
        Thực hiện:
        1. Append asin vào user:{user_id}:recent_items (List)
        2. Increment category/brand vào user:{user_id}:recent_categories (Hash)
        3. Update user:{user_id}:last_active (String)
        
        Args:
            user_id: User ID
            asin: Item ASIN
            category: Item category (optional, từ products.main_category)
            brand: Item brand (optional, từ products.raw_metadata)
            
        Returns:
            True nếu thành công, False nếu có lỗi
        """
        try:
            # 1. Append asin vào recent_items (List)
            # Push vào đầu list, giới hạn length (ví dụ 20)
            key_items = f"user:{user_id}:recent_items"
            self.redis_client.lpush(key_items, asin)
            self.redis_client.ltrim(key_items, 0, 19)  # Giữ tối đa 20 items
            self.redis_client.expire(key_items, self.ttl_seconds)
            
            # 2. Increment category vào recent_categories (Hash)
            if category:
                key_categories = f"user:{user_id}:recent_categories"
                self.redis_client.hincrby(key_categories, category, 1)
                self.redis_client.expire(key_categories, self.ttl_seconds)
            
            # 3. Update last_active (String)
            key_last_active = f"user:{user_id}:last_active"
            self.redis_client.set(
                key_last_active,
                str(int(time.time())),
                ex=self.ttl_seconds
            )
            
            logger.debug(
                f"Updated Redis context for user {user_id}: "
                f"asin={asin}, category={category}"
            )
            
            return True
            
        except redis.RedisError as e:
            logger.error(f"Redis error updating context: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating Redis context: {e}")
            return False
    
    def get_recent_items(self, user_id: int) -> list[str]:
        """
        Lấy danh sách recent items từ Redis.
        
        Args:
            user_id: User ID
            
        Returns:
            List of ASINs (mới nhất ở đầu)
        """
        try:
            key = f"user:{user_id}:recent_items"
            items = self.redis_client.lrange(key, 0, -1)
            return items
        except Exception as e:
            logger.warning(f"Failed to get recent items: {e}")
            return []
    
    def get_recent_categories(self, user_id: int) -> Dict[str, int]:
        """
        Lấy recent categories từ Redis.
        
        Args:
            user_id: User ID
            
        Returns:
            Dict mapping category -> count
        """
        try:
            key = f"user:{user_id}:recent_categories"
            categories = self.redis_client.hgetall(key)
            return {cat: int(count) for cat, count in categories.items()}
        except Exception as e:
            logger.warning(f"Failed to get recent categories: {e}")
            return {}


# Singleton instance
_redis_context_service_instance: Optional[RedisContextService] = None


def get_redis_context_service(
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_db: int = 0
) -> RedisContextService:
    """
    Get singleton instance của RedisContextService.
    
    Args:
        redis_host: Redis host
        redis_port: Redis port
        redis_db: Redis database number
        
    Returns:
        RedisContextService instance
    """
    global _redis_context_service_instance
    
    if _redis_context_service_instance is None:
        _redis_context_service_instance = RedisContextService(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db
        )
    
    return _redis_context_service_instance



