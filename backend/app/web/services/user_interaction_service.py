"""
User Interaction Service
========================
Service để aggregate user interactions từ Redis và PostgreSQL.
Sử dụng để cải thiện recommendations với realtime data.
"""

import logging
from typing import List, Dict, Optional, Set
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import redis

logger = logging.getLogger(__name__)


class UserInteractionService:
    """
    Service để aggregate user interactions từ Redis (realtime) và PostgreSQL (long-term).
    """
    
    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0
    ):
        """
        Khởi tạo UserInteractionService.
        
        Args:
            redis_client: Redis client (optional, sẽ tạo mới nếu None)
            redis_host: Redis host
            redis_port: Redis port
            redis_db: Redis database number
        """
        if redis_client:
            self.redis_client = redis_client
        else:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=True
            )
    
    def get_recent_items_from_redis(self, user_id: int, limit: int = 50) -> List[str]:
        """
        Lấy recent items từ Redis.
        
        Args:
            user_id: User ID
            limit: Số lượng items tối đa
            
        Returns:
            List of ASINs (mới nhất ở đầu)
        """
        try:
            key = f"user:{user_id}:recent_items"
            items = self.redis_client.lrange(key, 0, limit - 1)
            return [item for item in items if item]
        except Exception as e:
            logger.warning(f"Failed to get recent items from Redis for user {user_id}: {e}")
            return []
    
    def get_recent_categories_from_redis(self, user_id: int) -> Dict[str, int]:
        """
        Lấy recent categories từ Redis.
        
        Args:
            user_id: User ID
            
        Returns:
            Dict mapping category -> interaction_count
        """
        try:
            key = f"user:{user_id}:recent_categories"
            categories = self.redis_client.hgetall(key)
            return {k: int(v) for k, v in categories.items() if k and v}
        except Exception as e:
            logger.warning(f"Failed to get recent categories from Redis for user {user_id}: {e}")
            return {}
    
    async def get_user_interactions_from_db(
        self,
        db: AsyncSession,
        user_id: int,
        limit: int = 100,
        event_types: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Lấy user interactions từ PostgreSQL.
        
        Args:
            db: Database session
            user_id: User ID
            limit: Số lượng interactions tối đa
            event_types: List of event types để filter (None = all)
            
        Returns:
            List of interactions với format:
            {
                'asin': str,
                'event_type': str,
                'ts': datetime,
                'category': Optional[str]
            }
        """
        try:
            query = """
                SELECT 
                    il.asin,
                    il.event_type,
                    il.ts,
                    p.main_category as category
                FROM interaction_logs il
                LEFT JOIN items i ON il.asin = i.asin
                LEFT JOIN products p ON i.parent_asin = p.parent_asin
                WHERE il.user_id = :user_id
            """
            
            params = {"user_id": user_id}
            
            if event_types:
                query += " AND il.event_type = ANY(:event_types)"
                params["event_types"] = event_types
            
            query += " ORDER BY il.ts DESC LIMIT :limit"
            params["limit"] = limit
            
            result = await db.execute(text(query), params)
            rows = result.fetchall()
            
            interactions = []
            for row in rows:
                interactions.append({
                    'asin': row.asin,
                    'event_type': row.event_type,
                    'ts': row.ts,
                    'category': row.category
                })
            
            return interactions
            
        except Exception as e:
            logger.warning(f"Failed to get user interactions from DB for user {user_id}: {e}")
            return []
    
    async def get_user_item_history(
        self,
        db: AsyncSession,
        user_id: int,
        include_realtime: bool = True,
        limit: int = 100
    ) -> Dict:
        """
        Aggregate user item history từ Redis và PostgreSQL.
        
        Args:
            db: Database session
            user_id: User ID
            include_realtime: Có lấy data từ Redis không
            limit: Số lượng items tối đa
            
        Returns:
            Dict với:
            {
                'recent_items': List[str],  # Từ Redis (realtime)
                'all_items': Set[str],  # Tất cả items từ Redis + DB
                'recent_categories': Dict[str, int],  # Từ Redis
                'all_categories': Dict[str, int],  # Aggregate từ Redis + DB
                'interaction_count': int
            }
        """
        result = {
            'recent_items': [],
            'all_items': set(),
            'recent_categories': {},
            'all_categories': {},
            'interaction_count': 0
        }
        
        # 1. Lấy từ Redis (realtime)
        if include_realtime:
            recent_items = self.get_recent_items_from_redis(user_id, limit=50)
            result['recent_items'] = recent_items
            result['all_items'].update(recent_items)
            
            recent_categories = self.get_recent_categories_from_redis(user_id)
            result['recent_categories'] = recent_categories
            for cat, count in recent_categories.items():
                result['all_categories'][cat] = result['all_categories'].get(cat, 0) + count
        
        # 2. Lấy từ PostgreSQL (long-term)
        try:
            interactions = await self.get_user_interactions_from_db(
                db, user_id, limit=limit
            )
            
            db_items = set()
            db_categories = {}
            
            for interaction in interactions:
                db_items.add(interaction['asin'])
                if interaction['category']:
                    db_categories[interaction['category']] = db_categories.get(
                        interaction['category'], 0
                    ) + 1
            
            result['all_items'].update(db_items)
            for cat, count in db_categories.items():
                result['all_categories'][cat] = result['all_categories'].get(cat, 0) + count
            
            result['interaction_count'] = len(interactions)
            
        except Exception as e:
            logger.warning(f"Failed to get user interactions from DB: {e}")
        
        # Convert set to list for JSON serialization
        result['all_items'] = list(result['all_items'])
        
        return result


# Singleton instance
_user_interaction_service_instance: Optional[UserInteractionService] = None


def get_user_interaction_service(
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_db: int = 0
) -> UserInteractionService:
    """
    Get singleton instance của UserInteractionService.
    
    Args:
        redis_host: Redis host
        redis_port: Redis port
        redis_db: Redis database number
        
    Returns:
        UserInteractionService instance
    """
    global _user_interaction_service_instance
    
    if _user_interaction_service_instance is None:
        _user_interaction_service_instance = UserInteractionService(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db
        )
    
    return _user_interaction_service_instance


