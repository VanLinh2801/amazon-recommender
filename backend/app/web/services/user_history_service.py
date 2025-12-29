"""
User History Service
====================

Service để lấy lịch sử tương tác của user từ database.
"""

import logging
from typing import List, Dict, Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class UserHistoryService:
    """Service để lấy user interaction history."""
    
    @staticmethod
    async def get_cart_items(
        db: AsyncSession,
        user_id: int,
        limit: int = 20
    ) -> List[str]:
        """
        Lấy danh sách ASINs trong giỏ hàng của user.
        
        Args:
            db: Database session
            user_id: User ID
            limit: Số lượng items tối đa
            
        Returns:
            List of ASINs
        """
        try:
            result = await db.execute(
                text("""
                    SELECT DISTINCT ci.asin, sc.created_at
                    FROM cart_items ci
                    JOIN shopping_carts sc ON ci.cart_id = sc.id
                    WHERE sc.user_id = :user_id
                      AND sc.status = 'active'
                    ORDER BY sc.created_at DESC
                    LIMIT :limit
                """),
                {"user_id": user_id, "limit": limit}
            )
            
            rows = result.fetchall()
            return [row.asin for row in rows]
            
        except Exception as e:
            logger.warning(f"Error getting cart items for user {user_id}: {e}")
            return []
    
    @staticmethod
    async def get_recent_interactions(
        db: AsyncSession,
        user_id: int,
        event_types: Optional[List[str]] = None,
        limit: int = 20
    ) -> List[Dict]:
        """
        Lấy lịch sử tương tác gần đây của user.
        
        Args:
            db: Database session
            user_id: User ID
            event_types: List các event types (ví dụ: ['add_to_cart', 'purchase', 'view'])
            limit: Số lượng items tối đa
            
        Returns:
            List of dicts với keys: asin, event_type, ts
        """
        try:
            query = """
                SELECT asin, event_type, ts
                FROM interaction_logs
                WHERE user_id = :user_id
            """
            params = {"user_id": user_id, "limit": limit}
            
            if event_types:
                query += " AND event_type = ANY(:event_types)"
                params["event_types"] = event_types
            else:
                # Mặc định lấy các event types quan trọng
                query += " AND event_type IN ('add_to_cart', 'purchase', 'view', 'click')"
            
            query += " ORDER BY ts DESC LIMIT :limit"
            
            result = await db.execute(text(query), params)
            rows = result.fetchall()
            
            return [
                {
                    "asin": row.asin,
                    "event_type": row.event_type,
                    "ts": row.ts
                }
                for row in rows
            ]
            
        except Exception as e:
            logger.warning(f"Error getting recent interactions for user {user_id}: {e}")
            return []
    
    @staticmethod
    async def get_user_reference_items(
        db: AsyncSession,
        user_id: int,
        include_cart: bool = True,
        include_purchases: bool = True,
        include_views: bool = False,
        limit_per_source: int = 10
    ) -> List[str]:
        """
        Lấy danh sách reference items để dùng cho content-based recommendations.
        
        Ưu tiên: cart > purchases > views
        
        Args:
            db: Database session
            user_id: User ID
            include_cart: Có lấy items trong cart không
            include_purchases: Có lấy items đã mua không
            include_views: Có lấy items đã xem không
            limit_per_source: Số lượng items tối đa từ mỗi source
            
        Returns:
            List of ASINs (đã deduplicate)
        """
        reference_items = []
        
        # 1. Items trong cart (ưu tiên cao nhất)
        if include_cart:
            cart_items = await UserHistoryService.get_cart_items(
                db, user_id, limit=limit_per_source
            )
            reference_items.extend(cart_items)
        
        # 2. Items đã mua
        if include_purchases:
            purchases = await UserHistoryService.get_recent_interactions(
                db, user_id, event_types=['purchase'], limit=limit_per_source
            )
            reference_items.extend([item['asin'] for item in purchases])
        
        # 3. Items đã xem (nếu cần)
        if include_views:
            views = await UserHistoryService.get_recent_interactions(
                db, user_id, event_types=['view'], limit=limit_per_source
            )
            reference_items.extend([item['asin'] for item in views])
        
        # Deduplicate (giữ thứ tự)
        seen = set()
        unique_items = []
        for item in reference_items:
            if item not in seen:
                seen.add(item)
                unique_items.append(item)
        
        return unique_items


