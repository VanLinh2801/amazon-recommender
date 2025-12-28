"""
Item Service
============

Service để lấy thông tin items/products từ database.
"""

import json
import logging
from typing import Optional, List
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.web.schemas.item import ItemResponse

logger = logging.getLogger(__name__)


class ItemService:
    """Service xử lý item operations."""
    
    @staticmethod
    async def get_item_by_asin(
        db: AsyncSession,
        asin: str
    ) -> Optional[ItemResponse]:
        """
        Lấy thông tin item theo ASIN.
        
        Args:
            db: Database session
            asin: Item ASIN
            
        Returns:
            ItemResponse hoặc None nếu không tìm thấy
        """
        try:
            # Rollback bất kỳ transaction cũ nào nếu có lỗi
            try:
                await db.rollback()
            except Exception:
                pass
            
            result = await db.execute(
                text("""
                    SELECT 
                        i.asin,
                        i.parent_asin,
                        i.variant,
                        i.primary_image as item_image,
                        i.category,
                        p.title,
                        p.store,
                        p.main_category,
                        p.avg_rating,
                        p.rating_number,
                        p.primary_image as product_image,
                        p.raw_metadata
                    FROM items i
                    JOIN products p ON i.parent_asin = p.parent_asin
                    WHERE i.asin = :asin
                    LIMIT 1
                """),
                {"asin": asin}
            )
            
            row = result.fetchone()
            
            if not row:
                return None
            
            # Sử dụng item image nếu có, nếu không thì dùng product image
            primary_image = row.item_image or row.product_image
            
            # Parse raw_metadata nếu là string
            raw_metadata = row.raw_metadata
            if isinstance(raw_metadata, str):
                try:
                    raw_metadata = json.loads(raw_metadata)
                except (json.JSONDecodeError, TypeError):
                    raw_metadata = None
            elif raw_metadata is None:
                raw_metadata = None
            
            return ItemResponse(
                asin=row.asin,
                parent_asin=row.parent_asin,
                title=row.title,
                store=row.store,
                main_category=row.main_category,
                category=row.category,
                avg_rating=float(row.avg_rating) if row.avg_rating else None,
                rating_number=row.rating_number,
                primary_image=primary_image,
                variant=row.variant,
                raw_metadata=raw_metadata
            )
            
        except Exception as e:
            logger.error(f"Error getting item by ASIN {asin}: {e}")
            # Rollback transaction nếu có lỗi
            try:
                await db.rollback()
            except Exception:
                pass
            return None
    
    @staticmethod
    async def get_items_by_asins(
        db: AsyncSession,
        asins: List[str]
    ) -> List[ItemResponse]:
        """
        Lấy thông tin nhiều items theo danh sách ASINs.
        
        Args:
            db: Database session
            asins: List of ASINs
            
        Returns:
            List of ItemResponse
        """
        if not asins:
            return []
        
        try:
            # Rollback bất kỳ transaction cũ nào nếu có lỗi
            try:
                await db.rollback()
            except Exception:
                pass
            
            # Tạo mapping để giữ thứ tự
            asin_order = {asin: i for i, asin in enumerate(asins)}
            
            result = await db.execute(
                text("""
                    SELECT 
                        i.asin,
                        i.parent_asin,
                        i.variant,
                        i.primary_image as item_image,
                        i.category,
                        p.title,
                        p.store,
                        p.main_category,
                        p.avg_rating,
                        p.rating_number,
                        p.primary_image as product_image,
                        p.raw_metadata
                    FROM items i
                    JOIN products p ON i.parent_asin = p.parent_asin
                    WHERE i.asin = ANY(:asins)
                """),
                {"asins": asins}
            )
            
            # Tạo dict để map ASIN -> ItemResponse
            item_dict = {}
            for row in result.fetchall():
                primary_image = row.item_image or row.product_image
                
                # Parse raw_metadata nếu là string
                raw_metadata = row.raw_metadata
                if isinstance(raw_metadata, str):
                    try:
                        raw_metadata = json.loads(raw_metadata)
                    except (json.JSONDecodeError, TypeError):
                        raw_metadata = None
                elif raw_metadata is None:
                    raw_metadata = None
                
                item_dict[row.asin] = ItemResponse(
                    asin=row.asin,
                    parent_asin=row.parent_asin,
                    title=row.title,
                    store=row.store,
                    main_category=row.main_category,
                    category=row.category,
                    avg_rating=float(row.avg_rating) if row.avg_rating else None,
                    rating_number=row.rating_number,
                    primary_image=primary_image,
                    variant=row.variant,
                    raw_metadata=raw_metadata
                )
            
            # Trả về theo thứ tự ASINs ban đầu
            items = [item_dict[asin] for asin in asins if asin in item_dict]
            return items
            
        except Exception as e:
            logger.error(f"Error getting items by ASINs: {e}")
            # Rollback transaction nếu có lỗi
            try:
                await db.rollback()
            except Exception:
                pass
            
            # Fallback: query từng item với session mới
            items = []
            for asin in asins:
                try:
                    item = await ItemService.get_item_by_asin(db, asin)
                    if item:
                        items.append(item)
                except Exception as item_error:
                    logger.warning(f"Error getting item {asin}: {item_error}")
                    # Rollback và tiếp tục
                    try:
                        await db.rollback()
                    except Exception:
                        pass
                    continue
            return items
    
    @staticmethod
    async def search_items(
        db: AsyncSession,
        query: Optional[str] = None,
        category: Optional[str] = None,
        page: int = 1,
        page_size: int = 20
    ) -> tuple[List[ItemResponse], int]:
        """
        Tìm kiếm items.
        
        Args:
            db: Database session
            query: Search query (tìm trong title)
            category: Filter by category
            page: Page number (1-based)
            page_size: Number of items per page
            
        Returns:
            Tuple of (items, total_count)
        """
        try:
            # Build WHERE clause
            where_clauses = []
            params = {}
            
            if query:
                where_clauses.append("p.title ILIKE :query")
                params["query"] = f"%{query}%"
            
            if category:
                # Tìm theo cả main_category và category (từ semantic attributes)
                where_clauses.append("(p.main_category = :category OR i.category = :category)")
                params["category"] = category
            
            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            # Count total
            count_result = await db.execute(
                text(f"""
                    SELECT COUNT(DISTINCT i.asin) as total
                    FROM items i
                    JOIN products p ON i.parent_asin = p.parent_asin
                    WHERE {where_sql}
                """),
                params
            )
            total = count_result.fetchone().total
            
            # Get items
            offset = (page - 1) * page_size
            result = await db.execute(
                text(f"""
                    SELECT DISTINCT ON (i.asin)
                        i.asin,
                        i.parent_asin,
                        i.variant,
                        i.primary_image as item_image,
                        i.category,
                        p.title,
                        p.store,
                        p.main_category,
                        p.avg_rating,
                        p.rating_number,
                        p.primary_image as product_image,
                        p.raw_metadata
                    FROM items i
                    JOIN products p ON i.parent_asin = p.parent_asin
                    WHERE {where_sql}
                    ORDER BY i.asin, p.avg_rating DESC NULLS LAST
                    LIMIT :limit OFFSET :offset
                """),
                {**params, "limit": page_size, "offset": offset}
            )
            
            items = []
            for row in result.fetchall():
                primary_image = row.item_image or row.product_image
                
                # Parse raw_metadata nếu là string
                raw_metadata = row.raw_metadata
                if isinstance(raw_metadata, str):
                    try:
                        raw_metadata = json.loads(raw_metadata)
                    except (json.JSONDecodeError, TypeError):
                        raw_metadata = None
                elif raw_metadata is None:
                    raw_metadata = None
                
                items.append(ItemResponse(
                    asin=row.asin,
                    parent_asin=row.parent_asin,
                    title=row.title,
                    store=row.store,
                    main_category=row.main_category,
                    category=row.category,
                    avg_rating=float(row.avg_rating) if row.avg_rating else None,
                    rating_number=row.rating_number,
                    primary_image=primary_image,
                    variant=row.variant,
                    raw_metadata=raw_metadata
                ))
            
            return items, total
            
        except Exception as e:
            logger.error(f"Error searching items: {e}")
            return [], 0

