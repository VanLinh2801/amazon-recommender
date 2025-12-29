"""
Event Logging Service
=====================

Async worker service để ghi interaction_logs vào PostgreSQL (LONG-TERM).
Service này chạy trong background task, KHÔNG block request.
"""

import json
import logging
from typing import Optional, Dict, Any
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.web.schemas.event import EventType, InteractionLog

logger = logging.getLogger(__name__)


class EventLoggingService:
    """
    Service để ghi interaction logs vào PostgreSQL.
    
    Chạy trong background task, có retry đơn giản nếu insert fail.
    """
    
    @staticmethod
    async def log_interaction(
        db: AsyncSession,
        interaction_log: InteractionLog,
        max_retries: int = 3
    ) -> bool:
        """
        Ghi interaction log vào PostgreSQL.
        
        Args:
            db: Database session
            interaction_log: InteractionLog object
            max_retries: Số lần retry nếu insert fail (default: 3)
            
        Returns:
            True nếu thành công, False nếu fail sau retries
        """
        for attempt in range(max_retries):
            try:
                # INSERT vào interaction_logs
                # PostgreSQL JSONB có thể nhận dict trực tiếp
                # SQLAlchemy với asyncpg hỗ trợ named parameters
                # Type casting được xử lý tự động
                # Convert metadata dict to JSON string for asyncpg
                metadata_json = None
                if interaction_log.metadata:
                    metadata_json = json.dumps(interaction_log.metadata)
                
                await db.execute(
                    text("""
                        INSERT INTO interaction_logs (
                            user_id,
                            asin,
                            event_type,
                            ts,
                            metadata
                        ) VALUES (
                            :user_id,
                            :asin,
                            CAST(:event_type AS event_type_enum),
                            NOW(),
                            CAST(:metadata AS jsonb)
                        )
                    """),
                    {
                        "user_id": interaction_log.user_id,
                        "asin": interaction_log.asin,
                        "event_type": interaction_log.event_type.value,
                        "metadata": metadata_json
                    }
                )
                
                await db.commit()
                
                logger.debug(
                    f"Logged interaction: user_id={interaction_log.user_id}, "
                    f"asin={interaction_log.asin}, event_type={interaction_log.event_type.value}"
                )
                
                return True
                
            except Exception as e:
                logger.warning(
                    f"Failed to log interaction (attempt {attempt + 1}/{max_retries}): {e}"
                )
                
                if attempt < max_retries - 1:
                    # Retry
                    await db.rollback()
                    continue
                else:
                    # Fail sau khi retry hết
                    await db.rollback()
                    logger.error(
                        f"Failed to log interaction after {max_retries} attempts: "
                        f"user_id={interaction_log.user_id}, asin={interaction_log.asin}"
                    )
                    return False
        
        return False
    
    @staticmethod
    async def get_item_category(
        db: AsyncSession,
        asin: str
    ) -> Optional[str]:
        """
        Lấy category của item từ database.
        
        Args:
            db: Database session
            asin: Item ASIN
            
        Returns:
            Category string hoặc None
        """
        try:
            result = await db.execute(
                text("""
                    SELECT p.main_category
                    FROM items i
                    JOIN products p ON i.parent_asin = p.parent_asin
                    WHERE i.asin = :asin
                    LIMIT 1
                """),
                {"asin": asin}
            )
            
            row = result.fetchone()
            if row and row.main_category:
                return row.main_category
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to get item category for {asin}: {e}")
            return None
    
    @staticmethod
    async def get_item_brand(
        db: AsyncSession,
        asin: str
    ) -> Optional[str]:
        """
        Lấy brand của item từ database (từ raw_metadata).
        
        Args:
            db: Database session
            asin: Item ASIN
            
        Returns:
            Brand string hoặc None
        """
        try:
            result = await db.execute(
                text("""
                    SELECT p.raw_metadata
                    FROM items i
                    JOIN products p ON i.parent_asin = p.parent_asin
                    WHERE i.asin = :asin
                    LIMIT 1
                """),
                {"asin": asin}
            )
            
            row = result.fetchone()
            if row and row.raw_metadata:
                # Parse JSONB để lấy brand
                import json
                metadata = row.raw_metadata
                if isinstance(metadata, dict):
                    brand = metadata.get("brand") or metadata.get("Brand")
                    if brand:
                        return str(brand)
                elif isinstance(metadata, str):
                    try:
                        metadata_dict = json.loads(metadata)
                        brand = metadata_dict.get("brand") or metadata_dict.get("Brand")
                        if brand:
                            return str(brand)
                    except:
                        pass
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to get item brand for {asin}: {e}")
            return None

