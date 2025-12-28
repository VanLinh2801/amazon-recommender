"""
Event API routes
================

API endpoint để nhận events từ frontend và xử lý:
1. Ghi realtime context vào Redis (NGAY LẬP TỨC)
2. Trả HTTP 200 OK ngay (KHÔNG chờ PostgreSQL)
3. Gửi event sang async worker để ghi PostgreSQL (LONG-TERM)
"""

import logging
from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.web.schemas.event import EventRequest, EventResponse, InteractionLog
from app.web.services.redis_context_service import get_redis_context_service
from app.web.services.event_logging_service import EventLoggingService
from app.web.utils.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/event", tags=["events"])


async def log_interaction_to_postgres(
    db: AsyncSession,
    interaction_log: InteractionLog,
    category: str = None,
    brand: str = None
):
    """
    Background task để ghi interaction log vào PostgreSQL.
    
    Args:
        db: Database session
        interaction_log: InteractionLog object
        category: Item category (optional)
        brand: Item brand (optional)
    """
    try:
        # Nếu chưa có category, lấy từ database
        if not category:
            category = await EventLoggingService.get_item_category(db, interaction_log.asin)
        
        # Nếu chưa có brand, lấy từ database
        if not brand:
            brand = await EventLoggingService.get_item_brand(db, interaction_log.asin)
        
        # Thêm category và brand vào metadata nếu có
        if interaction_log.metadata is None:
            interaction_log.metadata = {}
        
        if category:
            interaction_log.metadata["category"] = category
        if brand:
            interaction_log.metadata["brand"] = brand
        
        # Ghi vào PostgreSQL
        success = await EventLoggingService.log_interaction(db, interaction_log)
        
        if success:
            logger.info(
                f"✅ Logged interaction to PostgreSQL: "
                f"user_id={interaction_log.user_id}, asin={interaction_log.asin}, "
                f"event_type={interaction_log.event_type.value}"
            )
        else:
            logger.error(
                f"❌ Failed to log interaction to PostgreSQL: "
                f"user_id={interaction_log.user_id}, asin={interaction_log.asin}"
            )
            
    except Exception as e:
        logger.error(f"Error in background task log_interaction_to_postgres: {e}")


@router.post(
    "/",
    response_model=EventResponse,
    status_code=200,
    summary="Log user interaction event",
    description="""
    Nhận event từ frontend và xử lý:
    1. Ghi realtime context vào Redis (NGAY LẬP TỨC)
    2. Trả HTTP 200 OK ngay (KHÔNG chờ PostgreSQL)
    3. Gửi event sang async worker để ghi PostgreSQL (LONG-TERM)
    
    Event types: view, click, add_to_cart, purchase, rate
    """
)
async def log_event(
    event_request: EventRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """
    Log user interaction event.
    
    Flow:
    1. Ghi Redis context (realtime) - NGAY LẬP TỨC
    2. Trả 200 OK - KHÔNG chờ PostgreSQL
    3. Background task ghi PostgreSQL (long-term)
    """
    try:
        # Lấy Redis context service
        redis_service = get_redis_context_service()
        
        # Lấy category và brand từ database (nhanh, không block)
        # Nếu không lấy được, sẽ lấy trong background task
        category = await EventLoggingService.get_item_category(db, event_request.asin)
        brand = await EventLoggingService.get_item_brand(db, event_request.asin)
        
        # 1. GHI REDIS CONTEXT (NGAY LẬP TỨC)
        # Đây là bước quan trọng nhất - phải làm ngay để re-ranking có context
        redis_success = redis_service.update_realtime_context(
            user_id=event_request.user_id,
            asin=event_request.asin,
            category=category,
            brand=brand
        )
        
        if not redis_success:
            logger.warning(
                f"Failed to update Redis context for user {event_request.user_id}, "
                f"asin={event_request.asin}"
            )
            # Vẫn tiếp tục, không fail request
        
        # 2. TẠO INTERACTION LOG OBJECT
        interaction_log = InteractionLog(
            user_id=event_request.user_id,
            asin=event_request.asin,
            event_type=event_request.event_type,
            metadata=event_request.metadata
        )
        
        # 3. GỬI SANG BACKGROUND TASK (KHÔNG CHỜ)
        # Background task sẽ ghi vào PostgreSQL
        background_tasks.add_task(
            log_interaction_to_postgres,
            db=db,
            interaction_log=interaction_log,
            category=category,
            brand=brand
        )
        
        # 4. TRẢ 200 OK NGAY (KHÔNG CHỜ POSTGRESQL)
        return EventResponse(
            success=True,
            message=f"Event logged: {event_request.event_type.value}"
        )
        
    except Exception as e:
        logger.error(f"Error in log_event endpoint: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to log event: {str(e)}"
        )

