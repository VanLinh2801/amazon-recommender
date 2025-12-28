"""
Item API routes
===============

API endpoints để lấy thông tin items/products.
"""

import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.web.schemas.item import ItemResponse, ItemListResponse
from app.web.services.item_service import ItemService
from app.web.utils.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/items", tags=["items"])


@router.get(
    "/{asin}",
    response_model=ItemResponse,
    summary="Get item by ASIN",
    description="Lấy thông tin chi tiết của một item theo ASIN"
)
async def get_item(
    asin: str,
    db: AsyncSession = Depends(get_db)
):
    """
    Lấy thông tin item theo ASIN.
    
    Args:
        asin: Item ASIN
        db: Database session
        
    Returns:
        ItemResponse
    """
    item = await ItemService.get_item_by_asin(db, asin)
    
    if not item:
        raise HTTPException(
            status_code=404,
            detail=f"Item not found: {asin}"
        )
    
    return item


@router.get(
    "/",
    response_model=ItemListResponse,
    summary="Search items",
    description="Tìm kiếm items theo query, category, với pagination"
)
async def search_items(
    query: Optional[str] = Query(None, description="Search query (tìm trong title)"),
    category: Optional[str] = Query(None, description="Filter by category"),
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Page size"),
    db: AsyncSession = Depends(get_db)
):
    """
    Tìm kiếm items.
    
    Args:
        query: Search query
        category: Filter by category
        page: Page number
        page_size: Page size
        db: Database session
        
    Returns:
        ItemListResponse
    """
    items, total = await ItemService.search_items(
        db=db,
        query=query,
        category=category,
        page=page,
        page_size=page_size
    )
    
    return ItemListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        has_more=(page * page_size) < total
    )



