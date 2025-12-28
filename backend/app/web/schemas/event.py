"""
Event schemas cho Event Ingestion Pipeline.
"""
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum


class EventType(str, Enum):
    """Event types theo database enum."""
    VIEW = "view"
    CLICK = "click"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"
    RATE = "rate"


class EventRequest(BaseModel):
    """
    Request schema cho POST /event.
    
    Attributes:
        user_id: User ID (BIGINT)
        asin: Item ASIN (TEXT)
        event_type: Loại event (event_type_enum)
        metadata: Optional metadata (JSONB)
    """
    user_id: int = Field(..., description="User ID")
    asin: str = Field(..., description="Item ASIN")
    event_type: EventType = Field(..., description="Event type")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Optional metadata (JSONB)")


class EventResponse(BaseModel):
    """
    Response schema cho POST /event.
    
    Attributes:
        success: True nếu event đã được ghi vào Redis
        message: Thông báo
    """
    success: bool
    message: str


class InteractionLog(BaseModel):
    """
    Internal model cho interaction log (để ghi vào PostgreSQL).
    
    Attributes:
        user_id: User ID
        asin: Item ASIN
        event_type: Event type
        metadata: Optional metadata
    """
    user_id: int
    asin: str
    event_type: EventType
    metadata: Optional[Dict[str, Any]] = None



