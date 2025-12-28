"""
Pydantic schemas cho shopping cart.
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class CartItemBase(BaseModel):
    """Base schema cho cart item."""
    asin: str = Field(..., description="Item ASIN")
    quantity: int = Field(..., ge=1, description="Số lượng (tối thiểu 1)")


class AddToCartRequest(CartItemBase):
    """Schema cho request thêm item vào cart."""
    pass


class UpdateCartItemRequest(BaseModel):
    """Schema cho request cập nhật quantity của item."""
    quantity: int = Field(..., ge=1, description="Số lượng mới (tối thiểu 1)")


class CartItemResponse(BaseModel):
    """Schema cho response cart item với thông tin product."""
    asin: str
    quantity: int
    # Product info
    title: Optional[str] = None
    parent_asin: Optional[str] = None
    primary_image: Optional[str] = None
    price: Optional[float] = None  # Có thể lấy từ raw_metadata nếu có
    
    class Config:
        from_attributes = True


class CartResponse(BaseModel):
    """Schema cho response cart."""
    cart_id: int
    user_id: int
    status: str
    created_at: datetime
    items: list[CartItemResponse] = []
    total_items: int = 0
    
    class Config:
        from_attributes = True

