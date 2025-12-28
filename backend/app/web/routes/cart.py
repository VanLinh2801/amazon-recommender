"""
Shopping cart routes.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.web.utils.database import get_db
from app.web.utils.auth_middleware import get_current_user
from app.web.schemas.cart import (
    AddToCartRequest,
    UpdateCartItemRequest,
    CartResponse,
)
from app.web.schemas.auth import UserResponse, ErrorResponse
from app.web.services.cart_service import CartService

router = APIRouter(prefix="/api/cart", tags=["Shopping Cart"])


@router.get(
    "",
    response_model=CartResponse,
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        404: {"model": ErrorResponse, "description": "Cart not found"}
    }
)
async def get_cart(
    current_user: UserResponse = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Lấy thông tin cart của user hiện tại.
    
    Yêu cầu authentication token trong header:
    ```
    Authorization: Bearer <token>
    ```
    
    Trả về cart với tất cả items và thông tin product.
    Nếu chưa có cart, trả về cart rỗng.
    """
    cart = await CartService.get_cart(db, current_user.id)
    
    if not cart:
        # Trả về cart rỗng (sẽ tạo cart khi add item đầu tiên)
        from datetime import datetime
        return CartResponse(
            cart_id=0,
            user_id=current_user.id,
            status="active",
            created_at=datetime.utcnow(),
            items=[],
            total_items=0
        )
    
    return cart


@router.post(
    "/items",
    response_model=CartResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {"model": ErrorResponse, "description": "Bad request"},
        401: {"model": ErrorResponse, "description": "Unauthorized"}
    }
)
async def add_to_cart(
    item_data: AddToCartRequest,
    current_user: UserResponse = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Thêm item vào cart.
    
    - **asin**: Item ASIN
    - **quantity**: Số lượng (tối thiểu 1)
    
    Nếu item đã có trong cart, sẽ tăng quantity.
    """
    success, error = await CartService.add_item(db, current_user.id, item_data)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error or "Không thể thêm item vào cart"
        )
    
    # Trả về cart sau khi thêm
    cart = await CartService.get_cart(db, current_user.id)
    return cart


@router.put(
    "/items/{asin}",
    response_model=CartResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Bad request"},
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        404: {"model": ErrorResponse, "description": "Item not found in cart"}
    }
)
async def update_cart_item(
    asin: str,
    item_data: UpdateCartItemRequest,
    current_user: UserResponse = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Cập nhật quantity của item trong cart.
    
    - **asin**: Item ASIN (path parameter)
    - **quantity**: Số lượng mới (tối thiểu 1)
    """
    success, error = await CartService.update_item(
        db, current_user.id, asin, item_data
    )
    
    if not success:
        status_code = status.HTTP_404_NOT_FOUND if "không có" in (error or "").lower() else status.HTTP_400_BAD_REQUEST
        raise HTTPException(
            status_code=status_code,
            detail=error or "Không thể cập nhật item"
        )
    
    # Trả về cart sau khi cập nhật
    cart = await CartService.get_cart(db, current_user.id)
    return cart


@router.delete(
    "/items/{asin}",
    response_model=CartResponse,
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        404: {"model": ErrorResponse, "description": "Item not found in cart"}
    }
)
async def remove_from_cart(
    asin: str,
    current_user: UserResponse = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Xóa item khỏi cart.
    
    - **asin**: Item ASIN (path parameter)
    """
    success, error = await CartService.remove_item(db, current_user.id, asin)
    
    if not success:
        status_code = status.HTTP_404_NOT_FOUND if "không có" in (error or "").lower() else status.HTTP_400_BAD_REQUEST
        raise HTTPException(
            status_code=status_code,
            detail=error or "Không thể xóa item"
        )
    
    # Trả về cart sau khi xóa
    cart = await CartService.get_cart(db, current_user.id)
    return cart


@router.delete(
    "",
    response_model=CartResponse,
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"}
    }
)
async def clear_cart(
    current_user: UserResponse = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Xóa tất cả items khỏi cart.
    """
    success, error = await CartService.clear_cart(db, current_user.id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error or "Không thể xóa cart"
        )
    
    # Trả về cart rỗng
    cart = await CartService.get_cart(db, current_user.id)
    return cart

