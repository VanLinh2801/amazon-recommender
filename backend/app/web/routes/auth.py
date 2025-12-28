"""
Authentication routes - đăng ký và đăng nhập.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.web.utils.database import get_db
from app.web.utils.auth_middleware import get_current_user
from app.web.schemas.auth import (
    RegisterRequest,
    LoginRequest,
    AuthResponse,
    UserResponse,
    ErrorResponse
)
from app.web.services.auth_service import AuthService

router = APIRouter(prefix="/api/auth", tags=["Authentication"])


@router.post(
    "/register",
    response_model=AuthResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {"model": ErrorResponse, "description": "Bad request"},
        409: {"model": ErrorResponse, "description": "Username đã tồn tại"}
    }
)
async def register(
    register_data: RegisterRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Đăng ký user mới.
    
    - **username**: Username (3-50 ký tự, không chứa khoảng trắng)
    - **password**: Password (tối thiểu 6 ký tự)
    - **phone_number**: Số điện thoại (optional)
    
    Trả về access token và thông tin user.
    """
    user, error = await AuthService.register(db, register_data)
    
    if error:
        if "đã tồn tại" in error:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=error
            )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )
    
    # Tạo token
    access_token = AuthService.create_token(user)
    
    return AuthResponse(
        access_token=access_token,
        token_type="bearer",
        user=user
    )


@router.get(
    "/me",
    response_model=UserResponse,
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"}
    }
)
async def get_current_user_info(
    current_user: UserResponse = Depends(get_current_user)
):
    """
    Lấy thông tin user hiện tại.
    
    Yêu cầu authentication token trong header:
    ```
    Authorization: Bearer <token>
    ```
    """
    return current_user


@router.post(
    "/login",
    response_model=AuthResponse,
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"}
    }
)
async def login(
    login_data: LoginRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Đăng nhập user.
    
    - **username**: Username
    - **password**: Password
    
    Trả về access token và thông tin user.
    """
    user, error = await AuthService.login(db, login_data)
    
    if error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=error
        )
    
    # Tạo token
    access_token = AuthService.create_token(user)
    
    return AuthResponse(
        access_token=access_token,
        token_type="bearer",
        user=user
    )

