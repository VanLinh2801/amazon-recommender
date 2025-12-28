"""
Authentication middleware và dependencies cho FastAPI.
"""
from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.web.utils.database import get_db
from app.web.utils.jwt import decode_access_token
from app.web.schemas.auth import UserResponse

security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
) -> UserResponse:
    """
    Dependency để lấy current user từ JWT token.
    
    Args:
        credentials: HTTP Bearer token từ header
        db: Database session
        
    Returns:
        UserResponse object
        
    Raises:
        HTTPException: Nếu token không hợp lệ hoặc user không tồn tại
    """
    token = credentials.credentials
    payload = decode_access_token(token)
    
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token không hợp lệ hoặc đã hết hạn",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token không hợp lệ",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Lấy user từ database
    result = await db.execute(
        text("""
            SELECT id, username, phone_number, created_at, last_login
            FROM users
            WHERE id = :user_id
        """),
        {"user_id": int(user_id)}
    )
    
    user_row = result.fetchone()
    
    if not user_row:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User không tồn tại",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return UserResponse(
        id=user_row.id,
        username=user_row.username,
        phone_number=user_row.phone_number,
        created_at=user_row.created_at,
        last_login=user_row.last_login
    )


async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False)),
    db: AsyncSession = Depends(get_db)
) -> Optional[UserResponse]:
    """
    Dependency để lấy current user nếu có token, trả về None nếu không có.
    Dùng cho các endpoints không bắt buộc authentication.
    """
    if not credentials:
        return None
    
    try:
        return await get_current_user(credentials, db)
    except HTTPException:
        return None

